import asyncio
import logging
import random
import time

from tornado.httpclient import AsyncHTTPClient
from tornado.escape import json_decode, json_encode

from toshieth.mixins import BalanceMixin
from toshieth.tasks import (
    BaseEthServiceWorker, BaseTaskHandler,
    manager_dispatcher, erc20_dispatcher, eth_dispatcher, push_dispatcher
)
from toshi.ethereum.mixin import EthereumMixin
from toshi.jsonrpc.errors import JsonRPCError
from toshi.log import configure_logger, log_unhandled_exceptions
from toshi.utils import parse_int
from toshi.sofa import SofaPayment
from toshi.ethereum.tx import (
    create_transaction, encode_transaction, calculate_transaction_hash
)
from toshi.ethereum.utils import data_decoder, data_encoder, decode_single_address

from toshieth.constants import TRANSFER_TOPIC, DEPOSIT_TOPIC, WITHDRAWAL_TOPIC, WETH_CONTRACT_ADDRESS
from toshi.config import config

log = logging.getLogger("toshieth.manager")

TRANSACTION_PROCESSING_TIMEOUT = 120

class TransactionQueueHandler(EthereumMixin, BalanceMixin, BaseTaskHandler):

    @log_unhandled_exceptions(logger=log)
    async def process_transaction_queue(self, ethereum_address):
        should_run = False
        try:
            should_run = await self.redis.set('processing_tx_queue:{}'.format(ethereum_address), 1,
                                              expire=TRANSACTION_PROCESSING_TIMEOUT,
                                              exist=self.redis.SET_IF_NOT_EXIST)
            if not should_run:
                await self.redis.set('processing_tx_queue:{}:should_re_run'.format(ethereum_address), 1)
                return
            while should_run:
                await self._process_transaction_queue(ethereum_address)
                tr = self.redis.multi_exec()
                fut1 = tr.get('processing_tx_queue:{}:should_re_run'.format(ethereum_address))
                fut2 = tr.delete('processing_tx_queue:{}:should_re_run'.format(ethereum_address))
                await tr.execute()
                should_run = await fut1
                await fut2
                if should_run:
                    # reset tx queue expiry
                    await self.redis.set('processing_tx_queue:{}'.format(ethereum_address), 1,
                                         expire=TRANSACTION_PROCESSING_TIMEOUT)
        except:
            log.exception("Error processing transaction queue for {}".format(ethereum_address))

        await self.redis.delete('processing_tx_queue:{}'.format(ethereum_address))

    async def _process_transaction_queue(self, ethereum_address):

        log.debug("processing tx queue for {}".format(ethereum_address))

        # check for un-scheduled transactions
        async with self.db:
            # get the last block number to use in ethereum calls
            # to avoid race conditions in transactions being confirmed
            # on the network before the block monitor sees and updates them in the database
            last_blocknumber = (await self.db.fetchval("SELECT blocknumber FROM last_blocknumber"))
            transactions_out = await self.db.fetch(
                "SELECT * FROM transactions "
                "WHERE from_address = $1 "
                "AND (status = 'new' OR status = 'queued') "
                "AND r IS NOT NULL "
                # order by nonce reversed so that .pop() can
                # be used in the loop below
                "ORDER BY nonce DESC",
                ethereum_address)

        # any time the state of a transaction is changed we need to make
        # sure those changes cascade down to the receiving address as well
        # this keeps a list of all the receiving addresses that need to be
        # checked after the current address's queue has been processed
        addresses_to_check = set()

        if transactions_out:

            # TODO: make sure the block number isn't too far apart from the current
            # if this is the case then we should just come back later!

            # get the current network balance for this address
            balance = await self.eth.eth_getBalance(ethereum_address, block=last_blocknumber or "latest")

            # get the unconfirmed_txs
            async with self.db:
                unconfirmed_txs = await self.db.fetch(
                    "SELECT nonce, value, gas, gas_price FROM transactions "
                    "WHERE from_address = $1 "
                    "AND (status = 'unconfirmed' "
                    "OR (status = 'confirmed' AND blocknumber > $2)) "
                    "ORDER BY nonce",
                    ethereum_address, last_blocknumber or 0)

            network_nonce = await self.eth.eth_getTransactionCount(ethereum_address, block=last_blocknumber or "latest")

            if unconfirmed_txs:
                nonce = unconfirmed_txs[-1]['nonce'] + 1
                balance -= sum(parse_int(tx['value']) + (parse_int(tx['gas']) * parse_int(tx['gas_price'])) for tx in unconfirmed_txs)
            else:
                # use the nonce from the network
                nonce = network_nonce

            # marker for whether a previous transaction had an error (signaling
            # that all the following should also be an error
            previous_error = False

            # for each one, check if we can schedule them yet
            while transactions_out:
                transaction = transactions_out.pop()

                # if there was a previous error in the queue, abort!
                if previous_error:
                    log.info("Setting tx '{}' to error due to previous error".format(transaction['hash']))
                    await self.update_transaction(transaction['transaction_id'], 'error')
                    addresses_to_check.add(transaction['to_address'])
                    continue

                # make sure the nonce is still valid
                if nonce != transaction['nonce'] and network_nonce != transaction['nonce']:
                    # check if this is an overwrite
                    if transaction['status'] == 'new':
                        async with self.db:
                            old_tx = await self.db.fetchrow("SELECT * FROM transactions where from_address = $1 AND nonce = $2 AND hash != $3", ethereum_address, transaction['nonce'], transaction['hash'])
                        if old_tx:
                            if old_tx['status'] == 'error':
                                # expected state for overwrites
                                pass
                            elif old_tx['status'] == 'unconfirmed' or old_tx['status'] == 'confirmed':
                                previous_error = True
                                log.info(("Setting tx '{}' to error due to another unconfirmed transaction"
                                          "with nonce ({}) already existing in the system").format(
                                              transaction['hash'], transaction['nonce']))
                                await self.update_transaction(transaction['transaction_id'], 'error')
                                addresses_to_check.add(transaction['to_address'])
                                continue
                            else:
                                # two transactions with the same nonce on the queue
                                # lets pick the one with the highest gas price and error the other
                                if transaction['nonce'] > old_tx['nonce']:
                                    # lets use this one!
                                    log.info(("Setting tx '{}' to error due to another unconfirmed transaction"
                                              "with nonce ({}) already existing in the system").format(
                                                  old_tx['hash'], transaction['nonce']))
                                    await self.update_transaction(old_tx['transaction_id'], 'error')
                                    addresses_to_check.add(old_tx['to_address'])
                                    # make sure the other transaction is pulled out of the queue
                                    try:
                                        idx = next(i for i, e in enumerate(transactions_out) if e['transaction_id'] == old_tx['transaction_id'])
                                        del transactions_out[idx]
                                    except:
                                        # old_tx not in the transactions_out list
                                        pass
                                else:
                                    # we'll use the other one
                                    log.info(("Setting tx '{}' to error due to another unconfirmed transaction"
                                              "with nonce ({}) already existing in the system").format(
                                                  old_tx['hash'], transaction['nonce']))
                                    await self.update_transaction(transaction['transaction_id'], 'error')
                                    addresses_to_check.add(transaction['to_address'])
                                    addresses_to_check.add(transaction['from_address'])
                                    # this case is actually pretty weird, so emptying the
                                    # transactions_out so we restart the queue check
                                    # completely
                                    transactions_out = []
                                    continue

                        else:
                            # well this is awkward! may as well let things go on in this case because
                            # it means a transaction in the nonce sequence is missing
                            pass
                    elif transaction['status'] == 'queued':
                        # then this and all the following transactions are now invalid
                        previous_error = True
                        log.info("Setting tx '{}' to error due to the nonce ({}) not matching the network ({})".format(
                            transaction['hash'], transaction['nonce'], nonce))
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        addresses_to_check.add(transaction['to_address'])
                        continue
                    else:
                        # this is a really weird state
                        # it's not clear what should be done here
                        log.error("Found unconfirmed transaction with out of order nonce for address: {}".format(ethereum_address))
                        return

                value = parse_int(transaction['value'])
                gas = parse_int(transaction['gas'])
                gas_price = parse_int(transaction['gas_price'])
                cost = value + (gas * gas_price)

                # check if the current balance is high enough to send to the network
                if balance >= cost:

                    # check if gas price is high enough that it makes sense to send the transaction
                    safe_gas_price = parse_int(await self.redis.get('gas_station_safelow_gas_price'))
                    if safe_gas_price and safe_gas_price > gas_price:
                        log.debug("Not queuing tx '{}' as current gas price would not support it".format(transaction['hash']))
                        # retry this address in a minute
                        manager_dispatcher.process_transaction_queue(ethereum_address).delay(60)
                        # abort the rest of the processing after sending PNs for any "new" transactions
                        while transaction:
                            if transaction['status'] == 'new':
                                await self.update_transaction(transaction['transaction_id'], 'queued')
                            transaction = transactions_out.pop() if transactions_out else None
                        break

                    # if so, send the transaction
                    # create the transaction
                    data = data_decoder(transaction['data']) if transaction['data'] else b''
                    tx = create_transaction(nonce=transaction['nonce'], value=value, gasprice=gas_price, startgas=gas,
                                            to=transaction['to_address'], data=data,
                                            v=parse_int(transaction['v']),
                                            r=parse_int(transaction['r']),
                                            s=parse_int(transaction['s']))
                    # make sure the signature was valid
                    if data_encoder(tx.sender) != ethereum_address:
                        # signature is invalid for the user
                        log.error("ERROR signature invalid for sender of tx: {}".format(transaction['hash']))
                        log.error("queue: {}, db: {}, tx: {}".format(ethereum_address, transaction['from_address'], data_encoder(tx.sender)))
                        previous_error = True
                        addresses_to_check.add(transaction['to_address'])
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        continue
                    # send the transaction
                    try:
                        tx_encoded = encode_transaction(tx)
                        await self.eth.eth_sendRawTransaction(tx_encoded)
                        await self.update_transaction(transaction['transaction_id'], 'unconfirmed')
                    except JsonRPCError as e:
                        # if something goes wrong with sending the transaction
                        # simply abort for now.
                        # TODO: depending on error, just break and queue to retry later
                        log.error("ERROR sending queued transaction: {}".format(e.format()))
                        if e.message and (e.message.startswith("Transaction nonce is too low") or
                                          e.message.startswith("Transaction with the same hash was already imported")):
                            existing_tx = await self.eth.eth_getTransactionByHash(transaction['hash'])
                            if existing_tx:
                                if existing_tx['blockNumber']:
                                    await self.update_transaction(transaction['transaction_id'], 'confirmed')
                                else:
                                    await self.update_transaction(transaction['transaction_id'], 'unconfirmed')
                                continue
                        previous_error = True
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        addresses_to_check.add(transaction['to_address'])
                        continue

                    # adjust the balance values for checking the other transactions
                    balance -= cost
                    if nonce == transaction['nonce']:
                        nonce += 1
                    continue
                else:
                    # make sure the pending_balance would support this transaction
                    # otherwise there's no way this transaction will be able to
                    # be send, so trigger a failure on all the remaining transactions

                    async with self.db:
                        transactions_in = await self.db.fetch(
                            "SELECT * FROM transactions "
                            "WHERE to_address = $1 "
                            "AND ("
                            "(status = 'new' OR status = 'queued' OR status = 'unconfirmed') "
                            "OR (status = 'confirmed' AND blocknumber > $2))",
                            ethereum_address, last_blocknumber or 0)

                    # TODO: test if loops in the queue chain are problematic
                    pending_received = sum((parse_int(p['value']) or 0) for p in transactions_in)

                    if balance + pending_received < cost:
                        previous_error = True
                        log.info("Setting tx '{}' to error due to insufficient pending balance".format(transaction['hash']))
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        addresses_to_check.add(transaction['to_address'])
                        continue
                    else:
                        if any(t['blocknumber'] is not None and t['blocknumber'] > last_blocknumber for t in transactions_in):
                            addresses_to_check.add(ethereum_address)

                        # there's no reason to continue on here since all the
                        # following transaction in the queue cannot be processed
                        # until this one is

                        # but we still need to send PNs for any "new" transactions
                        while transaction:
                            if transaction['status'] == 'new':
                                await self.update_transaction(transaction['transaction_id'], 'queued')
                            transaction = transactions_out.pop() if transactions_out else None
                        break

        for address in addresses_to_check:
            # make sure we don't try process any contract deployments
            if address != "0x":
                manager_dispatcher.process_transaction_queue(address)

        if transactions_out:
            manager_dispatcher.process_transaction_queue(ethereum_address)

    @log_unhandled_exceptions(logger=log)
    async def update_transaction(self, transaction_id, status, retry_start_time=0):

        async with self.db:
            tx = await self.db.fetchrow("SELECT * FROM transactions WHERE transaction_id = $1", transaction_id)
            if tx is None or tx['status'] == status:
                return

            token_txs = await self.db.fetch(
                "SELECT tok.symbol, tok.name, tok.decimals, tx.contract_address, tx.value, tx.from_address, tx.to_address, tx.transaction_log_index, tx.status "
                "FROM token_transactions tx "
                "JOIN tokens tok "
                "ON tok.contract_address = tx.contract_address "
                "WHERE tx.transaction_id = $1", transaction_id)

            # check if we're trying to update the state of a tx that is already confirmed, we have an issue
            if tx['status'] == 'confirmed':
                log.warning("Trying to update status of tx {} to {}, but tx is already confirmed".format(tx['hash'], status))
                return

            # only log if the transaction is internal
            if tx['v'] is not None:
                log.info("Updating status of tx {} to {} (previously: {})".format(tx['hash'], status, tx['status']))

        if status == 'confirmed':
            transaction = await self.eth.eth_getTransactionByHash(tx['hash'])
            if transaction and 'blockNumber' in transaction and transaction['blockNumber'] is not None:
                if retry_start_time > 0:
                    log.info("successfully confirmed tx {} after {} seconds".format(tx['hash'], round(time.time() - retry_start_time, 2)))
                blocknumber = parse_int(transaction['blockNumber'])
                async with self.db:
                    await self.db.execute("UPDATE transactions SET status = $1, blocknumber = $2, updated = (now() AT TIME ZONE 'utc') "
                                          "WHERE transaction_id = $3",
                                          status, blocknumber, transaction_id)
                    await self.db.commit()
            else:
                # this is probably because the node hasn't caught up with the latest block yet, retry in a "bit" (but only retry up to 60 seconds)
                if retry_start_time > 0 and time.time() - retry_start_time >= 60:
                    if transaction is None:
                        log.error("requested transaction {}'s status to be set to confirmed, but cannot find the transaction".format(tx['hash']))
                    else:
                        log.error("requested transaction {}'s status to be set to confirmed, but transaction is not confirmed on the node".format(tx['hash']))
                    return
                await asyncio.sleep(random.random())
                manager_dispatcher.update_transaction(transaction_id, status, retry_start_time=retry_start_time or time.time())
                return
        else:
            async with self.db:
                await self.db.execute("UPDATE transactions SET status = $1, updated = (now() AT TIME ZONE 'utc') WHERE transaction_id = $2",
                                      status, transaction_id)
                await self.db.commit()

        # render notification

        # don't send "queued"
        if status == 'queued':
            status = 'unconfirmed'
        elif status == 'unconfirmed' and tx['status'] == 'queued':
            # there's already been a tx for this so no need to send another
            return

        messages = []

        # check if this is an erc20 transaction, if so use those values
        if token_txs:
            if status == 'confirmed':
                tx_receipt = await self.eth.eth_getTransactionReceipt(tx['hash'])
                if tx_receipt is None:
                    log.error("Failed to get transaction receipt for confirmed transaction: {}".format(tx_receipt))
                    # requeue to try again
                    manager_dispatcher.update_transaction(transaction_id, status)
                    return
            for token_tx in token_txs:
                token_tx_status = status
                from_address = token_tx['from_address']
                to_address = token_tx['to_address']
                if status == 'confirmed':
                    # check transaction receipt to make sure the transfer was successful
                    has_transfer_event = False
                    if tx_receipt['logs'] is not None:  # should always be [], but checking just incase
                        for _log in tx_receipt['logs']:
                            if len(_log['topics']) > 2:
                                if _log['topics'][0] == TRANSFER_TOPIC and \
                                   decode_single_address(_log['topics'][1]) == from_address and \
                                   decode_single_address(_log['topics'][2]) == to_address:
                                    has_transfer_event = True
                                    break
                            elif _log['address'] == WETH_CONTRACT_ADDRESS:
                                if _log['topics'][0] == DEPOSIT_TOPIC and decode_single_address(_log['topics'][1]) == to_address:
                                    has_transfer_event = True
                                    break
                                elif _log['topics'][0] == WITHDRAWAL_TOPIC and decode_single_address(_log['topics'][1]) == from_address:
                                    has_transfer_event = True
                                    break
                    if not has_transfer_event:
                        # there was no Transfer event matching this transaction
                        token_tx_status = 'error'
                    else:
                        erc20_dispatcher.update_token_cache(token_tx['contract_address'],
                                                            from_address,
                                                            to_address,
                                                            blocknumber=parse_int(transaction['blockNumber']))
                if token_tx_status == 'confirmed':
                    data = {
                        "txHash": tx['hash'],
                        "fromAddress": from_address,
                        "toAddress": to_address,
                        "status": token_tx_status,
                        "value": token_tx['value'],
                        "contractAddress": token_tx['contract_address']
                    }
                    messages.append((from_address, to_address, token_tx_status, "SOFA::TokenPayment: " + json_encode(data)))
                async with self.db:
                    await self.db.execute(
                        "UPDATE token_transactions SET status = $1 "
                        "WHERE transaction_id = $2 AND transaction_log_index = $3",
                        token_tx_status, tx['transaction_id'], token_tx['transaction_log_index'])
                    await self.db.commit()

                # if a WETH deposit or withdrawal, we need to let the client know to
                # update their ETHER balance using a normal SOFA:Payment
                if token_tx['contract_address'] == WETH_CONTRACT_ADDRESS and (from_address == "0x0000000000000000000000000000000000000000" or to_address == "0x0000000000000000000000000000000000000000"):
                    payment = SofaPayment(value=parse_int(token_tx['value']), txHash=tx['hash'],
                                          status=status, fromAddress=from_address, toAddress=to_address,
                                          networkId=config['ethereum']['network_id'])
                    messages.append((from_address, to_address, status, payment.render()))

        else:
            from_address = tx['from_address']
            to_address = tx['to_address']
            payment = SofaPayment(value=parse_int(tx['value']), txHash=tx['hash'], status=status,
                                  fromAddress=from_address, toAddress=to_address,
                                  networkId=config['ethereum']['network_id'])
            messages.append((from_address, to_address, status, payment.render()))

        # figure out what addresses need pns
        # from address always needs a pn
        for from_address, to_address, status, message in messages:
            manager_dispatcher.send_notification(from_address, message)

            # no need to check to_address for contract deployments
            if to_address == "0x":
                # TODO: update any notification registrations to be marked as a contract
                return

            # check if this is a brand new tx with no status
            if tx['status'] == 'new':
                # if an error has happened before any PNs have been sent
                # we only need to send the error to the sender, thus we
                # only add 'to' if the new status is not an error
                if status != 'error':
                    manager_dispatcher.send_notification(to_address, message)
            else:
                manager_dispatcher.send_notification(to_address, message)

            # trigger a processing of the to_address's queue incase it has
            # things waiting on this transaction
            manager_dispatcher.process_transaction_queue(to_address)

    async def send_notification(self, address, message):
        async with self.db:
            rows = await self.db.fetch(
                "SELECT DISTINCT(service) FROM notification_registrations WHERE eth_address = $1",
                address)
        services = [row['service'] for row in rows]
        if 'ws' in services:
            eth_dispatcher.send_notification(address, message)
        if 'gcm' in services or 'apn' in services:
            push_dispatcher.send_notification(address, message)

    @log_unhandled_exceptions(logger=log)
    async def sanity_check(self, frequency):
        async with self.db:
            rows = await self.db.fetch(
                "SELECT DISTINCT from_address FROM transactions WHERE (status = 'unconfirmed' OR status = 'queued' OR status = 'new') "
                "AND v IS NOT NULL AND created < (now() AT TIME ZONE 'utc') - interval '3 minutes'"
            )
            rows2 = await self.db.fetch(
                "WITH t1 AS (SELECT DISTINCT from_address FROM transactions WHERE (status = 'new' OR status = 'queued') AND v IS NOT NULL), "
                "t2 AS (SELECT from_address, COUNT(*) FROM transactions WHERE (status = 'unconfirmed' AND v IS NOT NULL) GROUP BY from_address) "
                "SELECT t1.from_address FROM t1 LEFT JOIN t2 ON t1.from_address = t2.from_address WHERE t2.count IS NULL;")
        if rows or rows2:
            log.debug("sanity check found {} addresses with potential problematic transactions".format(len(rows) + len(rows2)))

        rows = set([row['from_address'] for row in rows]).union(set([row['from_address'] for row in rows2]))

        addresses_to_check = set()

        old_and_unconfirmed = []

        for ethereum_address in rows:

            # check on queued transactions
            async with self.db:
                queued_transactions = await self.db.fetch(
                    "SELECT * FROM transactions "
                    "WHERE from_address = $1 "
                    "AND (status = 'new' OR status = 'queued') AND v IS NOT NULL",
                    ethereum_address)

            if queued_transactions:
                # make sure there are pending incoming transactions
                async with self.db:
                    incoming_transactions = await self.db.fetch(
                        "SELECT * FROM transactions "
                        "WHERE to_address = $1 "
                        "AND (status = 'unconfirmed' OR status = 'queued' OR status = 'new')",
                        ethereum_address)

                if not incoming_transactions:
                    log.error("ERROR: {} has transactions in it's queue, but no unconfirmed transactions!".format(ethereum_address))
                    # trigger queue processing as last resort
                    addresses_to_check.add(ethereum_address)
                else:
                    # check health of the incoming transaction
                    for transaction in incoming_transactions:
                        if transaction['v'] is None:
                            try:
                                tx = await self.eth.eth_getTransactionByHash(transaction['hash'])
                            except:
                                log.exception("Error getting transaction {} in sanity check", transaction['hash'])
                                continue
                            if tx is None:
                                log.warning("external transaction (id: {}) no longer found on nodes".format(transaction['transaction_id']))
                                await self.update_transaction(transaction['transaction_id'], 'error')
                                addresses_to_check.add(ethereum_address)
                            elif tx['blockNumber'] is not None:
                                log.warning("external transaction (id: {}) confirmed on node, but wasn't confirmed in db".format(transaction['transaction_id']))
                                await self.update_transaction(transaction['transaction_id'], 'confirmed')
                                addresses_to_check.add(ethereum_address)

                # no need to continue with dealing with unconfirmed transactions if there are queued ones
                continue

            async with self.db:
                unconfirmed_transactions = await self.db.fetch(
                    "SELECT * FROM transactions "
                    "WHERE from_address = $1 "
                    "AND status = 'unconfirmed' AND v IS NOT NULL",
                    ethereum_address)

            if unconfirmed_transactions:

                for transaction in unconfirmed_transactions:

                    # check on unconfirmed transactions first
                    if transaction['status'] == 'unconfirmed':
                        # we neehed to check the true status of unconfirmed transactions
                        # as the block monitor may be inbetween calls and not have seen
                        # this transaction to mark it as confirmed.
                        try:
                            tx = await self.eth.eth_getTransactionByHash(transaction['hash'])
                        except:
                            log.exception("Error getting transaction {} in sanity check", transaction['hash'])
                            continue

                        # sanity check to make sure the tx still exists
                        if tx is None:
                            # if not, try resubmit
                            # NOTE: it may just be an issue with load balanced nodes not seeing all pending transactions
                            # so we don't want to adjust the status of the transaction at all at this stage
                            value = parse_int(transaction['value'])
                            gas = parse_int(transaction['gas'])
                            gas_price = parse_int(transaction['gas_price'])
                            data = data_decoder(transaction['data']) if transaction['data'] else b''
                            tx = create_transaction(nonce=transaction['nonce'], value=value, gasprice=gas_price, startgas=gas,
                                                    to=transaction['to_address'], data=data,
                                                    v=parse_int(transaction['v']),
                                                    r=parse_int(transaction['r']),
                                                    s=parse_int(transaction['s']))
                            if calculate_transaction_hash(tx) != transaction['hash']:
                                log.warning("error resubmitting transaction {}: regenerating tx resulted in a different hash".format(transaction['hash']))
                            else:
                                tx_encoded = encode_transaction(tx)
                                try:
                                    await self.eth.eth_sendRawTransaction(tx_encoded)
                                    addresses_to_check.add(transaction['from_address'])
                                except Exception as e:
                                    # note: usually not critical, don't panic
                                    log.warning("error resubmitting transaction {}: {}".format(transaction['hash'], str(e)))

                        elif tx['blockNumber'] is not None:
                            # confirmed! update the status
                            await self.update_transaction(transaction['transaction_id'], 'confirmed')
                            addresses_to_check.add(transaction['from_address'])
                            addresses_to_check.add(transaction['to_address'])

                        else:

                            old_and_unconfirmed.append(transaction['hash'])

        if len(old_and_unconfirmed):
            log.warning("WARNING: {} transactions are old and unconfirmed!".format(len(old_and_unconfirmed)))

        for address in addresses_to_check:
            # make sure we don't try process any contract deployments
            if address != "0x":
                manager_dispatcher.process_transaction_queue(address)

        if frequency:
            manager_dispatcher.sanity_check(frequency).delay(frequency)

    @log_unhandled_exceptions(logger=log)
    async def update_default_gas_price(self, frequency):

        client = AsyncHTTPClient()
        try:
            resp = await client.fetch("https://ethgasstation.info/json/ethgasAPI.json")
            rval = json_decode(resp.body)

            standard_wei = None
            safelow_wei = None

            if 'average' not in rval:
                log.error("Unexpected results from EthGasStation: {}".format(resp.body))
            elif not isinstance(rval['average'], float):
                log.error("Unexpected 'average' gas price returned by EthGasStation: {}".format(rval['average']))
            else:
                gwei_x10 = int(rval['average'])
                standard_wei = gwei_x10 * 100000000

            if 'safeLow' not in rval:
                log.error("Unexpected results from EthGasStation: {}".format(resp.body))
            elif not isinstance(rval['safeLow'], float):
                log.error("Unexpected 'safeLow' gas price returned by EthGasStation: {}".format(rval['safeLow']))
            else:
                gwei_x10 = int(rval['safeLow'])
                safelow_wei = gwei_x10 * 100000000

            # sanity check the values, if safelow is greater than standard
            # then use the safe low as standard + an extra gwei of padding
            if safelow_wei > standard_wei:
                standard_wei = safelow_wei + 1000000000

            await self.redis.mset(
                'gas_station_safelow_gas_price', hex(safelow_wei),
                'gas_station_standard_gas_price', hex(standard_wei))

        except:
            log.exception("Error updating default gas price from EthGasStation")

        if frequency:
            manager_dispatcher.update_default_gas_price(frequency).delay(frequency)


class TaskManager(BaseEthServiceWorker):

    def __init__(self):
        super().__init__([(TransactionQueueHandler,)], queue_name="manager")
        configure_logger(log)

    def start_interval_services(self):
        manager_dispatcher.sanity_check(60).delay(60)
        manager_dispatcher.update_default_gas_price(60).delay(60)

    async def _work(self):
        await super()._work()
        self.start_interval_services()

if __name__ == "__main__":
    from toshieth.app import extra_service_config
    extra_service_config()
    app = TaskManager()
    app.work()
    asyncio.get_event_loop().run_forever()
