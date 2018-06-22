import asyncio
import logging
import time

from tornado.escape import json_encode

from toshi.log import configure_logger, log_unhandled_exceptions
from toshi.utils import parse_int
from toshi.config import config

from toshi.ethereum.mixin import EthereumMixin
from toshi.jsonrpc.errors import JsonRPCError

from toshieth.tasks import BaseEthServiceWorker, BaseTaskHandler, manager_dispatcher, erc20_dispatcher

log = logging.getLogger("toshieth.erc20manager")

RETRY_DELAY = 10

class ERC20UpdateHandler(EthereumMixin, BaseTaskHandler):

    @log_unhandled_exceptions(logger=log)
    async def update_token_cache(self, contract_address, *eth_addresses, blocknumber=None):

        if len(eth_addresses) == 0:
            return

        is_wildcard = contract_address == "*"

        async with self.db:
            last_blocknumber = (await self.db.fetchval("SELECT blocknumber FROM last_blocknumber"))
            if blocknumber is None:
                blocknumber = last_blocknumber
            elif blocknumber > last_blocknumber:
                # don't continue until the block numbers match
                log.info("request to update erc20 cache before block processor is caught up")
                erc20_dispatcher.update_token_cache(contract_address, *eth_addresses, blocknumber=blocknumber).delay(RETRY_DELAY)
                return
            if is_wildcard:
                tokens = await self.db.fetch("SELECT contract_address, custom FROM tokens where custom = FALSE")
            else:
                tokens = await self.db.fetch("SELECT contract_address, custom FROM tokens where contract_address = $1",
                                             contract_address)

        if is_wildcard:
            if len(eth_addresses) > 1:
                # this is currently unneeded and dangerous
                raise Exception("wildcard update of token caches unsupported for multiple addresses")
            log.info("START update_token_cache(\"*\", {})".format(eth_addresses[0]))
            start_time = time.time()
            # NOTE: we don't remove this at the end on purpose
            # to avoid spamming of "*" refreshes
            should_run = await self.redis.set("bulk_token_update:{}".format(eth_addresses[0]), 1,
                                              expire=60, exist=self.redis.SET_IF_NOT_EXIST)
            if not should_run:
                log.info("ABORT update_token_cache(\"*\", {}): {}".format(eth_addresses[0], should_run))
                return

        client = self.eth.bulk(should_retry=False)
        futures = []
        for eth_address in eth_addresses:
            for token in tokens:
                data = "0x70a08231000000000000000000000000" + eth_address[2:]
                f = client.eth_call(to_address=token['contract_address'], data=data, block=blocknumber)
                futures.append((token['contract_address'], eth_address, token['custom'], f))

        if len(futures) > 0:
            should_retry = False
            try:
                await client.execute()
            except:
                log.exception("Error in http request updating erc20 cache update of '{}' for addresses: {}".format(contract_address, eth_addresses))
                futures = []
                should_retry = True

            bulk_insert = []
            for token_contract_address, eth_address, custom, f in futures:
                try:
                    value = f.result()
                    if value == "0x0000000000000000000000000000000000000000000000000000000000000000" or value == "0x":
                        if value == "0x":
                            log.warning("calling balanceOf for contract {} failed".format(token_contract_address))
                        value = 0
                    else:
                        value = parse_int(value)  # remove hex padding of value
                    bulk_insert.append((token_contract_address, eth_address, hex(value), blocknumber, 0 if custom else 1))
                except JsonRPCError as e:
                    if e.message == "Unknown Block Number" or e.message == "This request is not supported because your node is running with state pruning. Run with --pruning=archive.":
                        # reschedule the update and abort for now
                        # NOTE: not aborting right away as we should clear out the rest of the future values
                        if not should_retry:
                            log.info("got unknown block number in erc20 cache update of '{}' for address: {}".format(token_contract_address, eth_address))
                            should_retry = True
                        continue
                    log.exception("WARNING: failed to update token cache of '{}' for address: {}".format(token_contract_address, eth_address))

            if should_retry:
                if is_wildcard:
                    # clear up bulk_token_update key, as we want to allow this to run again
                    await self.redis.delete("bulk_token_update:{}".format(eth_addresses[0]))
                erc20_dispatcher.update_token_cache(contract_address, *eth_addresses, blocknumber=blocknumber).delay(RETRY_DELAY)
                return

            send_update = False
            if len(bulk_insert) > 0:
                async with self.db:
                    await self.db.executemany(
                        "INSERT INTO token_balances (contract_address, eth_address, balance, blocknumber, visibility) "
                        "VALUES ($1, $2, $3, $4, $5) "
                        "ON CONFLICT (contract_address, eth_address) "
                        "DO UPDATE SET balance = EXCLUDED.balance, blocknumber = EXCLUDED.blocknumber "
                        "WHERE token_balances.blocknumber < EXCLUDED.blocknumber",
                        bulk_insert)
                    await self.db.commit()
                    send_update = True

            # wildcard updates usually mean we need to send a refresh trigger to clients
            # currently clients only use a TokenPayment as a trigger to refresh their
            # token cache, so we abuse this functionality here
            if is_wildcard and send_update:
                # lots of fake values so it doesn't get confused with a real tx
                data = {
                    "txHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "fromAddress": "0x0000000000000000000000000000000000000000",
                    "toAddress": eth_addresses[0],
                    "status": "confirmed",
                    "value": "0x0",
                    "contractAddress": "0x0000000000000000000000000000000000000000"
                }
                message = "SOFA::TokenPayment: " + json_encode(data)
                manager_dispatcher.send_notification(eth_addresses[0], message)
        if is_wildcard:
            end_time = time.time()
            log.info("DONE update_token_cache(\"*\", {}) in {}s".format(eth_addresses[0], round(end_time - start_time, 2)))

class TaskManager(BaseEthServiceWorker):

    def __init__(self):
        super().__init__([(ERC20UpdateHandler,)], queue_name="erc20")
        configure_logger(log)

if __name__ == "__main__":
    from toshieth.app import extra_service_config
    extra_service_config()
    app = TaskManager()
    app.work()
    asyncio.get_event_loop().run_forever()
