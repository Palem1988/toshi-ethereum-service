import asyncio
import warnings
import re

from tornado.escape import json_decode
from tornado.testing import gen_test
from toshi.test.ethereum.faucet import FaucetMixin
from toshi.ethereum.tx import sign_transaction, decode_transaction, signature_from_transaction
from toshi.sofa import SofaPayment, parse_sofa_message
from toshi.ethereum.utils import data_encoder

from toshieth.test.base import requires_full_stack, EthServiceBaseTest

from toshieth.test.test_transaction import (
    TEST_PRIVATE_KEY as TEST_ID_KEY,
    TEST_ADDRESS as TEST_ID_ADDRESS,
    TEST_PRIVATE_KEY_2 as TEST_WALLET_KEY,
    TEST_ADDRESS_2 as TEST_WALLET_ADDRESS
)
from toshi.test.ethereum.parity import FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshieth.test.test_pn_registration import TEST_GCM_ID, TEST_GCM_ID_2

from toshieth.tasks import manager_dispatcher

class TestSendGCMPushNotification(FaucetMixin, EthServiceBaseTest):

    @gen_test(timeout=30)
    @requires_full_stack(push_client=True)
    async def test_get_single_push_notification(self, *, push_client):
        """makes sure both source and target of a transaction get a confirmation
        push notification"""

        # register for GCM PNs
        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_WALLET_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        # register source account for pn
        body = {
            "registration_id": TEST_GCM_ID_2,
            "address": FAUCET_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=FAUCET_PRIVATE_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        async with self.pool.acquire() as con:
            rows1 = await con.fetch("SELECT * FROM notification_registrations WHERE toshi_id = $1", TEST_ID_ADDRESS)
            rows2 = await con.fetch("SELECT * FROM notification_registrations WHERE toshi_id = $1", FAUCET_ADDRESS)
        self.assertEqual(len(rows1), 1)
        self.assertEqual(len(rows2), 1)

        value = 2821181018869341261
        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_WALLET_ADDRESS, value)

        unconfirmed_count = 0
        confirmed_1 = None
        unconfirmed_1 = None
        while True:
            token, payload = await push_client.get()

            self.assertIn(token, [TEST_GCM_ID, TEST_GCM_ID_2])

            message = parse_sofa_message(payload['message'])

            self.assertIsInstance(message, SofaPayment)
            self.assertEqual(message['value'], hex(value))
            self.assertEqual(message['txHash'], tx_hash)
            self.assertEqual(message['networkId'], '66')

            if message['status'] == "confirmed":
                # ensures that the 2 expected confirmed pns are
                # meant for the different targets
                if confirmed_1:
                    self.assertNotEqual(token, confirmed_1)
                    break
                else:
                    confirmed_1 = token
                    continue

            self.assertEqual(message['status'], "unconfirmed")
            if unconfirmed_1:
                self.assertNotEqual(token, unconfirmed_1)
            else:
                unconfirmed_1 = token
            unconfirmed_count += 1
            # since this tx came from outside our system there's no guarantee we
            # even see this since the block monitor might miss it before it's
            # actually confirmed
            if unconfirmed_count > 2:
                warnings.warn("got more than one unconfirmed notification for a single transaction")

    @gen_test(timeout=30)
    @requires_full_stack(push_client=True)
    async def test_get_multiple_push_notification(self, *, push_client):

        # register for GCM PNs
        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_WALLET_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)
        # register 2nd device
        body = {
            "registration_id": TEST_GCM_ID_2,
            "address": TEST_WALLET_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT * FROM notification_registrations WHERE toshi_id = $1", TEST_ID_ADDRESS)
        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 2)

        value = 2821181018869341261
        tx_hash = await self.faucet(TEST_WALLET_ADDRESS, value)

        unconfirmed_counts = {TEST_GCM_ID: 0, TEST_GCM_ID_2: 0}
        while True:
            token, payload = await push_client.get()

            self.assertTrue(token in (TEST_GCM_ID, TEST_GCM_ID_2))

            message = parse_sofa_message(payload['message'])

            self.assertIsInstance(message, SofaPayment)
            self.assertEqual(message['value'], hex(value))
            self.assertEqual(message['txHash'], tx_hash)

            if message['status'] == "confirmed":
                break

            self.assertEqual(message['status'], "unconfirmed")
            unconfirmed_counts[token] += 1
            if unconfirmed_counts[token] > 1:
                warnings.warn("got more than one unconfirmed notification for a single device")

    @gen_test(timeout=120)
    @requires_full_stack(ethminer=True, push_client=True)
    async def test_always_get_unconfirmed_push_notification(self, *, ethminer, push_client):
        """Tests that when tx's are send through our systems we always get
        an unconfirmed push notification"""

        # register for GCM PNs
        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_WALLET_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT * FROM notification_registrations WHERE toshi_id = $1", TEST_ID_ADDRESS)
        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 1)

        # run this a bunch of times to see if
        # we can expose any race conditions
        for iteration in range(4):

            if iteration > 2:
                ethminer.pause()

            value = 2821181018869341261

            resp = await self.fetch("/tx/skel", method="POST", body={
                "from": FAUCET_ADDRESS,
                "to": TEST_WALLET_ADDRESS,
                "value": value
            })
            self.assertResponseCodeEqual(resp, 200, resp.body)
            body = json_decode(resp.body)
            tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)
            resp = await self.fetch("/tx", method="POST", body={
                "tx": tx
            })
            self.assertResponseCodeEqual(resp, 200, resp.body)
            tx_hash = json_decode(resp.body)['tx_hash']

            tx = decode_transaction(tx)
            self.assertEqual(tx_hash, data_encoder(tx.hash))

            if iteration > 2:
                await asyncio.sleep(5)
                ethminer.start()

            async with self.pool.acquire() as con:
                rows = await con.fetch("SELECT * FROM transactions WHERE nonce = $1", tx.nonce)
            self.assertEqual(len(rows), 1)

            unconfirmed_count = 0
            while True:
                token, payload = await push_client.get()

                self.assertEqual(token, TEST_GCM_ID)

                message = parse_sofa_message(payload['message'])

                self.assertIsInstance(message, SofaPayment)
                self.assertEqual(message['value'], hex(value))
                self.assertEqual(message['txHash'], tx_hash)

                if message['status'] == "confirmed":
                    break

                self.assertEqual(message['status'], "unconfirmed")
                unconfirmed_count += 1
            # when the tx is sent through our systems we should
            # always get one unconfirmed notification, and we
            # should never get more than one
            self.assertEqual(unconfirmed_count, 1)

    @gen_test(timeout=30)
    @requires_full_stack(push_client=True)
    async def test_sending_transactions_and_storing_the_hash_correctly(self, *, push_client):

        """This test is born out of the fact that `UnsingedTransaction.hash` always
        calculates the hash of the transaction without the signature, even after `.sign`
        has been called on the transaction. This caused errors in what was being stored
        in the database and incorrectly detecting transaction overwrites.

        This test exposed the behaviour correctly so that it could be fixed"""

        # register for GCM PNs
        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_ID_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ID_ADDRESS,
            "value": 10 ** 10
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)
        tx = decode_transaction(body['tx'])
        tx = sign_transaction(tx, FAUCET_PRIVATE_KEY)
        sig = signature_from_transaction(tx)

        body = {
            "tx": body['tx'],
            "signature": data_encoder(sig)
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        async def check_db():
            async with self.pool.acquire() as con:
                rows = await con.fetch("SELECT * FROM transactions WHERE nonce = $1", tx.nonce)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['hash'], tx_hash)
            if rows[0]['status'] != 'new':
                self.assertEqual(rows[0]['status'], 'unconfirmed')
            self.assertIsNone(rows[0]['error'])

        await self.wait_on_tx_confirmation(tx_hash, check_db)
        while True:
            token, payload = await push_client.get()
            message = parse_sofa_message(payload['message'])

            self.assertIsInstance(message, SofaPayment)
            self.assertEqual(message['txHash'], tx_hash)

            if message['status'] == "confirmed":
                break

class MonitorTest(FaucetMixin, EthServiceBaseTest):

    @gen_test(timeout=45)
    @requires_full_stack
    async def test_external_transactions(self):

        val = 761751855997712

        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_ID_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        tx_hash = await self.faucet(TEST_ID_ADDRESS, val)

        while True:
            async with self.pool.acquire() as con:
                row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx_hash)
            if row is not None:
                break
            await asyncio.sleep(0.1)

        # make sure value is stored as hex
        self.assertIsNotNone(re.match('^0x[a-fA-F0-9]+$', row['value']), "expected transaction value to be stored as hex. got: {}".format(row['value']))
        self.assertEqual(int(row['value'][2:], 16), val)


class MonitorErrorsTest(FaucetMixin, EthServiceBaseTest):

    @gen_test(timeout=45)
    @requires_full_stack(parity=True, push_client=True)
    async def test_parity_failures(self, *, parity, push_client):

        """Tests that the block monitor recovers correctly after errors
        in the ethereum node"""

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 761751855997712

        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_ID_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        tx_hash = await self.faucet(TEST_ID_ADDRESS, val * 10)
        for status in ['unconfirmed', 'confirmed']:
            _, pn = await push_client.get()
            message = parse_sofa_message(pn['message'])
            self.assertIsInstance(message, SofaPayment)
            self.assertEqual(message['txHash'], tx_hash)
            if status == 'unconfirmed' and message['status'] == 'confirmed':
                # this can happen if the monitor sees the confirmed message
                # first as the faucet sends raw txs
                break
            self.assertEqual(message['status'], status)

        # kill the server!
        parity.pause()

        # sleep for a bit to give the monitor time to poll
        await asyncio.sleep(5)

        # start it again
        parity.start()

        # see if we get notifications still!

        tx_hash = await self.send_tx(TEST_ID_KEY, addr, 10 ** 10)
        # wait for 2 messages, as the confirmed one should come
        # from the block monitor
        for status in ['unconfirmed', 'confirmed']:
            _, pn = await push_client.get()
            message = parse_sofa_message(pn['message'])
            self.assertIsInstance(message, SofaPayment)
            self.assertEqual(message['txHash'], tx_hash)
            self.assertEqual(message['status'], status)

    @gen_test(timeout=120)
    @requires_full_stack(redis=True, push_client=True)
    async def test_redis_failures(self, *, redis_server, push_client):

        """Tests that the block monitor recovers correctly after errors
        in the ethereum node"""

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 761751855997712

        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_ID_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        tx_hash = await self.faucet(TEST_ID_ADDRESS, val * 1000)
        for status in ['unconfirmed', 'confirmed']:
            _, pn = await push_client.get()
            message = parse_sofa_message(pn['message'])
            self.assertIsInstance(message, SofaPayment)
            self.assertEqual(message['txHash'], tx_hash)
            if status == 'unconfirmed' and message['status'] == 'confirmed':
                # this can happen if the monitor sees the confirmed message
                # first as the faucet sends raw txs
                break
            self.assertEqual(message['status'], status)

        # do this 10 times to see if the pool is working as expected as well
        for _ in range(0, 10):

            # kill the server!
            redis_server.pause()

            await asyncio.sleep(0.1)

            # start it again
            redis_server.start()

            # see if everything still works
            tx_hash = await self.send_tx(TEST_ID_KEY, addr, 10 ** 10)
            # wait for 2 messages, as the confirmed one should come
            # from the block monitor
            for status in ['unconfirmed', 'confirmed']:
                _, pn = await push_client.get()
                message = parse_sofa_message(pn['message'])
                self.assertIsInstance(message, SofaPayment)
                self.assertEqual(message['txHash'], tx_hash)
                self.assertEqual(message['status'], status)

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor='monitor')
    async def test_filter_timeout(self, *, monitor):
        # give some time for the monitor to start up
        await asyncio.sleep(0.3)

        pending_filter_id = monitor._new_pending_transaction_filter_id = "0x1111111111"
        monitor._new_block_filter_id = "0x1111111111111"
        await asyncio.sleep(0.3)
        last_block_number = monitor.last_block_number
        monitor._last_saw_new_block -= 300
        while monitor.last_block_number == last_block_number:
            await asyncio.sleep(0.1)

        monitor._last_saw_new_pending_transactions -= 300
        while monitor._new_pending_transaction_filter_id == pending_filter_id:
            await asyncio.sleep(0.1)

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor='monitor')
    async def test_sanity_check(self, *, monitor):
        # give some time for the monitor to start up
        await asyncio.sleep(0.3)

        resp = await self.fetch("/status")
        self.assertEqual(resp.code, 200)
        self.assertNotEqual(resp.body.decode('utf-8'), 'OK')

        monitor._new_pending_transaction_filter_id = None
        while monitor._new_pending_transaction_filter_id is None:
            await asyncio.sleep(0.1)

        resp = await self.fetch("/status")
        self.assertEqual(resp.code, 200)

        self.assertEqual(resp.body.decode('utf-8'), 'OK')

class ManagerTest(FaucetMixin, EthServiceBaseTest):

    @gen_test(timeout=45)
    @requires_full_stack(block_monitor='monitor', push_client=True)
    async def test_eth_nodes_out_of_sync_on_confirm(self, *, monitor, push_client):

        # shutdown the monitor so that the confirmed transaction status can be
        # manually triggered before the transaction is actually confirmed
        await monitor.shutdown()

        val = 761751855997712

        body = {
            "registration_id": TEST_GCM_ID,
            "address": TEST_ID_ADDRESS
        }
        resp = await self.fetch_signed("/gcm/register", signing_key=TEST_ID_KEY, method="POST", body=body)
        self.assertResponseCodeEqual(resp, 204, resp.body)

        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ID_ADDRESS, val)
        tx_id = None
        while tx_id is None:
            async with self.pool.acquire() as con:
                tx_id = await con.fetchval("SELECT transaction_id FROM transactions WHERE hash = $1", tx_hash)
        # force confirmed, even though it should take some time
        manager_dispatcher.update_transaction(tx_id, 'confirmed')

        _, pn = await push_client.get()
        message = parse_sofa_message(pn['message'])
        self.assertIsInstance(message, SofaPayment)
        self.assertEqual(message['txHash'], tx_hash)
        self.assertEqual(message['status'], 'unconfirmed')
        _, pn = await push_client.get()
        message = parse_sofa_message(pn['message'])
        self.assertIsInstance(message, SofaPayment)
        self.assertEqual(message['txHash'], tx_hash)
        self.assertEqual(message['status'], 'confirmed')
