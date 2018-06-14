import asyncio
import time
from tornado.escape import json_decode, json_encode
from tornado.testing import gen_test
from tornado.platform.asyncio import to_asyncio_future

from toshieth.test.base import EthServiceBaseTest, requires_task_manager, requires_full_stack
from toshi.test.database import requires_database
from toshi.test.redis import requires_redis
from toshi.test.ethereum.parity import requires_parity, FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshi.analytics import encode_id
from toshi.request import sign_request
from toshi.ethereum.utils import data_decoder, data_encoder
from toshi.ethereum.tx import create_transaction, sign_transaction, decode_transaction, signature_from_transaction, encode_transaction, DEFAULT_STARTGAS, DEFAULT_GASPRICE
from toshi.utils import parse_int

TEST_PRIVATE_KEY = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_ADDRESS = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"

TEST_PRIVATE_KEY_2 = data_decoder("0x0ffdb88a7a0a40831ca0b19bd31f3f6085764ef8b7db1bd6b57072e5eaea24ff")
TEST_ADDRESS_2 = "0x35351b44e03ec8515664a955146bf9c6e503a381"

class TransactionTest(EthServiceBaseTest):

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_transaction(self):

        val = 10 ** 10
        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": val
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)

        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        # ensure we get a tracking events
        self.assertEqual((await self.next_tracking_event())[0], None)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        tx = decode_transaction(tx)
        self.assertEqual(tx_hash, data_encoder(tx.hash))

        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT * FROM transactions WHERE nonce = $1", tx.nonce)
        self.assertEqual(len(rows), 1)

        # wait for a push notification
        await self.wait_on_tx_confirmation(tx_hash)
        while True:
            async with self.pool.acquire() as con:
                row = await con.fetchrow("SELECT * FROM transactions WHERE nonce = $1", tx.nonce)
            if row['status'] == 'confirmed':
                break

        # make sure updated field is updated
        self.assertGreater(row['updated'], row['created'])

        # make sure balance is returned correctly
        resp = await self.fetch('/balance/{}'.format(TEST_ADDRESS))
        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']), val)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_transaction_with_separate_sig(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
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

        # ensure we get a tracking events
        self.assertEqual((await self.next_tracking_event())[0], None)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        async def check_db():
            async with self.pool.acquire() as con:
                rows = await con.fetch("SELECT * FROM transactions WHERE nonce = $1", tx.nonce)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['hash'], tx_hash)

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_signed_transaction_with_separate_sig(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)
        tx = decode_transaction(body['tx'])
        tx = sign_transaction(tx, FAUCET_PRIVATE_KEY)
        sig = signature_from_transaction(tx)

        body = {
            "tx": encode_transaction(tx),
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

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_multiple_transactions(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10
        }

        tx_hashes = []

        resp = await self.fetch("/balance/{}".format(FAUCET_ADDRESS))
        last_balance = json_decode(resp.body)['unconfirmed_balance']

        for i in range(10):
            resp = await self.fetch("/tx/skel", method="POST", body=body)

            self.assertEqual(resp.code, 200)

            tx = sign_transaction(json_decode(resp.body)['tx'], FAUCET_PRIVATE_KEY)

            resp = await self.fetch("/tx", method="POST", body={
                "tx": tx
            })

            self.assertEqual(resp.code, 200, resp.body)

            tx_hash = json_decode(resp.body)['tx_hash']
            tx_hashes.append(tx_hash)

            resp = await self.fetch("/balance/{}".format(FAUCET_ADDRESS))
            balance = json_decode(resp.body)['unconfirmed_balance']
            # ensure the unconfirmed balance is changing with each request
            self.assertNotEqual(balance, last_balance)

        for tx_hash in tx_hashes:
            await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_empty_account(self):
        """Makes sure an account can be emptied completely"""

        val = 10 ** 16
        default_fees = DEFAULT_STARTGAS * DEFAULT_GASPRICE

        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS, val)
        tx = await self.wait_on_tx_confirmation(tx_hash)

        resp = await self.fetch('/balance/{}'.format(TEST_ADDRESS))
        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']), val)

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": TEST_ADDRESS,
            "to": FAUCET_ADDRESS,
            "value": val - default_fees
        })
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], TEST_PRIVATE_KEY)
        resp = await self.fetch("/tx", method="POST", body={
            "tx": tx
        })
        self.assertEqual(resp.code, 200, resp.body)
        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        # wait for a push notification
        tx = await self.wait_on_tx_confirmation(tx_hash)

        # make sure balance is returned correctly (and is 0)
        resp = await self.fetch('/balance/{}'.format(TEST_ADDRESS))
        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), 0)
        self.assertEqual(parse_int(data['unconfirmed_balance']), 0)

    @gen_test
    @requires_database
    @requires_parity
    async def test_invalid_transaction(self):

        resp = await self.fetch("/tx/{}".format("0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240c"))
        self.assertEqual(resp.code, 404)

        resp = await self.fetch("/tx/{}".format("0x2f321aa116146a9bc62b61c7340c9e613ebfc27e33f240c"))
        self.assertEqual(resp.code, 404)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_transactions_with_known_sender_toshi_id(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        tx = sign_transaction(json_decode(resp.body)['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch_signed("/tx", signing_key=TEST_PRIVATE_KEY_2, method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        # ensure we get a tracking events with a live id
        self.assertEqual((await self.next_tracking_event())[0], encode_id(TEST_ADDRESS_2))

        tx_hash = json_decode(resp.body)['tx_hash']

        async with self.pool.acquire() as con:

            row = await con.fetch("SELECT * FROM transactions WHERE sender_toshi_id = $1", FAUCET_ADDRESS)

            self.assertEqual(len(row), 0)

            row = await con.fetch("SELECT * FROM transactions WHERE sender_toshi_id = $1", TEST_ADDRESS_2)

            self.assertEqual(len(row), 1)
            self.assertEqual(row[0]['from_address'], FAUCET_ADDRESS)

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_transactions_with_known_sender_toshi_id_but_invalid_signature(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        tx = sign_transaction(json_decode(resp.body)['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        timestamp = int(time.time())
        signature = sign_request(FAUCET_PRIVATE_KEY, "POST", "/v1/tx", timestamp, json_encode(body).encode('utf-8'))

        resp = await self.fetch_signed("/tx", method="POST", body=body,
                                       address=TEST_ADDRESS_2, signature=signature, timestamp=timestamp)

        self.assertEqual(resp.code, 400, resp.body)
        self.assertIsNotNone(resp.body)
        error = json_decode(resp.body)
        self.assertIn('errors', error)
        self.assertEqual(len(error['errors']), 1)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_transaction_with_data(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10,
            "data": "0xffffffff"
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)

        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_transaction_with_0_value_and_data(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 0,
            "data": "0xffffffff"
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)

        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_transaction_with_no_value_and_data(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "data": "0xffffffff"
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)

        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_transaction_with_large_data(self):

        body = {
            "from": "0x0004DE837Ea93edbE51c093f45212AB22b4B35fc",
            "to": "0xa0c4d49fe1a00eb5ee3d85dc7a287d84d8c66699",
            "value": 0,
            "data": "0x94d9cf8f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000003c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        tx = sign_transaction(json_decode(resp.body)['tx'], FAUCET_PRIVATE_KEY)
        print(", ".join(["0x{:02x}".format(b) for b in data_decoder(tx)]))
        print(FAUCET_ADDRESS)
        print(", ".join(["0x{:02x}".format(b) for b in decode_transaction(tx).hash]))

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_tx_with_no_to_address(self):

        body = {
            "from": FAUCET_ADDRESS,
            "data": data_encoder(b"Hello World"),
            "value": 0,
            "gas": 530000,
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)

        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        tx = decode_transaction(tx)
        self.assertEqual(tx_hash, data_encoder(tx.hash))

        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT * FROM transactions WHERE nonce = $1", tx.nonce)
        self.assertEqual(len(rows), 1)
        print(rows[0])

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_and_send_transaction_with_custom_values(self):

        # try creating a skel that is invalid
        # (should be invalid due to amount of gas being too low)
        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "gas": 21000,
            "gasPrice": 20000000000,
            "data": "0xffffffff"
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)
        self.assertEqual(resp.code, 400)

        # make sure valid values are fine (incresed max gas and let skel pick the nonce)
        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "gas": 25000,
            "gasPrice": 20000000000,
            "data": "0xffffffff"
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertEqual(resp.code, 200)

        body = json_decode(resp.body)

        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertEqual(resp.code, 200, resp.body)

        body = json_decode(resp.body)
        tx_hash = body['tx_hash']

        await self.wait_on_tx_confirmation(tx_hash)

    @gen_test(timeout=30)
    @requires_database
    @requires_redis
    @requires_parity
    async def test_transaction_nonce_lock(self):
        """Spams transactions with the same nonce, and ensures the server rejects all but one"""

        no_tests = 20

        txs = []
        tx = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_ADDRESS, 10 ** 10)
        dtx = decode_transaction(tx)
        txs.append(sign_transaction(tx, FAUCET_PRIVATE_KEY))
        for i in range(11, 10 + no_tests):
            tx = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_ADDRESS, 10 ** i)
            self.assertEqual(decode_transaction(tx).nonce, dtx.nonce)
            txs.append(sign_transaction(tx, FAUCET_PRIVATE_KEY))

        responses = await asyncio.gather(*(to_asyncio_future(self.fetch("/tx", method="POST", body={"tx": tx})) for tx in txs))

        ok = 0
        bad = 0
        for resp in responses:
            if resp.code == 200:
                ok += 1
            else:
                bad += 1
        self.assertEqual(ok, 1)
        self.assertEqual(bad, no_tests - 1)

        # TODO: deal with lingering ioloop tasks better
        await asyncio.sleep(1)

    @gen_test(timeout=30)
    @requires_database
    @requires_redis
    @requires_task_manager
    @requires_parity
    async def test_prevent_out_of_order_txs(self):
        """Spams transactions with the same nonce, and ensures the server rejects all but one"""

        tx1 = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_ADDRESS, 10 ** 10)
        dtx1 = decode_transaction(tx1)
        stx1 = sign_transaction(tx1, FAUCET_PRIVATE_KEY)
        tx2 = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_ADDRESS, 10 ** 10, dtx1.nonce + 1)
        stx2 = sign_transaction(tx2, FAUCET_PRIVATE_KEY)

        resp = await self.fetch("/tx", method="POST", body={"tx": stx2})
        self.assertEqual(resp.code, 400, resp.body)

        resp = await self.fetch("/tx", method="POST", body={"tx": stx1})
        self.assertEqual(resp.code, 200, resp.body)
        resp = await self.fetch("/tx", method="POST", body={"tx": stx2})
        self.assertEqual(resp.code, 200, resp.body)

        # lets the transaction queue processing run before ending the test
        await asyncio.sleep(1)

    @gen_test(timeout=30)
    @requires_database
    @requires_redis
    @requires_task_manager
    @requires_parity
    async def test_trying_to_create_negative_value_txs(self):
        """Ensures Attempting to request a negative balance transaction skeleton
        fails gracefully"""

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": -(10 ** 18)
        })

        self.assertResponseCodeEqual(resp, 400, resp.body)

    @gen_test(timeout=15)
    @requires_full_stack
    async def test_only_from_and_to_required(self):

        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)
        resp = await self.fetch("/tx", method="POST", body={
            "tx": tx
        })
        self.assertEqual(resp.code, 200, resp.body)

        # ensure we get a tracking events
        self.assertEqual((await self.next_tracking_event())[0], None)

    @gen_test(timeout=15)
    @requires_full_stack
    async def test_insufficient_funds(self):

        body = {
            "to": FAUCET_ADDRESS,
            "from": TEST_ADDRESS,
            "value": 10 ** 18
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], TEST_PRIVATE_KEY)
        resp = await self.fetch("/tx", method="POST", body={
            "tx": tx
        })
        self.assertEqual(resp.code, 400, resp.body)
        body = json_decode(resp.body)
        self.assertEqual(len(body['errors']), 1)
        print(body)
        self.assertEqual(body['errors'][0]['id'], 'insufficient_funds')

    @gen_test(timeout=15)
    @requires_full_stack
    async def test_gas_station_gas_price(self):

        gas_price = 50000000000
        assert(gas_price != DEFAULT_GASPRICE)
        await self.redis.set("gas_station_standard_gas_price", hex(gas_price))
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO from_address_gas_price_whitelist (address) VALUES ($1)", FAUCET_ADDRESS)
        body = {
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)
        self.assertEqual(resp.code, 200)
        result = json_decode(resp.body)
        self.assertEqual(result['gas_price'], hex(gas_price))

        # make sure specifying a specific gas price still works
        body['gas_price'] = DEFAULT_GASPRICE

        resp = await self.fetch("/tx/skel", method="POST", body=body)
        self.assertEqual(resp.code, 200)
        result = json_decode(resp.body)
        self.assertEqual(result['gas_price'], hex(DEFAULT_GASPRICE))

    @gen_test(timeout=10)
    @requires_full_stack
    async def test_skeleton_address_checksum(self):

        invalid_address = "0xCd2a3d9f938e13Cd947eC05ABC7fe734df8DD826"
        only_lower_case = TEST_ADDRESS
        valid_address = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": only_lower_case,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": valid_address,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": invalid_address,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 400)

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": only_lower_case,
            "to": FAUCET_ADDRESS,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": valid_address,
            "to": FAUCET_ADDRESS,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": invalid_address,
            "to": FAUCET_ADDRESS,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 400)

    @gen_test(timeout=10)
    @requires_full_stack
    async def test_gas_estimate_buffer(self):
        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10
        })
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(int(body['gas'], 16), 21000,
                         "gas price should not be buffered for standard transactions")

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS,
            "value": 10 ** 10,
            "data": "0x1"
        })
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(int(body['gas'], 16), int(21068 * 1.2))

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_create_skeleton_with_whitespace_in_address(self):

        val = 10 ** 10
        body = {
            "from": FAUCET_ADDRESS,
            "to": "{}\n".format(TEST_ADDRESS),
            "value": val
        }

        resp = await self.fetch("/tx/skel", method="POST", body=body)
        self.assertEqual(resp.code, 200)

    @gen_test(timeout=60)
    @requires_full_stack(block_monitor=True, manager=True)
    async def test_transactions_get_queued_in_queue_is_weird(self, *, monitor, manager):
        """This test reproduces a bug where if there was a transaction with too low a gas price queued,
        the monitor sanity check would force any further queued transactions with a good gas price and
        cause the previous transaction to the error state due to the nonce being problematic.

        This tests that even if the queue gets in a weird state like this it still gets resolved correctly
        """

        gas_price = 50000000000
        safe_low_gas_price = 40000000000
        too_low_gas_price = 30000000000
        assert(gas_price != DEFAULT_GASPRICE)
        assert(gas_price > safe_low_gas_price)
        assert(safe_low_gas_price > too_low_gas_price)
        await self.redis.set("gas_station_standard_gas_price", hex(gas_price))
        await self.redis.set("gas_station_safelow_gas_price", hex(safe_low_gas_price))

        for nonce, gasprice, value in [(0, too_low_gas_price, 0x1000000000000),
                                       (1, safe_low_gas_price, 0x2000000000000),
                                       (2, gas_price, 0x3000000000000)]:

            tx = create_transaction(nonce=0x100000 + nonce, gasprice=gasprice, startgas=DEFAULT_STARTGAS,
                                    to=TEST_ADDRESS, value=value, network_id=0x42)
            await self.sign_and_send_tx(FAUCET_PRIVATE_KEY, encode_transaction(tx))

        await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            txs = await con.fetch("SELECT * FROM transactions")

        for tx in txs:
            self.assertEqual(tx['status'], 'queued')

        # purposefully cause some problems
        async with self.pool.acquire() as con:
            txs = await con.fetch("UPDATE transactions SET status = 'unconfirmed' WHERE nonce > $1", 0x100000)

        from toshieth.tasks import manager_dispatcher
        manager_dispatcher.process_transaction_queue(FAUCET_ADDRESS)

        await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            tx = await con.fetchrow("SELECT * FROM transactions WHERE nonce = $1", 0x100000)

        self.assertEqual(tx['status'], 'queued')

        # fix up gas prices
        await self.redis.set("gas_station_standard_gas_price", hex(safe_low_gas_price))
        await self.redis.set("gas_station_safelow_gas_price", hex(too_low_gas_price))

        manager_dispatcher.process_transaction_queue(FAUCET_ADDRESS)

        await self.wait_on_tx_confirmation(tx['hash'])

        async with self.pool.acquire() as con:
            tx = await con.fetchrow("SELECT * FROM transactions WHERE nonce = $1", 0x100000)

        self.assertEqual(tx['status'], 'confirmed')

        # NOTE: this test will not shutdown the task manger correctly as there is a retry
        # on the queued transaction set up
