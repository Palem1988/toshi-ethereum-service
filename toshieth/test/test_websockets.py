import os
import time
import random
from datetime import datetime
from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from tornado.testing import gen_test
from toshi.test.ethereum.faucet import FaucetMixin
from toshi.ethereum.tx import sign_transaction, create_transaction, DEFAULT_STARTGAS, DEFAULT_GASPRICE, encode_transaction
from toshi.ethereum.utils import data_encoder
from toshi.sofa import parse_sofa_message
from toshi.jsonrpc.errors import JsonRPCError

from toshieth.test.test_transaction import (
    TEST_PRIVATE_KEY as TEST_ID_KEY,
    TEST_ADDRESS as TEST_ID_ADDRESS,
    TEST_PRIVATE_KEY_2 as TEST_WALLET_KEY,
    TEST_ADDRESS_2 as TEST_WALLET_ADDRESS
)

class WebsocketTest(FaucetMixin, EthServiceBaseTest):

    @gen_test(timeout=60)
    @requires_full_stack
    async def test_subscriptions(self):

        val = 761751855997712

        ws_con = await self.websocket_connect(TEST_ID_KEY)
        await ws_con.call("subscribe", [TEST_ID_ADDRESS])

        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT COUNT(*) FROM notification_registrations WHERE toshi_id = $1", TEST_ID_ADDRESS)
        self.assertEqual(row['count'], 1)

        tx_hash = await self.faucet(TEST_ID_ADDRESS, val)

        result = await ws_con.read()
        self.assertIsNotNone(result)
        payment = parse_sofa_message(result['params']['message'])
        self.assertEqual(payment['txHash'], tx_hash)
        ws_con.con.close()

        ws_con = await self.websocket_connect(TEST_ID_KEY)

        await self.faucet(TEST_ID_ADDRESS, val)

        # make sure we don't still get any notifications
        result = await ws_con.read(timeout=1)
        self.assertIsNone(result)

        # make sure subscriptions to a different address from the toshi id work
        await ws_con.call("subscribe", [TEST_WALLET_ADDRESS])

        tx_hash = await self.faucet(TEST_WALLET_ADDRESS, val)

        result = await ws_con.read()
        self.assertIsNotNone(result)
        payment = parse_sofa_message(result['params']['message'])
        self.assertEqual(payment['txHash'], tx_hash)

        await ws_con.call("unsubscribe", [TEST_WALLET_ADDRESS])

        await self.faucet(TEST_WALLET_ADDRESS, val)

        result = await ws_con.read(timeout=1)
        # handle case where confirmed comes in before the unsibscribe
        if result:
            payment = parse_sofa_message(result['params']['message'])
            self.assertEqual(payment['txHash'], tx_hash)
            self.assertEqual(payment['status'], 'confirmed')
            result = await ws_con.read(timeout=1)

        self.assertIsNone(result)

    @gen_test(timeout=60)
    @requires_full_stack
    async def test_subscriptions_without_signing(self):

        val = 761751855997712

        ws_con = await self.websocket_connect(None)
        await ws_con.call("subscribe", [TEST_ID_ADDRESS])

        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT COUNT(*) FROM notification_registrations WHERE eth_address = $1", TEST_ID_ADDRESS)
        self.assertEqual(row['count'], 1)

        tx_hash = await self.faucet(TEST_ID_ADDRESS, val)

        result = await ws_con.read()
        self.assertIsNotNone(result)
        payment = parse_sofa_message(result['params']['message'])
        self.assertEqual(payment['txHash'], tx_hash)
        ws_con.con.close()

        ws_con = await self.websocket_connect(None)

        await self.faucet(TEST_ID_ADDRESS, val)

        # make sure we don't still get any notifications
        result = await ws_con.read(timeout=1)
        self.assertIsNone(result)

        # make sure subscriptions don't cross over when no toshi id is used
        await ws_con.call("subscribe", [TEST_WALLET_ADDRESS])
        ws_con2 = await self.websocket_connect(None)

        tx_hash = await self.faucet(TEST_WALLET_ADDRESS, val)

        result = await ws_con.read()
        self.assertIsNotNone(result)
        payment = parse_sofa_message(result['params']['message'])
        self.assertEqual(payment['txHash'], tx_hash)

        # check 2nd connection has no notifications
        result = await ws_con2.read(timeout=1)
        self.assertIsNone(result)

        # make sure unsubscribe works
        await ws_con.call("unsubscribe", [TEST_WALLET_ADDRESS])

        await self.faucet(TEST_WALLET_ADDRESS, val)

        result = await ws_con.read(timeout=1)
        # handle case where confirmed comes in before the unsibscribe
        if result:
            payment = parse_sofa_message(result['params']['message'])
            self.assertEqual(payment['txHash'], tx_hash)
            self.assertEqual(payment['status'], 'confirmed')
            result = await ws_con.read(timeout=1)

        self.assertIsNone(result)

    @gen_test(timeout=60)
    @requires_full_stack
    async def test_list_subscriptions(self):

        ws_con = await self.websocket_connect(TEST_ID_KEY)

        subs = await ws_con.call("list_subscriptions")
        self.assertIsNotNone(subs)
        self.assertEqual(len(subs), 0)

        await ws_con.call("subscribe", [TEST_ID_ADDRESS])
        subs = await ws_con.call("list_subscriptions")
        self.assertIsNotNone(subs)
        self.assertEqual(len(subs), 1)
        self.assertEqual(subs[0], TEST_ID_ADDRESS)

        await ws_con.call("subscribe", [TEST_ID_ADDRESS, TEST_WALLET_ADDRESS])
        subs = await ws_con.call("list_subscriptions")
        self.assertIsNotNone(subs)
        self.assertEqual(len(subs), 2)
        self.assertEqual(set(subs), {TEST_ID_ADDRESS, TEST_WALLET_ADDRESS})

        await ws_con.call("unsubscribe", [TEST_ID_ADDRESS])
        subs = await ws_con.call("list_subscriptions")
        self.assertIsNotNone(subs)
        self.assertEqual(len(subs), 1)
        self.assertEqual(subs[0], TEST_WALLET_ADDRESS)

    @gen_test(timeout=60)
    @requires_full_stack
    async def test_send_transaction(self):

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 761751855997712

        await self.wait_on_tx_confirmation(await self.faucet(TEST_ID_ADDRESS, val * 10))

        ws_con = await self.websocket_connect(TEST_ID_KEY)

        result = await ws_con.call("create_transaction_skeleton", {
            "from": TEST_ID_ADDRESS,
            "to": addr,
            "value": val
        })
        tx = sign_transaction(result['tx'], TEST_ID_KEY)

        tx_hash = await ws_con.call("send_transaction", {
            "tx": tx
        })

        self.assertIsNotNone(tx_hash)

    @gen_test(timeout=60)
    @requires_full_stack(ethminer=True)
    async def test_get_balance(self, *, ethminer):

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 8761751855997712
        val2 = int(val / 2)

        await self.wait_on_tx_confirmation(await self.faucet(TEST_ID_ADDRESS, val))

        ws_con = await self.websocket_connect(TEST_ID_KEY)

        result = await ws_con.call("get_balance", [TEST_ID_ADDRESS])

        self.assertEqual(int(result['confirmed_balance'][2:], 16), val)
        self.assertEqual(int(result['unconfirmed_balance'][2:], 16), val)

        # make sure no blocks get mined for a bit
        ethminer.pause()

        tx = create_transaction(nonce=0x100000, gasprice=DEFAULT_GASPRICE, startgas=DEFAULT_STARTGAS,
                                to=addr, value=val2, network_id=self.network_id)
        tx = sign_transaction(tx, TEST_ID_KEY)
        tx = encode_transaction(tx)

        await ws_con.call("subscribe", [addr])

        tx_hash = await ws_con.call("send_transaction", {"tx": tx})

        new_balance = val - (val2 + DEFAULT_STARTGAS * DEFAULT_GASPRICE)

        result = await ws_con.call("get_balance", [TEST_ID_ADDRESS])
        self.assertEqual(int(result['confirmed_balance'][2:], 16), val)
        self.assertEqual(int(result['unconfirmed_balance'][2:], 16), new_balance)

        # check for the unconfirmed notification
        result = await ws_con.read()
        self.assertIsNotNone(result)
        payment = parse_sofa_message(result['params']['message'])
        self.assertEqual(payment['txHash'], tx_hash)
        self.assertEqual(payment['status'], 'unconfirmed')

        # restart mining
        ethminer.start()

        result = await ws_con.read()
        payment = parse_sofa_message(result['params']['message'])
        self.assertEqual(payment['txHash'], tx_hash)
        self.assertEqual(payment['status'], 'confirmed')

        result = await ws_con.call("get_balance", [TEST_ID_ADDRESS])

        self.assertEqual(int(result['confirmed_balance'][2:], 16), new_balance,
                         "int('{}', 16) != {}".format(result['confirmed_balance'], new_balance))
        self.assertEqual(int(result['unconfirmed_balance'][2:], 16), new_balance,
                         "int('{}', 16) != {}".format(result['unconfirmed_balance'], new_balance))

    @gen_test(timeout=60)
    @requires_full_stack
    async def test_list_payment_updates(self):

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'

        thetime = int(time.time())

        # make sure half of the unconfirmed txs
        stime = thetime - 300

        async def store_tx(status, created, updated=None):
            tx_hash = data_encoder(os.urandom(64))
            from_addr = data_encoder(os.urandom(20))
            value = random.randint(10**15, 10**20)
            if updated is None:
                updated = created
            async with self.pool.acquire() as con:
                    await con.execute(
                        "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status, created, updated) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                        tx_hash, from_addr, addr, 1, hex(value),
                        hex(DEFAULT_STARTGAS), hex(DEFAULT_GASPRICE), status,
                        datetime.utcfromtimestamp(created),
                        datetime.utcfromtimestamp(updated))

        txs_per_state = 5
        # create 5 transactions that were created before the start time, but confirmed after

        created = stime - 30
        updated = stime + 30
        for i in range(0, txs_per_state):
            await store_tx('confirmed', created + i, updated + i)

        # create 5 transactions that were both created and confirmed during the requested period
        created = stime + 30
        updated = stime + 60
        for i in range(0, txs_per_state):
            await store_tx('confirmed', created + i, updated + i)

        # create 5 transactions created but not confirmed during the requested period
        created = stime + 60
        for i in range(0, txs_per_state):
            await store_tx('unconfirmed', created + i)

        # create 5 transactions outside of the requested period
        created = thetime
        for i in range(0, txs_per_state):
            await store_tx('unconfirmed', created + i)

        ws_con = await self.websocket_connect(TEST_ID_KEY)

        result = await ws_con.call("list_payment_updates", [addr, stime, thetime])

        # expect 5 confirmed payments
        c = 0
        for i in range(0, txs_per_state):
            self.assertEqual(parse_sofa_message(result[c + i])['status'], 'confirmed')

        # expect 5 txs with both unconfirmed and confirmed
        c += txs_per_state
        for i in range(0, txs_per_state):
            self.assertEqual(parse_sofa_message(result[c + (i * 2)])['status'], 'unconfirmed')
            self.assertEqual(parse_sofa_message(result[c + (i * 2) + 1])['status'], 'confirmed')

        c += txs_per_state * 2

        # expect 5 unconfirmed txs
        for i in range(0, txs_per_state):
            self.assertEqual(parse_sofa_message(result[c + i])['status'], 'unconfirmed')

        # make sure there's no more!
        c += txs_per_state
        self.assertEqual(len(result), c)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_subscribe_with_bad_args(self):

        ws_con = await self.websocket_connect(TEST_ID_KEY)

        # make sure trying to subscribe without the params
        # being in brackets fails
        with self.assertRaises(JsonRPCError):
            await ws_con.call("subscribe", TEST_ID_ADDRESS)

        with self.assertRaises(JsonRPCError):
            await ws_con.call("subscribe", "not an address")
