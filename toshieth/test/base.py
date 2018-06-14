import asyncio

import toshieth.monitor
import toshieth.manager
import toshieth.push_service
import toshieth.erc20manager
import toshieth.collectibles.erc721
import toshieth.collectibles.worker
import toshieth.collectibles.fungible

from toshi.config import config
from toshi.test.base import AsyncHandlerTest
from toshi.test.base import ToshiWebSocketJsonRPCClient
from toshieth.app import Application, urls
from tornado.escape import json_decode

from toshi.ethereum.utils import private_key_to_address
from toshi.ethereum.tx import sign_transaction
from toshi.ethereum.utils import prepare_ethereum_jsonrpc_client

from toshi.test.database import requires_database
from toshi.test.redis import requires_redis
from toshi.test.ethereum.parity import requires_parity
from toshi.test.ethereum.faucet import FAUCET_PRIVATE_KEY

class EthServiceBaseTest(AsyncHandlerTest):

    APPLICATION_CLASS = Application

    def get_urls(self):
        return urls

    def get_url(self, path):
        path = "/v1{}".format(path)
        return super().get_url(path)

    async def wait_on_tx_confirmation(self, tx_hash, interval_check_callback=None):
        while True:
            tx = await self.eth.eth_getTransactionByHash(tx_hash)
            if tx is not None and tx['blockNumber'] is not None:
                break
            if interval_check_callback:
                f = interval_check_callback()
                if asyncio.iscoroutine(f):
                    await f
            await asyncio.sleep(0)
        # make sure the last_blocknumber has been saved to the db before returning
        # and if the transaction is in the database, make sure it and any matching
        # token transaactions are confirmed as it can take a few event loop cycles
        # to update the status
        while True:
            async with self.pool.acquire() as con:
                blocknumber = await con.fetchval("SELECT blocknumber FROM last_blocknumber")
                row = await con.fetchrow("SELECT transaction_id, status FROM transactions WHERE hash = $1", tx_hash)
                if row:
                    tkrow = await con.fetchrow("SELECT status FROM token_transactions WHERE transaction_id = $1", row['transaction_id'])
                else:
                    tkrow = None
            if blocknumber and blocknumber >= int(tx['blockNumber'], 16) and \
               (row is None or (row['status'] == 'confirmed' or row['status'] == 'error')) and \
               (tkrow is None or (tkrow['status'] == 'confirmed' or tkrow['status'] == 'error')):
                return tx
            await asyncio.sleep(0)

    async def get_tx_skel(self, from_key, to_addr, val, nonce=None, gas_price=None, gas=None, data=None, token_address=None, expected_response_code=200):
        from_addr = private_key_to_address(from_key)
        body = {
            "from": from_addr,
            "to": to_addr,
            "value": hex(val) if isinstance(val, int) else val
        }
        if nonce is not None:
            body['nonce'] = nonce
        if gas_price is not None:
            body['gasPrice'] = gas_price
        if gas is not None:
            body['gas'] = gas
        if data is not None:
            body['data'] = data
        if token_address is not None:
            body['token_address'] = token_address

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertResponseCodeEqual(resp, expected_response_code, resp.body)
        if expected_response_code == 200:
            body = json_decode(resp.body)
            tx = body['tx']
            return tx
        return None

    async def sign_and_send_tx(self, from_key, tx, expected_response_code=200, wait_on_tx_confirmation=False):

        tx = sign_transaction(tx, from_key)
        return await self.send_raw_tx(tx, expected_response_code=expected_response_code, wait_on_tx_confirmation=wait_on_tx_confirmation)

    async def send_tx(self, from_key, to_addr, val, nonce=None, data=None, gas=None, gas_price=None, token_address=None):

        tx = await self.get_tx_skel(from_key, to_addr, val, nonce=nonce, data=data, gas=gas, gas_price=gas_price, token_address=token_address)
        return await self.sign_and_send_tx(from_key, tx)

    async def send_raw_tx(self, tx, wait_on_tx_confirmation=True, expected_response_code=200):
        resp = await self.fetch("/tx", method="POST", body={"tx": tx})
        self.assertResponseCodeEqual(resp, expected_response_code, resp.body)
        if expected_response_code == 200:
            body = json_decode(resp.body)
            tx_hash = body['tx_hash']
            if wait_on_tx_confirmation:
                await self.wait_on_tx_confirmation(tx_hash)
            return tx_hash
        return None

    async def faucet(self, to_address, value):
        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, to_address, value)
        await self.wait_on_tx_confirmation(tx_hash)
        return tx_hash

    async def websocket_connect(self, signing_key):
        con = ToshiWebSocketJsonRPCClient(self.get_url("/ws"), signing_key=signing_key)
        await con.connect()
        return con

    @property
    def network_id(self):
        return int(config['ethereum']['network_id'])

    @property
    def eth(self):
        if 'ethereum' not in config:
            raise Exception("Missing ethereum configuration")
        if not hasattr(self, '_eth_jsonrpc_client'):
            self._eth_jsonrpc_client = prepare_ethereum_jsonrpc_client(config['ethereum'])
        return self._eth_jsonrpc_client

def requires_block_monitor(func=None, cls=toshieth.monitor.BlockMonitor, pass_monitor=False, begin_started=True):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'ethereum' not in config:
                raise Exception("Missing ethereum config from setup")

            monitor = cls()

            if begin_started:
                await monitor.start()

            if pass_monitor:
                if pass_monitor is True:
                    kwargs['monitor'] = monitor
                else:
                    kwargs[pass_monitor] = monitor

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await monitor.shutdown()

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

# overrides the start method to not trigger things that should only run when live
class TestTaskManager(toshieth.manager.TaskManager):
    def start_interval_services(self):
        pass

def requires_task_manager(func=None, pass_manager=False):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'redis' not in config:
                raise Exception("Missing redis config from setup")

            task_manager = TestTaskManager()

            task_manager.work()

            if pass_manager:
                if pass_manager is True:
                    kwargs['manager'] = task_manager
                else:
                    kwargs[pass_manager] = task_manager

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                try:
                    await task_manager.shutdown()
                except:
                    print("WARNING: task manager didn't shutdown cleanly")

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

def requires_push_service(func, cls, pass_push_service=False, pass_push_client=False):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'redis' not in config:
                raise Exception("Missing redis config from setup")

            pushclient = cls()
            push_service = toshieth.push_service.PushNotificationService(
                pushclient=pushclient)

            await push_service.work()

            if pass_push_service:
                if pass_push_service is True:
                    kwargs['push_service'] = push_service
                else:
                    kwargs[pass_push_service] = push_service
            if pass_push_client:
                if pass_push_client is True:
                    kwargs['push_client'] = pushclient
                else:
                    kwargs[pass_push_client] = pushclient

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await push_service.shutdown()

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

class MockPushClient:

    def __init__(self):
        self.send_queue = asyncio.Queue()

    async def send(self, toshi_id, network, device_toshi, data):
        if len(data) > 1 or 'message' not in data:
            raise NotImplementedError("Only data key allowed is 'message'")

        self.send_queue.put_nowait((device_toshi, data))

    def get(self):
        return self.send_queue.get()

def requires_collectible_monitor(func=None, pass_collectible_monitor=False, pass_fungible_monitor=False):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'ethereum' not in config:
                raise Exception("Missing ethereum config from setup")

            if 'collectibles' not in config:
                config['collectibles'] = {'image_format': ''}

            monitor = toshieth.collectibles.erc721.ERC721TaskManager()
            fung = toshieth.collectibles.fungible.FungibleCollectibleTaskManager()
            w = toshieth.collectibles.worker.CollectiblesWorker()
            w.add_instance(monitor)
            w.add_instance(fung)
            w.work()

            if pass_collectible_monitor:
                if pass_collectible_monitor is True:
                    kwargs['collectible_monitor'] = monitor
                else:
                    kwargs[pass_collectible_monitor] = monitor

            if pass_fungible_monitor:
                if pass_collectible_monitor is True:
                    kwargs['fungible_monitor'] = fung
                else:
                    kwargs[pass_fungible_monitor] = fung

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await monitor.shutdown()
                await w.shutdown()

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

def requires_erc20_manager(func=None, pass_erc20_manager=False, begin_started=True):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'ethereum' not in config:
                raise Exception("Missing ethereum config from setup")

            manager = toshieth.erc20manager.TaskManager()

            if begin_started:
                await manager.work()

            if pass_erc20_manager:
                if pass_erc20_manager is True:
                    kwargs['erc20_manager'] = manager
                else:
                    kwargs[pass_erc20_manager] = manager

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await manager.shutdown()

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

def requires_websocket_worker(func=None):

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'redis' not in config:
                raise Exception("Missing redis config from setup")

            worker = toshieth.websocket.EthServiceWorker()
            self._app.worker = worker
            worker.work()

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await worker.shutdown()

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

def composed(*decs):
    """Decorator to combine multiple decorators together"""
    def deco(f):
        for dec in reversed(decs):
            args = ()
            kwargs = {}
            if not callable(dec):
                dec, *xargs = dec
                if len(xargs):
                    if isinstance(xargs[0], (list, tuple)):
                        args = xargs[0]
                        xargs = xargs[1:]
                    if xargs and isinstance(xargs[0], dict):
                        kwargs = xargs[0]
                        xargs = xargs[1:]
                    if xargs:
                        raise Exception("invalid arguments")
            f = dec(f, *args, **kwargs)
        return f
    return deco

def requires_full_stack(func=None, *, redis=None, parity=None, ethminer=None, manager=None, block_monitor=None, push_client=None, erc20_manager=None, collectible_monitor=None, fungible_monitor=None):
    dec = composed(
        requires_database,
        (requires_redis, {'pass_redis': redis}),
        (requires_parity, {'pass_parity': parity, 'pass_ethminer': ethminer}),
        (requires_websocket_worker,),
        (requires_task_manager, {'pass_manager': manager}),
        (requires_block_monitor, {'pass_monitor': block_monitor}),
        (requires_push_service, (MockPushClient,), {'pass_push_client': push_client}),
        (requires_erc20_manager, {'pass_erc20_manager': erc20_manager}),
        (requires_collectible_monitor, {'pass_collectible_monitor': collectible_monitor, 'pass_fungible_monitor': fungible_monitor})
    )
    if func is None:
        return dec
    else:
        return dec(func)
