import asyncio
import logging
import random
import time
from toshi.log import configure_logger, log_unhandled_exceptions
from toshi.ethereum.utils import data_decoder
from ethereum.utils import sha3
from ethereum.abi import decode_abi, process_type, decode_single
from toshi.utils import parse_int
from toshieth.collectibles.base import CollectiblesTaskManager
from urllib.parse import urlparse
from tornado.httpclient import AsyncHTTPClient
from tornado.escape import json_decode
from toshi.jsonrpc.errors import JsonRPCError

log = logging.getLogger("toshieth.fungible")

ASSET_CREATED_TOPIC = "0xa34547120a941eab43859acf535a121237e5536fd476dccda8174fb1af6926ed"
ASSET_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOKEN_URI_CALL_DATA = "0x" + sha3("tokenURI()")[:4].hex() + "0" * 56
NAME_CALL_DATA = "0x" + sha3("name()")[:4].hex() + "0" * 56
CREATOR_CALL_DATA = "0x" + sha3("creator()")[:4].hex() + "0" * 56
TOTAL_SUPPLY_CALL_DATA = "0x" + sha3("totalSupply()")[:4].hex() + "0" * 56

class FungibleCollectibleTaskManager(CollectiblesTaskManager):

    def __init__(self):
        super().__init__()
        configure_logger(log)
        self._processing = {}
        self._queue = set()

    async def process_block(self, blocknumber=None):

        async with self.pool.acquire() as con:
            latest_block_number = await con.fetchval(
                "SELECT blocknumber FROM last_blocknumber")
            if latest_block_number is None:
                log.warning("no blocks processed by block monitor yet")
                self._processing = False
                return

            collectibles = await con.fetch("SELECT * FROM collectibles WHERE type = 2")
            fungible_collectibles = await con.fetch("SELECT * FROM fungible_collectibles")

        for collectible in collectibles:
            asyncio.get_event_loop().create_task(self.process_block_for_asset_creation_contract(collectible['contract_address']))
        # TODO: move this to a different process as it will not scale this way once the number of fungible collectibles grows a lot
        for collectible in fungible_collectibles:
            asyncio.get_event_loop().create_task(self.process_block_for_asset_contract(collectible['contract_address']))
            await asyncio.sleep(random.random() / 10)

    @log_unhandled_exceptions(logger=log)
    async def process_block_for_asset_creation_contract(self, collectible_address):

        if collectible_address in self._processing and not self._processing[collectible_address].done():
            log.debug("Already processing {}".format(collectible_address))
            self._queue.add(collectible_address)
            return

        self._processing[collectible_address] = asyncio.Task.current_task()

        async with self.pool.acquire() as con:
            latest_block_number = await con.fetchval(
                "SELECT blocknumber FROM last_blocknumber")
            collectible = await con.fetchrow("SELECT * FROM collectibles WHERE contract_address = $1",
                                             collectible_address)

        from_block_number = collectible['last_block'] + 1

        if latest_block_number < from_block_number:
            del self._processing[collectible_address]
            return

        to_block_number = min(from_block_number + 1000, latest_block_number)

        topics = [[ASSET_CREATED_TOPIC]]

        log.debug("Getting logs for {} from blocks {}->{}".format(collectible_address, from_block_number, to_block_number))
        req_start = time.time()
        while True:
            try:
                logs = await self.eth.eth_getLogs(
                    fromBlock=from_block_number, toBlock=to_block_number,
                    topics=topics,
                    address=collectible['contract_address'])
                if time.time() - req_start > 10:
                    log.warning("eth_getLogs(fromBlock={}, toBlock={}, topics={}, address={}) took {} seconds to complete".format(
                        from_block_number, to_block_number, topics, collectible['contract_address'], time.time() - req_start))
                break
            except JsonRPCError as e:
                if e.message != "Unknown block number":
                    log.exception("unexpected error getting logs for fungible creation contract: {} (after {} seconds)".format(collectible_address, time.time() - req_start))
                await asyncio.sleep(random.random())
                continue
            except:
                log.exception("unexpected error getting logs for fungible creation contract: {} (after {} seconds)".format(collectible_address, time.time() - req_start))
                await asyncio.sleep(random.random())
                continue

        if len(logs):

            log.debug("Found {} logs for {} in blocks {}->{}".format(len(logs), collectible_address, from_block_number, to_block_number))

            for i, _log in enumerate(logs):
                log_block_number = int(_log['blockNumber'], 16)
                if log_block_number < from_block_number or log_block_number > to_block_number:
                    log.error("go unexpected block number in logs: {} (fromBlock={}, toBlock={}, collectible_address={})".format(
                        log_block_number, from_block_number, to_block_number, collectible['contract_address']))
                    del self._processing[collectible_address]
                    return

                topic = _log['topics'][0]

                if topic != ASSET_CREATED_TOPIC:
                    continue

                asset_contract_address = decode_single(
                    process_type('address'), data_decoder(_log['topics'][1]))

                try:
                    token_uri_data = await self.eth.eth_call(to_address=asset_contract_address, data=TOKEN_URI_CALL_DATA)
                except:
                    log.exception("Error getting token uri for fungible collectible asset {}".format(asset_contract_address))
                    continue
                asset_token_uri = decode_abi(['string'], data_decoder(token_uri_data))
                try:
                    asset_token_uri = asset_token_uri[0].decode('utf-8', errors='replace')
                except:
                    log.exception("Invalid tokenURI for fungible collectible asset {}".format(asset_contract_address))
                    continue
                try:
                    name_data = await self.eth.eth_call(to_address=asset_contract_address, data=NAME_CALL_DATA)
                except:
                    log.exception("Error getting name for fungible collectible asset {}".format(asset_contract_address))
                    continue
                asset_name = decode_abi(['string'], data_decoder(name_data))
                try:
                    asset_name = asset_name[0].decode('utf-8', errors='replace')
                except:
                    log.exception("Invalid name for fungible collectible asset {}".format(asset_contract_address))
                    continue

                try:
                    creator_data = await self.eth.eth_call(to_address=asset_contract_address, data=CREATOR_CALL_DATA)
                except:
                    log.exception("Error getting creator for fungible collectible asset {}".format(asset_contract_address))
                    continue
                asset_creator = decode_abi(['address'], data_decoder(creator_data))[0]
                try:
                    total_supply_data = await self.eth.eth_call(to_address=asset_contract_address, data=TOTAL_SUPPLY_CALL_DATA)
                except:
                    log.exception("Error getting total supply for fungible collectible asset {}".format(asset_contract_address))
                    continue
                total_supply = decode_abi(['uint256'], data_decoder(total_supply_data))[0]

                # owner is currently always the address that triggered the AssetCreate event
                tx = await self.eth.eth_getTransactionByHash(_log['transactionHash'])
                asset_owner = tx['from']

                asset_image = None
                asset_description = None
                parsed_uri = urlparse(asset_token_uri)
                if asset_token_uri and parsed_uri.netloc and parsed_uri.scheme in ['http', 'https']:
                    try:
                        resp = await AsyncHTTPClient(max_clients=100).fetch(parsed_uri.geturl())
                        metadata = json_decode(resp.body)
                        if "properties" in metadata:
                            metadata = metadata['properties']
                        if 'name' in metadata:
                            if type(metadata['name']) == dict and 'description' in metadata['name']:
                                asset_name = metadata['name']['description']
                            elif type(metadata['name']) == str:
                                asset_name = metadata['name']
                        if 'description' in metadata:
                            if type(metadata['description']) == dict and 'description' in metadata['description']:
                                asset_description = metadata['description']['description']
                            elif type(metadata['description']) == str:
                                asset_description = metadata['description']
                        if 'image' in metadata:
                            if type(metadata['image']) == dict and 'description' in metadata['image']:
                                asset_image = metadata['image']['description']
                            elif type(metadata['image']) == str:
                                asset_image = metadata['image']
                    except:
                        log.exception("Error getting token metadata for {}:{} from {}".format(
                            collectible_address, asset_contract_address, asset_token_uri))
                        pass

                if asset_image is None:
                    if collectible['image_url_format_string'] is not None:
                        asset_image = collectible['image_url_format_string'].format(
                            contract_address=asset_contract_address,
                            collectible_address=collectible_address,
                            name=asset_name,
                            token_uri=asset_token_uri,
                            creator_address=asset_creator)

                async with self.pool.acquire() as con:
                    await con.execute(
                        "INSERT INTO fungible_collectibles (contract_address, collectible_address, name, description, token_uri, creator_address, last_block, image) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                        "ON CONFLICT (contract_address) DO NOTHING",
                        asset_contract_address, collectible_address, asset_name, asset_description, asset_token_uri, asset_creator, log_block_number, asset_image)
                    await con.execute(
                        "INSERT INTO fungible_collectible_balances (contract_address, owner_address, balance) "
                        "VALUES ($1, $2, $3)",
                        asset_contract_address, asset_owner, hex(total_supply))
                asyncio.get_event_loop().create_task(self.process_block_for_asset_contract(asset_contract_address))

        else:
            log.debug("No logs found for {} in blocks {}->{}".format(collectible_address, from_block_number, to_block_number))

        ready = collectible['ready'] or to_block_number == latest_block_number

        async with self.pool.acquire() as con:
            await con.execute("UPDATE collectibles SET last_block = $1, ready = $2 WHERE contract_address = $3",
                              to_block_number, ready, collectible_address)

        del self._processing[collectible_address]
        if to_block_number < latest_block_number or collectible_address in self._queue:
            self._queue.discard(collectible_address)
            asyncio.get_event_loop().create_task(self.process_block_for_asset_creation_contract(collectible_address))

    @log_unhandled_exceptions(logger=log)
    async def process_block_for_asset_contract(self, contract_address):

        if contract_address in self._processing and not self._processing[contract_address].done():
            log.debug("Already processing {}".format(contract_address))
            self._queue.add(contract_address)
            return

        self._processing[contract_address] = asyncio.Task.current_task()

        async with self.pool.acquire() as con:
            latest_block_number = await con.fetchval(
                "SELECT blocknumber FROM last_blocknumber")
            collectible = await con.fetchrow("SELECT * FROM fungible_collectibles WHERE contract_address = $1",
                                             contract_address)

        from_block_number = collectible['last_block'] + 1

        if latest_block_number < from_block_number:
            del self._processing[contract_address]
            return

        to_block_number = min(from_block_number + 1000, latest_block_number)

        topics = [[ASSET_TRANSFER_TOPIC]]

        updates = {}

        req_start = time.time()
        while True:
            try:
                logs = await self.eth.eth_getLogs(
                    fromBlock=from_block_number, toBlock=to_block_number,
                    topics=topics,
                    address=contract_address)
                if time.time() - req_start > 10:
                    log.warning("eth_getLogs(fromBlock={}, toBlock={}, topics={}, address={}) took {} seconds to complete".format(
                        from_block_number, to_block_number, topics, contract_address, time.time() - req_start))
                break
            except JsonRPCError as e:
                if e.message != "Unknown block number":
                    log.exception("unexpected error getting logs for fungible asset contract: {} (after {} seconds)".format(contract_address, time.time() - req_start))
                await asyncio.sleep(random.random())
                continue
            except:
                log.exception("unexpected error getting logs for fungible asset contract: {} (after {} seconds)".format(contract_address, time.time() - req_start))
                # backoff randomly
                await asyncio.sleep(random.random())
                continue

        if len(logs):

            for i, _log in enumerate(logs):
                log_block_number = int(_log['blockNumber'], 16)
                if log_block_number < from_block_number or log_block_number > to_block_number:
                    log.error("go unexpected block number in logs: {} (fromBlock={}, toBlock={}, address={})".format(
                        log_block_number, from_block_number, to_block_number, contract_address))
                    del self._processing[contract_address]
                    return

                topic = _log['topics'][0]

                if topic == ASSET_TRANSFER_TOPIC:
                    indexed_data = _log['topics'][1:]
                    data_types = ['uint256']
                    try:
                        data = decode_abi(data_types, data_decoder(_log['data']))
                    except:
                        log.exception("Error decoding log data: {} {}".format(data_types, _log['data']))
                        del self._processing[contract_address]
                        return
                    arguments = []
                    try:
                        for t, i in [('address', True), ('address', True), ('uint256', False)]:
                            if i is True:
                                arguments.append(decode_single(process_type(t), data_decoder(indexed_data.pop(0))))
                            else:
                                arguments.append(data.pop(0))
                    except:
                        log.exception("Error compiling event data")
                        log.info("LOG: {}".format(_log))
                        del self._processing[contract_address]
                        return

                    from_address = arguments[0]
                    to_address = arguments[1]
                    value = parse_int(arguments[2])

                    async with self.pool.acquire() as con:
                        if from_address and from_address not in updates:
                            balance = await con.fetchval(
                                "SELECT balance FROM fungible_collectible_balances WHERE contract_address = $1 AND owner_address = $2",
                                contract_address, from_address)
                            updates[from_address] = parse_int(balance) if balance is not None else 0

                        if to_address not in updates:
                            balance = await con.fetchval(
                                "SELECT balance FROM fungible_collectible_balances WHERE contract_address = $1 AND owner_address = $2",
                                contract_address, to_address)
                            updates[to_address] = parse_int(balance) if balance is not None else 0

                    updates[from_address] -= value
                    updates[to_address] += value

            if len(updates) > 0:
                async with self.pool.acquire() as con:
                    await con.executemany(
                        "INSERT INTO fungible_collectible_balances (contract_address, owner_address, balance) "
                        "VALUES ($1, $2, $3) "
                        "ON CONFLICT (contract_address, owner_address) DO UPDATE "
                        "SET balance = EXCLUDED.balance",
                        [(contract_address, address, hex(value)) for address, value in updates.items()])

        ready = collectible['ready'] or to_block_number == latest_block_number

        async with self.pool.acquire() as con:
            await con.execute("UPDATE fungible_collectibles SET last_block = $1, ready = $2 WHERE contract_address = $3",
                              to_block_number, ready, contract_address)

        del self._processing[contract_address]
        if to_block_number < latest_block_number or contract_address in self._queue:
            self._queue.discard(contract_address)
            asyncio.get_event_loop().create_task(self.process_block_for_asset_contract(contract_address))


if __name__ == "__main__":
    app = FungibleCollectibleTaskManager()
    app.run()
