import asyncio
import logging
from toshi.log import configure_logger
from toshi.config import config
from toshi.ethereum.utils import data_decoder
from ethereum.utils import sha3
from ethereum.abi import decode_abi, process_type, decode_single
from toshi.utils import parse_int
from toshieth.collectibles.base import CollectiblesTaskManager
from urllib.parse import urlparse
from tornado.httpclient import AsyncHTTPClient
from tornado.escape import json_decode

log = logging.getLogger("toshieth.erc721")

TOKEN_URI_CALL_DATA = "0x" + sha3("tokenURI(uint256)")[:4].hex()

class ERC721TaskManager(CollectiblesTaskManager):

    def __init__(self):
        super().__init__()
        configure_logger(log)
        self._processing = {}

    async def process_block(self, blocknumber=None):
        async with self.pool.acquire() as con:
            latest_block_number = await con.fetchval(
                "SELECT blocknumber FROM last_blocknumber")
        if latest_block_number is None:
            log.warning("no blocks processed by block monitor yet")
            return

        async with self.pool.acquire() as con:
            contract_addresses = await con.fetch(
                "SELECT contract_address FROM collectibles WHERE type = 1 OR type = 721")

        for row in contract_addresses:
            asyncio.get_event_loop().create_task(self.process_block_for_contract(row['contract_address']))

    async def process_block_for_contract(self, collectible_address):
        if collectible_address in self._processing and not self._processing[collectible_address].done():
            log.debug("Already processing {}".format(collectible_address))
            return

        self._processing[collectible_address] = asyncio.Task.current_task()

        async with self.pool.acquire() as con:
            latest_block_number = await con.fetchval(
                "SELECT blocknumber FROM last_blocknumber")
            collectible = await con.fetchrow("SELECT * FROM collectibles WHERE contract_address = $1",
                                             collectible_address)
            if collectible is None:
                log.error("Unable to find collectible with contract_address {}".format(collectible_address))
                del self._processing[collectible_address]
                return

            if collectible['type'] == 1:
                events = await con.fetch("SELECT * FROM collectible_transfer_events "
                                         "WHERE collectible_address = $1",
                                         collectible_address)
            elif collectible['type'] == 721:
                # use default erc721 event
                # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-721.md
                events = [{
                    'collectible_address': collectible_address,
                    'contract_address': collectible_address,
                    'name': 'Transfer',
                    'topic_hash': '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                    'arguments': ['address', 'address', 'uint256'],
                    'indexed_arguments': [True, True, False],
                    'to_address_offset': 1,
                    'token_id_offset': 2
                }]

        from_block_number = collectible['last_block'] + 1

        if latest_block_number < from_block_number:
            del self._processing[collectible_address]
            log.info("Aborting {} because latest block number < collectible's next block".format(collectible_address))
            return

        to_block_number = min(from_block_number + 1000, latest_block_number)

        updates = {}

        for event in events:
            contract_address = event['contract_address']

            while True:
                try:
                    logs = await self.eth.eth_getLogs(
                        fromBlock=from_block_number, toBlock=to_block_number,
                        topics=[[event['topic_hash']]],
                        address=contract_address)
                    break
                except Exception as e:
                    if hasattr(e, 'message') and e.message != "Unknown block number":
                        log.exception("unexpected error getting logs for collectible at address: {}".format(contract_address))
                    continue

            if len(logs):

                for _log in logs:
                    indexed_data = _log['topics'][1:]
                    data_types = [t for t, i in zip(event['arguments'], event['indexed_arguments']) if i is False]
                    try:
                        data = decode_abi(data_types, data_decoder(_log['data']))
                    except:
                        log.exception("Error decoding log data: {} {}".format(data_types, _log['data']))
                        del self._processing[collectible_address]
                        return
                    arguments = []
                    try:
                        for t, i in zip(event['arguments'], event['indexed_arguments']):
                            if i is True:
                                arguments.append(decode_single(process_type(t), data_decoder(indexed_data.pop(0))))
                            else:
                                arguments.append(data.pop(0))
                    except:
                        log.exception("Error compiling event data")
                        log.info("EVENT: {}".format(event))
                        log.info("LOG: {}".format(_log))
                        del self._processing[collectible_address]
                        return

                    to_address = arguments[event['to_address_offset']]
                    token_id = parse_int(arguments[event['token_id_offset']])

                    log.debug("{} #{} -> {} -> {}".format(collectible['name'], token_id,
                                                          event['name'], to_address))
                    updates[hex(token_id)] = (collectible_address, hex(token_id), to_address)

        if len(updates) > 0:
            new_tokens = []
            for token_id in list(updates.keys()):
                async with self.pool.acquire() as con:
                    token = await con.fetchrow("SELECT * FROM collectible_tokens WHERE contract_address = $1 AND token_id = $2",
                                               collectible_address, token_id)
                if token is None:
                    # get token details
                    token_uri = None
                    token_uri_data = await self.eth.eth_call(to_address=collectible_address, data="{}{:064x}".format(
                        TOKEN_URI_CALL_DATA, int(token_id, 16)))
                    if token_uri_data and token_uri_data != "0x":
                        try:
                            token_uri = decode_abi(['string'], data_decoder(token_uri_data))[0].decode('utf-8', errors='replace')
                        except:
                            log.exception("Error decoding tokenURI data")

                    token_image = None
                    token_name = None
                    token_description = None
                    # if token_uri points to a valid url check if it points to json (for the erc721 metadata)
                    parsed_uri = urlparse(token_uri)
                    if token_uri and parsed_uri.netloc and parsed_uri.scheme in ['http', 'https']:
                        try:
                            resp = await AsyncHTTPClient(max_clients=100).fetch(parsed_uri.geturl())
                            metadata = json_decode(resp.body)
                            if "properties" in metadata:
                                metadata = metadata['properties']
                            if 'name' in metadata:
                                if type(metadata['name']) == dict and 'description' in metadata['name']:
                                    token_name = metadata['name']['description']
                                elif type(metadata['name']) == str:
                                    token_name = metadata['name']
                            if 'description' in metadata:
                                if type(metadata['description']) == dict and 'description' in metadata['description']:
                                    token_description = metadata['description']['description']
                                elif type(metadata['description']) == str:
                                    token_description = metadata['description']
                            if 'image' in metadata:
                                if type(metadata['image']) == dict and 'description' in metadata['image']:
                                    token_image = metadata['image']['description']
                                elif type(metadata['image']) == str:
                                    token_image = metadata['image']
                        except:
                            log.exception("Error getting token metadata for {}:{} from {}".format(collectible_address, token_id, token_uri))
                            pass

                    if not token_image:
                        if collectible['image_url_format_string'] is not None:
                            image_format_string = collectible['image_url_format_string']
                        else:
                            image_format_string = config['collectibles']['image_format']
                        token_image = image_format_string.format(
                            contract_address=collectible_address,
                            token_id_hex=token_id,
                            token_id_int=int(token_id, 16),
                            token_uri=token_uri)

                    new_token = updates.pop(token_id, ()) + (token_uri, token_name, token_description, token_image)
                    new_tokens.append(new_token)

            async with self.pool.acquire() as con:
                if len(new_tokens) > 0:
                    await con.executemany(
                        "INSERT INTO collectible_tokens (contract_address, token_id, owner_address, token_uri, name, description, image) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        new_tokens)

                await con.executemany(
                    "INSERT INTO collectible_tokens (contract_address, token_id, owner_address) "
                    "VALUES ($1, $2, $3) "
                    "ON CONFLICT (contract_address, token_id) DO UPDATE "
                    "SET owner_address = EXCLUDED.owner_address",
                    list(updates.values()))

        ready = collectible['ready'] or to_block_number == latest_block_number

        self.last_block = to_block_number
        async with self.pool.acquire() as con:
            await con.execute("UPDATE collectibles SET last_block = $1, ready = $2 WHERE contract_address = $3",
                              to_block_number, ready, collectible_address)

        del self._processing[collectible_address]
        if to_block_number < latest_block_number:
            asyncio.get_event_loop().create_task(self.process_block_for_contract(collectible_address))

if __name__ == "__main__":
    app = ERC721TaskManager()
    app.run()
