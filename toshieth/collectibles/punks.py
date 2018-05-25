import asyncio
import logging
from toshi.log import configure_logger
from toshi.config import config
from toshi.ethereum.utils import data_decoder
from ethereum.abi import decode_abi, process_type, decode_single
from toshi.utils import parse_int
from toshieth.collectibles.base import CollectiblesTaskManager

log = logging.getLogger("toshieth.cryptopunks")

CRYPTO_PUNKS_CONTRACT_ADDRESS = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
PUNK_TRANSFER_TOPIC = "0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8"
PUNK_BOUGHT_TOPIC = "0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

class CryptoPunksTaskManager(CollectiblesTaskManager):

    def __init__(self):
        super().__init__()
        configure_logger(log)
        self._processing = False
        self.__call = 0

    async def process_block(self, blocknumber=None):
        if self._processing and not self._processing.done():
            return
        self._processing = asyncio.Task.current_task()
        self.__call += 1

        async with self.pool.acquire() as con:
            latest_block_number = await con.fetchval(
                "SELECT blocknumber FROM last_blocknumber")
            if latest_block_number is None:
                log.warning("no blocks processed by block monitor yet")
                self._processing = False
                return

            collectible = await con.fetchrow("SELECT * FROM collectibles WHERE contract_address = $1",
                                             CRYPTO_PUNKS_CONTRACT_ADDRESS)
        if collectible is None:
            return

        from_block_number = collectible['last_block'] + 1

        if latest_block_number < from_block_number:
            self._processing = False
            return

        to_block_number = min(from_block_number + 1000, latest_block_number)

        topics = [[TRANSFER_TOPIC, PUNK_BOUGHT_TOPIC, PUNK_TRANSFER_TOPIC]]

        while True:
            try:
                logs = await self.eth.eth_getLogs(
                    fromBlock=from_block_number, toBlock=to_block_number,
                    topics=topics,
                    address=CRYPTO_PUNKS_CONTRACT_ADDRESS)
                break
            except:
                continue

        if len(logs):

            transactions = {}
            updates = []

            for i, _log in enumerate(logs):
                tx = transactions.setdefault(_log['transactionHash'], {'function': 'unknown', 'done': False})
                log_block_number = int(_log['blockNumber'], 16)
                assert log_block_number >= from_block_number and log_block_number <= to_block_number
                if tx['done'] is True:
                    log.error("tried to reprocess transaction that was already added")
                    continue

                topic = _log['topics'][0]

                if topic == TRANSFER_TOPIC:
                    tx['to_address'] = decode_single(process_type('address'),
                                                     data_decoder(_log['topics'][2]))
                elif topic == PUNK_TRANSFER_TOPIC:
                    tx['token_id'] = decode_abi(['uint256'], data_decoder(_log['data']))[0]
                    tx['function'] = 'transferPunk'
                elif topic == PUNK_BOUGHT_TOPIC:
                    tx['token_id'] = parse_int(decode_single(process_type('address'),
                                                             data_decoder(_log['topics'][1])))
                    to_address = decode_single(process_type('address'),
                                               data_decoder(_log['topics'][3]))
                    if to_address == "0x0000000000000000000000000000000000000000":
                        tx['function'] = 'acceptBidForPunk'
                    else:
                        tx['function'] = 'buyPunk'
                else:
                    log.warning("got unknown topic: {}".format(topic))
                    continue

                if 'to_address' in tx and 'token_id' in tx:

                    tx['done'] = True
                    log.info("CryptoPunk #{} -> {} -> {}".format(
                        tx['token_id'], tx['function'], tx['to_address']))
                    token_image = config['collectibles']['image_format'].format(
                        contract_address=CRYPTO_PUNKS_CONTRACT_ADDRESS,
                        token_id_hex=hex(tx['token_id']),
                        token_id_int=tx['token_id'])
                    updates.append((CRYPTO_PUNKS_CONTRACT_ADDRESS, hex(tx['token_id']), tx['to_address'], token_image))

            async with self.pool.acquire() as con:
                await con.executemany(
                    "INSERT INTO collectible_tokens (contract_address, token_id, owner_address, image) "
                    "VALUES ($1, $2, $3, $4) "
                    "ON CONFLICT (contract_address, token_id) DO UPDATE "
                    "SET owner_address = EXCLUDED.owner_address",
                    updates)

        ready = collectible['ready'] or to_block_number == latest_block_number

        async with self.pool.acquire() as con:
            await con.execute("UPDATE collectibles SET last_block = $1, ready = $2 WHERE contract_address = $3",
                              to_block_number, ready, CRYPTO_PUNKS_CONTRACT_ADDRESS)

        self._processing = False
        if to_block_number < latest_block_number:
            asyncio.get_event_loop().create_task(self.process_block())

if __name__ == "__main__":
    app = CryptoPunksTaskManager()
    app.run()
