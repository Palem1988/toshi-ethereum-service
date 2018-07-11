import asyncio
from toshi.utils import parse_int
from toshi.ethereum.utils import data_decoder
from toshi.ethereum.tx import create_transaction
from toshi.redis import get_redis_connection

class RedisLockException(Exception):
    pass

class RedisLock:
    def __init__(self, key, raise_when_locked=None, prefix="lock:", ex=30):
        self.key = prefix + key
        self.raise_when_locked = raise_when_locked or RedisLockException
        self.ex = ex
        self.locked = None

    def __enter__(self):
        raise NotImplemented

    async def __aenter__(self):
        redis = get_redis_connection()
        self.locked = locked = await redis.set(
            self.key, 1,
            exist=redis.SET_IF_NOT_EXIST,
            expire=self.ex)
        if not locked:
            raise self.raise_when_locked()

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.locked:
            await get_redis_connection().delete(self.key)

def database_transaction_to_rlp_transaction(transaction):
    """returns an rlp transaction for the given transaction"""

    nonce = transaction['nonce']
    value = parse_int(transaction['value'])
    gas = parse_int(transaction['gas'])
    gas_price = parse_int(transaction['gas_price'])

    tx = create_transaction(nonce=nonce, gasprice=gas_price, startgas=gas,
                            to=transaction['to_address'], value=value,
                            data=data_decoder(transaction['data']),
                            v=parse_int(transaction['v']),
                            r=parse_int(transaction['r']),
                            s=parse_int(transaction['s']))

    return tx

def unwrap_or(future, default):
    """returns the result of the future, or returns the default value if the future is an exception"""
    try:
        return future.result()
    except asyncio.InvalidStateError:
        raise
    except:
        return default

def get_transaction_log_index(_log):
    # parity
    if 'transactionLogIndex' in _log:
        return int(_log['transactionLogIndex'], 16)
    # geth
    elif 'logIndex' in _log:
        return int(_log['logIndex'], 16)
    else:
        return None
