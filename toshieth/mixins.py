import tornado.httpclient
import regex
import os

from toshi.log import log
from toshi.utils import parse_int

JSON_RPC_VERSION = "2.0"
HEX_RE = regex.compile("(0x)?([0-9a-fA-F]+)")

def validate_hex(value):
    if isinstance(value, int):
        if value < 0:
            raise ValueError("Negative values are unsupported")
        value = hex(value)[2:]
    if isinstance(value, bytes):
        value = binascii.b2a_hex(value).decode('ascii')
    else:
        m = HEX_RE.match(value)
        if m:
            value = m.group(2)
        else:
            raise ValueError("Unable to convert value to valid hex string")

    return '0x' + value

def validate_block_param(param):
    if param not in ("earliest", "latest", "pending"):
        return validate_hex(param)
    return param

httpCli = tornado.httpclient.AsyncHTTPClient(max_clients=100)

class BalanceMixin:

    async def get_balances(self, eth_address, include_queued=True):
        """Gets the confirmed balance of the eth address from the ethereum network
        and adjusts the value based off any pending transactions.

        Returns 4 values as a tuple:
          - the confirmed (network) balance
          - the balance adjusted for any pending transactions
          - the total value of pending transactions sent from the given address
          - the total value of pending transactions sent to the given address
        """
        async with self.db:
            # get the last block number to use in ethereum calls
            # to avoid race conditions in transactions being confirmed
            # on the network before the block monitor sees and updates them in the database
            block = (await self.db.fetchval("SELECT blocknumber FROM last_blocknumber"))

            pending_sent = await self.db.fetch(
                "SELECT hash, value, gas, gas_price, status FROM transactions "
                "WHERE from_address = $1 "
                "AND ("
                "((status != 'error' AND status != 'confirmed') OR status = 'new') "
                "OR (status = 'confirmed' AND blocknumber > $2))",
                eth_address, block or 0)

            pending_received = await self.db.fetch(
                "SELECT hash, value, status FROM transactions "
                "WHERE to_address = $1 "
                "AND ("
                "((status != 'error' AND status != 'confirmed') OR status = 'new') "
                "OR (status = 'confirmed' AND blocknumber > $2))",
                eth_address, block or 0)

        pending_sent = sum(parse_int(p['value']) + (parse_int(p['gas']) * parse_int(p['gas_price']))
                           for p in pending_sent
                           if include_queued or p['status'] == 'unconfirmed')

        pending_received = sum(parse_int(p['value'])
                               for p in pending_received
                               if include_queued or p['status'] == 'unconfirmed')

        confirmed_balance = 0
        if os.getenv('USE_INFURA', 'false') == 'true':
            confirmed_balance = await self.getEthBalance(eth_address, block=block or "latest")
        else:
            confirmed_balance = await self.eth.eth_getBalance(eth_address, block=block or "latest")

        balance = (confirmed_balance + pending_received) - pending_sent

        return confirmed_balance, balance, pending_sent, pending_received

    async def getEthBalance(self, address, block="latest"):
        data = {
            "jsonrpc": JSON_RPC_VERSION,
            "id": 1,
            "method": 'eth_getBalance',
            "params": [address, validate_block_param(block)]
        }

        try:
            resp = await httpCli.fetch(
                'https://mainnet.infura.io',
                method="POST",
                headers={'Content-Type': "application/json"},
                body=tornado.escape.json_encode(data)
            )
        except Exception as e:
            log.warning("Error in JsonRPCClient._fetch ({}, {}) \"{}\"".format(data['method'], data['params'], str(e)))

        rval = tornado.escape.json_decode(resp.body)
        return parse_int(rval['result'])
