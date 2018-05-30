from toshi.utils import parse_int

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

        confirmed_balance = await self.eth.eth_getBalance(eth_address, block=block or "latest")

        balance = (confirmed_balance + pending_received) - pending_sent

        return confirmed_balance, balance, pending_sent, pending_received
