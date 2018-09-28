import asyncio
import logging
from toshi.log import configure_logger, log_unhandled_exceptions
from toshi.database import prepare_database
from toshi.redis import prepare_redis
from toshieth.tasks import erc20_dispatcher

log = logging.getLogger("toshieth.erc20sanitycheck")

class ERC20SanityCheck:

    def __init__(self):
        configure_logger(log)

    def start(self):
        asyncio.get_event_loop().create_task(self._initialise())

    @log_unhandled_exceptions(logger=log)
    async def _initialise(self):
        # prepare databases
        self.pool = await prepare_database(handle_migration=False)
        await prepare_redis()

        asyncio.get_event_loop().create_task(self.erc20_balance_sanity_check())

    @log_unhandled_exceptions(logger=log)
    async def erc20_balance_sanity_check(self):
        while True:
            log.info("starting erc20 balance sanity check process")

            async with self.pool.acquire() as con:
                bad_from = await con.fetch(
                    "SELECT b.eth_address, b.contract_address FROM token_balances b "
                    "JOIN token_transactions t ON b.contract_address = t.contract_address AND b.eth_address = t.from_address "
                    "JOIN transactions x ON x.transaction_id = t.transaction_id "
                    "WHERE b.blocknumber > 0 and x.blocknumber > b.blocknumber "
                    "GROUP BY b.eth_address, b.contract_address")
                bad_to = await con.fetch(
                    "SELECT b.eth_address, b.contract_address FROM token_balances b "
                    "JOIN token_transactions t ON b.contract_address = t.contract_address AND b.eth_address = t.to_address "
                    "JOIN transactions x ON x.transaction_id = t.transaction_id "
                    "WHERE b.blocknumber > 0 and x.blocknumber > b.blocknumber "
                    "GROUP BY b.eth_address, b.contract_address")

            bad_balances = {}
            for b in bad_from:
                if b['contract_address'] not in bad_balances:
                    bad_balances[b['contract_address']] = set()
                bad_balances[b['contract_address']].add(b['eth_address'])
            for b in bad_to:
                if b['contract_address'] not in bad_balances:
                    bad_balances[b['contract_address']] = set()
                bad_balances[b['contract_address']].add(b['eth_address'])

            for contract_address, addresses in bad_balances.items():
                log.info("Found bad balances for token {} for addresses:\n    - {}".format(contract_address, "\n    - ".join(list(addresses))))
                erc20_dispatcher.update_token_cache(contract_address, *addresses)

            # run every 5 minutes
            await asyncio.sleep(300)


if __name__ == '__main__':
    from toshieth.app import extra_service_config
    extra_service_config()
    monitor = ERC20SanityCheck()
    monitor.start()
    asyncio.get_event_loop().run_forever()
