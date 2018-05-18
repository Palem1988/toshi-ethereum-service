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
                token_registrations = await con.fetch("SELECT * FROM token_registrations")

            for reg in token_registrations:
                async with self.pool.acquire() as con:
                    balances = await con.fetch("SELECT * FROM token_balances where eth_address = $1",
                                               reg['eth_address'])
                tokens1 = {t['contract_address']: t['balance'] for t in balances}
                erc20_dispatcher.update_token_cache("*", reg['eth_address'])
                await asyncio.sleep(10)
                async with self.pool.acquire() as con:
                    balances = await con.fetch("SELECT * FROM token_balances where eth_address = $1",
                                               reg['eth_address'])
                tokens2 = {t['contract_address']: t['balance'] for t in balances}

                # report
                fixed = 0
                for key in tokens1:
                    if key in tokens2:
                        if tokens1[key] != tokens2[key]:
                            fixed += 1
                            log.warning("fixed {}'s {} balance: {} -> {}".format(reg['eth_address'], key, tokens1[key], tokens2[key]))
                        tokens2.pop(key)
                    else:
                        fixed += 1
                        log.warning("fixed {}'s {} balance: {} -> {}".format(reg['eth_address'], key, tokens1[key], "0x0"))
                        for key in tokens2:
                            fixed += 1
                            log.warning("fixed {}'s {} balance: {} -> {}".format(reg['eth_address'], key, "0x0", tokens2[key]))


if __name__ == '__main__':
    from toshieth.app import extra_service_config
    extra_service_config()
    monitor = ERC20SanityCheck()
    monitor.start()
    asyncio.get_event_loop().run_forever()
