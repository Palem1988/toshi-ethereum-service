# -*- coding: utf-8 -*-
import asyncio
import os

from tornado.escape import json_decode
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.faucet import FAUCET_PRIVATE_KEY
from toshi.sofa import parse_sofa_message
from toshi.ethereum.utils import private_key_to_address, data_decoder

from toshi.ethereum.contract import Contract

ERC20_CONTRACT = open(os.path.join(os.path.dirname(__file__), "erc20.sol")).read()

TEST_PRIVATE_KEY = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_PRIVATE_KEY_2 = data_decoder("0x8945608e66736aceb34a83f94689b4e98af497ffc9dc2004a93824096330fa77")
TEST_ADDRESS = private_key_to_address(TEST_PRIVATE_KEY)
TEST_ADDRESS_2 = private_key_to_address(TEST_PRIVATE_KEY_2)

TEST_APN_ID = "64be4fe95ba967bb533f0c240325942b9e1f881b5cd2982568a305dd4933e0bd"

class ReorgTest(EthServiceBaseTest):

    async def deploy_erc20_contract(self, symbol, name, decimals):
        sourcecode = ERC20_CONTRACT.encode('utf-8')
        contract_name = "Token"
        constructor_data = [2**256 - 1, name, decimals, symbol]
        contract = await Contract.from_source_code(sourcecode, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)

        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO tokens (contract_address, symbol, name, decimals) VALUES ($1, $2, $3, $4)",
                              contract.address, symbol, name, decimals)

        return contract

    @gen_test(timeout=60)
    @requires_full_stack(block_monitor=True)
    async def test_reorg(self, *, monitor):

        token_contract = await self.deploy_erc20_contract("TST", "Test Token", 18)

        tx_hashes = []
        to_addresses = ["0x{}".format(os.urandom(20).hex()) for _ in range(5)]
        for to_address in to_addresses:
            resp = await self.fetch_signed("/apn/register", signing_key=TEST_PRIVATE_KEY, method="POST", body={
                "registration_id": os.urandom(64).hex(),
                "address": to_address
            })
            self.assertEqual(resp.code, 204)
            resp = await self.fetch("/tokens/{}".format(to_address))
            self.assertResponseCodeEqual(resp, 200)

            tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, to_address, 10 ** 18,
                                         wait_on_tx_confirmation=True)
            tx_hashes.append(tx_hash)
            tx_hash = await token_contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(to_address, 10 ** 18)
            tx_hashes.append(tx_hash)

        await monitor.block_check()
        last_block = monitor.last_block_number

        print(tx_hashes)
        # fake a reorg
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute("UPDATE blocks SET hash = $1 WHERE blocknumber > $2",
                                  "0x0000000000000000000000000000000000000000000000000000000000000000",
                                  2)
                await con.executemany("DELETE FROM transactions WHERE blocknumber > $2 AND hash = $1",
                                      [(hash, 2) for hash in tx_hashes[-5:]])
                await con.execute("UPDATE transactions SET blocknumber = $1 WHERE blocknumber IS NOT NULL",
                                  0)

        while monitor.last_block_number <= last_block:
            await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            wrong_blocks = await con.fetchval(
                "SELECT count(*) FROM blocks where hash = $1",
                "0x0000000000000000000000000000000000000000000000000000000000000000")
            wrong_txs = await con.fetchval(
                "SELECT count(*) FROM transactions WHERE blocknumber = $1", 0)
            right_txs = await con.fetchval(
                "SELECT count(*) FROM transactions")
            balances = await con.fetch(
                "SELECT * FROM token_balances")

        self.assertEqual(wrong_blocks, 0)
        self.assertEqual(wrong_txs, 0)
        self.assertEqual(right_txs, len(tx_hashes))
        for b in balances:
            print(b)
