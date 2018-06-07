# -*- coding: utf-8 -*-
import asyncio
import os

from tornado.escape import json_decode
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.faucet import FAUCET_PRIVATE_KEY
from toshi.sofa import parse_sofa_message
from toshi.ethereum.utils import private_key_to_address, data_decoder, checksum_encode_address

from toshi.ethereum.contract import Contract

ERC20_CONTRACT = open(os.path.join(os.path.dirname(__file__), "erc20.sol")).read()

TEST_PRIVATE_KEY = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_PRIVATE_KEY_2 = data_decoder("0x8945608e66736aceb34a83f94689b4e98af497ffc9dc2004a93824096330fa77")
TEST_PRIVATE_KEY_3 = data_decoder("0xba0b8c25855f2dab533b101a34f920e17cbb88cebb2f41b329d5b244b6ce35b3")
TEST_ADDRESS = private_key_to_address(TEST_PRIVATE_KEY)
TEST_ADDRESS_2 = private_key_to_address(TEST_PRIVATE_KEY_2)
TEST_ADDRESS_3 = private_key_to_address(TEST_PRIVATE_KEY_3)

TEST_APN_ID = "64be4fe95ba967bb533f0c240325942b9e1f881b5cd2982568a305dd4933e0bd"

class CustomERC20Test(EthServiceBaseTest):

    async def deploy_erc20_contract(self, symbol, name, decimals):
        sourcecode = ERC20_CONTRACT.encode('utf-8')
        contract_name = "Token"
        constructor_data = [2**256 - 1, name, decimals, symbol]
        contract = await Contract.from_source_code(sourcecode, contract_name,
                                                   constructor_data=constructor_data,
                                                   deployer_private_key=FAUCET_PRIVATE_KEY)
        return contract

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor=True)
    async def test_get_unknown_erc20_token(self, *, monitor):

        name = "TesT"
        symbol = "TST"
        decimals = 18

        contract = await self.deploy_erc20_contract(symbol, name, decimals)

        await monitor.block_check()

        resp = await self.fetch("/token/{}".format(contract.address))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(body['name'], name)
        self.assertEqual(body['symbol'], symbol)
        self.assertEqual(body['decimals'], decimals)

        resp = await self.fetch("/token/{}".format("0x0000000000000000000000000000000000000000"))
        self.assertResponseCodeEqual(resp, 404)

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor=True)
    async def test_register_custom_erc20_token(self, *, monitor):

        value = 10 * 10 ** 18

        await self.faucet(TEST_ADDRESS, value)
        await self.faucet(TEST_ADDRESS_2, value)

        normal_contract = await self.deploy_erc20_contract("TOK", "TokEN", 18)
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO tokens (contract_address, symbol, name, decimals) VALUES ($1, $2, $3, $4)",
                              normal_contract.address, "TOK", "TokEN", 18)
        await normal_contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS, value)

        name = "TesT"
        symbol = "TST"
        decimals = 18

        custom_contract = await self.deploy_erc20_contract(symbol, name, decimals)
        await custom_contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS, value)

        # initialise token registrations
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)

        await monitor.block_check()

        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY, body={
            "contract_address": custom_contract.address})
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(body['name'], name)
        self.assertEqual(body['symbol'], symbol)
        self.assertEqual(body['decimals'], decimals)
        self.assertEqual(body['balance'], hex(value))

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 2)

        # test registering a token for which the user has no balance
        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY_2, body={
            "contract_address": custom_contract.address})
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(body['name'], name)
        self.assertEqual(body['symbol'], symbol)
        self.assertEqual(body['decimals'], decimals)
        self.assertEqual(body['balance'], hex(0))

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        print(body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['balance'], hex(0))

        # test that emptying the balance of a custom token doesn't hide the token
        tx = await self.send_tx(TEST_PRIVATE_KEY, TEST_ADDRESS_2, value, token_address=custom_contract.address)
        await self.wait_on_tx_confirmation(tx)

        await monitor.block_check()

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 2)
        self.assertEqual(body['tokens'][0]['balance'], hex(value))
        self.assertEqual(body['tokens'][1]['balance'], hex(0))

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['balance'], hex(value))

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor=True)
    async def test_hiding_erc20_tokens_from_balance(self, *, monitor):

        value = 10 * 10 ** 18

        normal_contract = await self.deploy_erc20_contract("TOK", "TokEN", 18)
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO tokens (contract_address, symbol, name, decimals) VALUES ($1, $2, $3, $4)",
                              normal_contract.address, "TOK", "TokEN", 18)
        await normal_contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS, value)
        await normal_contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS_2, value)

        # initialise token registrations
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)

        await monitor.block_check()
        # small delay to make sure all block processing related things have finished
        await asyncio.sleep(0.1)

        resp = await self.fetch_signed("/token/{}".format(normal_contract.address), method="DELETE", signing_key=TEST_PRIVATE_KEY)
        self.assertResponseCodeEqual(resp, 204)

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 0)

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor=True)
    async def test_showing_erc20_tokens_with_0_balance(self, *, monitor):

        normal_contract = await self.deploy_erc20_contract("TOK", "TokEN", 18)
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO tokens (contract_address, symbol, name, decimals) VALUES ($1, $2, $3, $4)",
                              normal_contract.address, "TOK", "TokEN", 18)

        # initialise token registrations
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)

        await monitor.block_check()

        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY, body={
            "contract_address": normal_contract.address})
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['balance'], hex(0))

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_fail_register_custom_erc20_token(self):

        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY, body={
            "contract_address": "0x0123456789012345678901234567890123456789"})
        self.assertResponseCodeEqual(resp, 400)
        body = json_decode(resp.body)
        self.assertEqual(body['errors'][0]['message'], "Invalid ERC20 Token")

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor=True)
    async def test_new_custom_erc20_token_is_hidden_from_others(self, *, monitor):

        value = 10 * 10 ** 18
        name = "TokEN"
        symbol = "TOK"
        decimals = 18
        contract = await self.deploy_erc20_contract(symbol, name, decimals)
        await contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS, value)
        await contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS_2, value)
        await contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS_3, value)

        # initialise token registrations
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)

        await monitor.block_check()

        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY, body={
            "contract_address": contract.address})
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(body['name'], name)
        self.assertEqual(body['symbol'], symbol)
        self.assertEqual(body['decimals'], decimals)
        self.assertEqual(body['balance'], hex(value))

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)

        # wait a bit for things to happen
        await asyncio.sleep(2)

        # make sure other the main address still has the custom token
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        # make sure other addresses don't have any tokens still
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_2))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 0)

        # check that new addresses also don't get custom tokens by default (even if they hold value)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_3))
        self.assertResponseCodeEqual(resp, 200)

        # wait a bit for things to happen
        await asyncio.sleep(1)

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_3))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 0)

        # make sure they can add the token now
        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY_3, body={
            "contract_address": contract.address})
        self.assertResponseCodeEqual(resp, 200)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_3))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['balance'], hex(value))

        # make sure the address also receives updates
        await contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS_3, value)
        await monitor.block_check()
        await asyncio.sleep(0.1)
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS_3))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['balance'], hex(value * 2))

    @gen_test(timeout=30)
    @requires_full_stack(block_monitor=True)
    async def test_erc20_contract_address_casing_matches(self, *, monitor):

        normal_contract = await self.deploy_erc20_contract("TOK", "TokEN", 18)
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO tokens (contract_address, symbol, name, decimals) VALUES ($1, $2, $3, $4)",
                              normal_contract.address, "TOK", "TokEN", 18)

        # initialise token registrations
        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        await monitor.block_check()

        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY, body={
            "contract_address": normal_contract.address})
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch_signed("/token", method="POST", signing_key=TEST_PRIVATE_KEY, body={
            "contract_address": checksum_encode_address(normal_contract.address)})
        self.assertResponseCodeEqual(resp, 200)

        resp = await self.fetch("/tokens/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['balance'], hex(0))
