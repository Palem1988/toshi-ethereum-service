# -*- coding: utf-8 -*-
import asyncio
import os
import tornado.web

from tornado.escape import json_decode
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.faucet import FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshi.ethereum.utils import private_key_to_address, data_decoder
from toshi.jsonrpc.client import JsonRPCClient

from toshi.ethereum.contract import Contract
from ethereum.utils import sha3

ABC_TOKEN_ADDRESS = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"
YAC_TOKEN_ADDRESS = "0x9ab6c6111577c51da46e2c4c93a3622671578657"

ARTTOKEN_CONTRACT = open(os.path.join(os.path.dirname(__file__), "arttokencreator.sol")).read().encode('utf-8')
TEST_PRIVATE_KEY = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_PRIVATE_KEY_2 = data_decoder("0x8945608e66736aceb34a83f94689b4e98af497ffc9dc2004a93824096330fa77")
TEST_PRIVATE_KEY_3 = data_decoder("0x4327bedc5023c7f575b6c87898c85bd338de01f9fbe9482259c31d610887fd64")
TEST_ADDRESS = private_key_to_address(TEST_PRIVATE_KEY)
TEST_ADDRESS_2 = private_key_to_address(TEST_PRIVATE_KEY_2)
TEST_ADDRESS_3 = private_key_to_address(TEST_PRIVATE_KEY_3)

TEST_APN_ID = "64be4fe95ba967bb533f0c240325942b9e1f881b5cd2982568a305dd4933e0bd"

class FakeIPFSHandler(tornado.web.RequestHandler):

    def get(self, key):
        self.write({"properties": {
            "image": {"description": "http://{}.png".format(key)}
        }})

class FakeJSONRPCHandler(tornado.web.RequestHandler):

    async def post(self):
        await asyncio.sleep(1)
        raise Exception("on purpose error!")

class FungibleERC721Test(EthServiceBaseTest):

    def get_urls(self):
        return super().get_urls() + [
            ('/v1/fake_ipfs/(.+)', FakeIPFSHandler),
            ('/v1/fake_jsonrpc/?', FakeJSONRPCHandler)
        ]

    async def deploy_contract(self, sourcecode, contract_name, constructor_data):
        contract = await Contract.from_source_code(sourcecode, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)
        return contract

    @gen_test(timeout=60)
    @requires_full_stack(parity=True, push_client=True, block_monitor=True, collectible_monitor=True)
    async def test_createarttoken(self, *, parity, push_client, monitor, collectible_monitor):

        creator_contract = await self.deploy_contract(ARTTOKEN_CONTRACT, "ArtTokenCreator", [])
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO collectibles (contract_address, name, type, image_url_format_string) VALUES ($1, $2, $3, $4)",
                              creator_contract.address, "Art Tokens", 2, "https://ipfs.node/{token_uri}")

        await self.faucet(TEST_ADDRESS, 10 ** 18)

        # "mint" some tokens
        txhash = await creator_contract.createAsset.set_sender(TEST_PRIVATE_KEY)("ART1", 10, "dasdasdasdasdasdasdasd", TEST_ADDRESS)
        receipt = await self.eth.eth_getTransactionReceipt(txhash)
        arttokenaddr = "0x" + receipt['logs'][0]['topics'][1][-40:]

        arttoken1 = await Contract.from_source_code(ARTTOKEN_CONTRACT, "ArtToken", address=arttokenaddr, deploy=False)

        txhash = await creator_contract.createAsset.set_sender(TEST_PRIVATE_KEY)("ART2", 10, self.get_url("/fake_ipfs/ART2"), TEST_ADDRESS)
        receipt = await self.eth.eth_getTransactionReceipt(txhash)
        arttokenaddr = "0x" + receipt['logs'][0]['topics'][1][-40:]

        arttoken2 = await Contract.from_source_code(ARTTOKEN_CONTRACT, "ArtToken", address=arttokenaddr, deploy=False)

        self.assertEqual(await arttoken1.name(), "ART1")
        self.assertEqual(await arttoken1.creator(), TEST_ADDRESS)
        self.assertEqual(await arttoken1.balanceOf(TEST_ADDRESS), 10)

        self.assertEqual(await arttoken2.tokenURI(), self.get_url("/fake_ipfs/ART2"))

        # force block check to clear out txs pre registration
        await asyncio.sleep(0.1)
        await monitor.block_check()
        await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            self.assertEqual(await con.fetchval("SELECT count(*) FROM fungible_collectibles"), 2)

        # send an art token!
        await arttoken1.transfer.set_sender(TEST_PRIVATE_KEY)(TEST_ADDRESS_2, 1)

        await asyncio.sleep(0.1)
        await monitor.block_check()
        await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            owner_balance = await con.fetchval(
                "SELECT balance FROM fungible_collectible_balances WHERE owner_address = $1 AND contract_address = $2",
                TEST_ADDRESS, arttoken1.address)
            receiver_balance = await con.fetchval(
                "SELECT balance FROM fungible_collectible_balances WHERE owner_address = $1 AND contract_address = $2",
                TEST_ADDRESS_2, arttoken1.address)
        self.assertEqual(owner_balance, hex(9))
        self.assertEqual(receiver_balance, hex(1))

        resp = await self.fetch("/collectibles/{}".format(TEST_ADDRESS))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['collectibles']), 1)
        self.assertEqual(body['collectibles'][0]['value'], hex(2))

        resp = await self.fetch("/collectibles/{}/{}".format(TEST_ADDRESS, creator_contract.address))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 2)
        self.assertEqual(body['tokens'][0]['image'], "http://ART2.png" if body['tokens'][0]['name'] == "ART2" else "https://ipfs.node/dasdasdasdasdasdasdasd")
        self.assertEqual(body['tokens'][1]['image'], "http://ART2.png" if body['tokens'][1]['name'] == "ART2" else "https://ipfs.node/dasdasdasdasdasdasdasd")

    @gen_test(timeout=300)
    @requires_full_stack(parity=True, push_client=True, block_monitor=True, fungible_monitor='fung')
    async def test_jsonrpc_errors(self, *, parity, push_client, monitor, fung):

        creator_contract = await self.deploy_contract(ARTTOKEN_CONTRACT, "ArtTokenCreator", [])
        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO collectibles (contract_address, name, type, image_url_format_string) VALUES ($1, $2, $3, $4)",
                              creator_contract.address, "Art Tokens", 2, "https://ipfs.node/{token_uri}")

        await self.faucet(TEST_ADDRESS, 10 ** 18)

        # "mint" some tokens
        txhash = await creator_contract.createAsset.set_sender(TEST_PRIVATE_KEY)("ART1", 10, "dasdasdasdasdasdasdasd", TEST_ADDRESS)
        receipt = await self.eth.eth_getTransactionReceipt(txhash)
        arttokenaddr = "0x" + receipt['logs'][0]['topics'][1][-40:]

        arttoken1 = await Contract.from_source_code(ARTTOKEN_CONTRACT, "ArtToken", address=arttokenaddr, deploy=False)

        # force block check to clear out txs pre registration
        await asyncio.sleep(0.1)
        await monitor.block_check()
        await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            self.assertEqual(await con.fetchval("SELECT count(*) FROM fungible_collectibles"), 1)

        # send an art token!
        await arttoken1.transfer.set_sender(TEST_PRIVATE_KEY)(TEST_ADDRESS_2, 1)

        await asyncio.sleep(0.1)
        await monitor.block_check()
        await asyncio.sleep(0.1)

        async with self.pool.acquire() as con:
            owner_balance = await con.fetchval(
                "SELECT balance FROM fungible_collectible_balances WHERE owner_address = $1 AND contract_address = $2",
                TEST_ADDRESS, arttoken1.address)
            receiver_balance = await con.fetchval(
                "SELECT balance FROM fungible_collectible_balances WHERE owner_address = $1 AND contract_address = $2",
                TEST_ADDRESS_2, arttoken1.address)
        self.assertEqual(owner_balance, hex(9))
        self.assertEqual(receiver_balance, hex(1))

        # break the fungible monitor's eth instance
        old_fung_eth = fung.eth
        fung.eth = JsonRPCClient(self.get_url('/fake_jsonrpc'), should_retry=False)

        await arttoken1.transfer.set_sender(TEST_PRIVATE_KEY)(TEST_ADDRESS_3, 1)

        for i in range(10):
            async with self.pool.acquire() as con:
                val = await con.fetchval("SELECT blocknumber FROM last_blocknumber")
                blk = await con.fetchval("SELECT last_block FROM fungible_collectibles")
                count = await con.fetchval("SELECT count(*) FROM fungible_collectible_balances")
            self.assertEqual(count, 2)
            if i > 0:
                self.assertNotEqual(val, blk)
            await asyncio.sleep(1)

        fung.eth = old_fung_eth
        await asyncio.sleep(5)
        async with self.pool.acquire() as con:
            val = await con.fetchval("SELECT blocknumber FROM last_blocknumber")
            blk = await con.fetchval("SELECT last_block FROM fungible_collectibles")
            count = await con.fetchval("SELECT count(*) FROM fungible_collectible_balances")
        self.assertEqual(count, 3)
        if val - blk > 1:
            self.fail("fungible last_block is not caught up with the current block after failures")
