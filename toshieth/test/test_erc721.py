# -*- coding: utf-8 -*-
import asyncio
import os
import tornado.web

from tornado.escape import json_decode
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.faucet import FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshi.ethereum.utils import private_key_to_address, data_decoder

from toshi.ethereum.contract import Contract
from ethereum.utils import sha3

ABC_TOKEN_ADDRESS = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"
YAC_TOKEN_ADDRESS = "0x9ab6c6111577c51da46e2c4c93a3622671578657"

ERC721_CONTRACT = open(os.path.join(os.path.dirname(__file__), "erc721.sol")).read().encode('utf-8')
TEST_PRIVATE_KEY = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_PRIVATE_KEY_2 = data_decoder("0x8945608e66736aceb34a83f94689b4e98af497ffc9dc2004a93824096330fa77")
TEST_ADDRESS = private_key_to_address(TEST_PRIVATE_KEY)
TEST_ADDRESS_2 = private_key_to_address(TEST_PRIVATE_KEY_2)

NOT_QUITE_ERC721_CONTRACT = open(os.path.join(os.path.dirname(__file__), "not_quite_erc721.sol")).read().encode('utf-8')
NOT_QUITE_ERC721_MINTER_CONTRACT = open(os.path.join(os.path.dirname(__file__), "not_quite_erc721_minter.sol")).read().encode('utf-8')

TEST_APN_ID = "64be4fe95ba967bb533f0c240325942b9e1f881b5cd2982568a305dd4933e0bd"

class FakeIPFSHandler(tornado.web.RequestHandler):

    def get(self, key):
        self.write({"name": key,
                    "description": "description of {}".format(key),
                    # image with "description" to test correct ERC721 metadata format style
                    "image": {"description": "http://{}.png".format(key)}})

class ERC721Test(EthServiceBaseTest):

    def get_urls(self):
        return super().get_urls() + [
            ('/v1/fake_ipfs/(.+)', FakeIPFSHandler)
        ]

    async def deploy_contract(self, sourcecode, contract_name, constructor_data):
        contract = await Contract.from_source_code(sourcecode, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)
        return contract

    @gen_test(timeout=60)
    @requires_full_stack(parity=True, push_client=True, block_monitor=True, collectible_monitor=True)
    async def test_erc721_transfer(self, *, parity, push_client, monitor, collectible_monitor):
        constructor_args = [
            ["TST", "Test ERC721 Token"]
        ]
        collectibles = {}
        contracts = {}

        for args in constructor_args:
            print("deploying {}".format(args[1]))
            contract = await self.deploy_contract(ERC721_CONTRACT, "NonFungibleToken", args)
            contracts[contract.address] = contract
            collectibles[contract.address] = {"symbol": args[0], "name": args[1], "contract": contract, 'tokens': {}}
            args.append(contract.address)
            async with self.pool.acquire() as con:
                await con.execute("INSERT INTO collectibles (contract_address, name) VALUES ($1, $2)",
                                  contract.address, args[1])

        # "mint" some tokens
        for collectible in collectibles.values():
            contract = collectible['contract']
            for i in range(10):
                token_id = int(os.urandom(16).hex(), 16)
                uri = self.get_url("/fake_ipfs/{}".format(hex(token_id)))
                await contract.mint.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS, token_id, uri)
                collectible['tokens'][hex(token_id)] = TEST_ADDRESS

                test_uri = await contract.tokenURI(token_id)
                self.assertEqual(test_uri, uri)

            result = await contract.balanceOf(TEST_ADDRESS)
            self.assertEqual(result, len(collectible['tokens']))

        # force block check to clear out txs pre registration
        await monitor.block_check()
        await asyncio.sleep(0.1)

        collectible = next(iter(collectibles.values()))
        contract = collectible['contract']
        users_tokens = [token_id for token_id, owner in collectible['tokens'].items() if owner == TEST_ADDRESS]

        resp = await self.fetch("/collectibles/{}/{}".format(TEST_ADDRESS, contract.address))

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), len(users_tokens))

        for token in body['tokens']:
            self.assertIn(token['token_id'], users_tokens)

        # give TEST_ADDRESS some funds
        await self.wait_on_tx_confirmation(await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS, 10 ** 18))

        token_id = next(iter(collectible['tokens'].keys()))
        await contract.transfer.set_sender(TEST_PRIVATE_KEY)(TEST_ADDRESS_2, token_id)

        # force block check to clear out txs
        await monitor.block_check()
        await asyncio.sleep(0.1)

        resp = await self.fetch("/collectibles/{}/{}".format(TEST_ADDRESS_2, contract.address))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
        self.assertEqual(body['tokens'][0]['token_id'], token_id)
        self.assertEqual(body['tokens'][0]['name'], token_id)
        self.assertEqual(body['tokens'][0]['description'], "description of {}".format(token_id))
        self.assertEqual(body['tokens'][0]['image'], "http://{}.png".format(token_id))

        # make sure the token has been removed from the original owner
        resp = await self.fetch("/collectibles/{}/{}".format(TEST_ADDRESS, contract.address))

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), len(users_tokens) - 1)

    @gen_test(timeout=60)
    @requires_full_stack(parity=True, push_client=True, block_monitor=True, collectible_monitor=True)
    async def test_not_quite_erc721_transfer(self, *, parity, push_client, monitor, collectible_monitor):
        args = ["NQT", "Test Not Quite ERC721 Token"]

        print("deploying {}".format(args[1]))
        contract = await self.deploy_contract(NOT_QUITE_ERC721_CONTRACT, "NonFungibleToken", args)
        minter = await self.deploy_contract(NOT_QUITE_ERC721_MINTER_CONTRACT, "NonFungibleTokenMinter",
                                            [contract.address])

        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO collectible_transfer_events "
                              "(collectible_address, contract_address, name, topic_hash, arguments, indexed_arguments, to_address_offset, token_id_offset) "
                              "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                              contract.address, contract.address, "Transfer1", "0x" + sha3("Transfer1(address,address,uint256)").hex(), ["address", "address", "uint256"], [True, False, False], 1, 2)
            await con.execute("INSERT INTO collectible_transfer_events "
                              "(collectible_address, contract_address, name, topic_hash, arguments, indexed_arguments, to_address_offset, token_id_offset) "
                              "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                              contract.address, minter.address, "TokenMinted", "0x" + sha3("TokenMinted(address,uint256)").hex(), ["address", "uint256"], [False, False], 0, 1)

            await con.execute("INSERT INTO collectibles (contract_address, name, type) VALUES ($1, $2, $3)",
                              contract.address, args[1], 1)

        # "mint" some tokens
        tokens = []
        for i in range(10):
            token_id = int(os.urandom(16).hex(), 16)
            await minter.mint.set_sender(FAUCET_PRIVATE_KEY)(token_id, "")
            tokens.append(token_id)

        result = await contract.balanceOf(FAUCET_ADDRESS)
        self.assertEqual(result, len(tokens))

        await monitor.block_check()
        # note: giving 0.5 otherwise this test randomly fails
        await asyncio.sleep(0.5)

        resp = await self.fetch("/collectibles/{}/{}".format(FAUCET_ADDRESS, contract.address))

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), len(tokens))

        await contract.transfer.set_sender(FAUCET_PRIVATE_KEY)(TEST_ADDRESS, tokens[0])

        await monitor.block_check()
        await asyncio.sleep(0.1)

        resp = await self.fetch("/collectibles/{}/{}".format(FAUCET_ADDRESS, contract.address))

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), len(tokens) - 1)

        resp = await self.fetch("/collectibles/{}/{}".format(TEST_ADDRESS, contract.address))

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 1)
