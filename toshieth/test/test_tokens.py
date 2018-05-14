# -*- coding: utf-8 -*-
import blockies
import hashlib

from tornado.escape import json_decode
from tornado.testing import gen_test

from toshieth.app import urls
from toshi.test.database import requires_database
from toshi.test.base import AsyncHandlerTest

# reuse constant from test_avatar.py (toshiid)
TEST_ADDRESS = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"
TEST_ADDRESS_2 = "0x9a5be3baa66e40b8517e13da9b0a9d69e6a29f52"

class TokenHandlerTest(AsyncHandlerTest):

    def get_urls(self):
        return urls

    def get_url(self, path):
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/token/"):
            path = "/v1{}".format(path)
        return super().get_url(path)

    @gen_test
    @requires_database
    async def test_tokens(self):
        image = blockies.create(TEST_ADDRESS, size=8, scale=12)
        hasher = hashlib.md5()
        hasher.update(image)
        hash = hasher.hexdigest()

        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO tokens "
                "(contract_address, symbol, name, decimals, icon, hash, format) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                "0x1111111111111111111111111111111111111111", "ABC", "Awesome Balls Currency Token", 18, image, hash, 'png'
            )
            await con.execute(
                "INSERT INTO tokens "
                "(contract_address, symbol, name, decimals) "
                "VALUES ($1, $2, $3, $4)",
                "0x2222222222222222222222222222222222222222", "YAC", "Yet Another Currency Token", 2
            )

        resp = await self.fetch(
            "/tokens", method="GET"
        )

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 2)

        for token in body['tokens']:
            icon_url = token['icon']
            if token['symbol'] == "YAC":
                self.assertIsNone(token['icon'])
            else:
                self.assertEqual(self.get_url("/token/{}.png".format(token['contract_address'])),
                                 icon_url)
                resp = await self.fetch(
                    icon_url, method="GET"
                )
                self.assertResponseCodeEqual(resp, 200)
                self.assertEqual(resp.headers.get('Content-Type'),
                                 'image/png')
                self.assertEqual(resp.body, image)

        resp = await self.fetch("/token/0x0000000000000000000000000000000000000000.png")
        self.assertResponseCodeEqual(resp, 404)

    @gen_test
    @requires_database
    async def test_token_balances(self):
        image = blockies.create(TEST_ADDRESS, size=8, scale=12)
        hasher = hashlib.md5()
        hasher.update(image)
        hash = hasher.hexdigest()

        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO tokens "
                "(contract_address, symbol, name, decimals, icon, hash, format) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                "0x1111111111111111111111111111111111111111", "ABC", "Awesome Balls Currency Token", 18, image, hash, 'png'
            )
            await con.execute(
                "INSERT INTO tokens "
                "(contract_address, symbol, name, decimals) "
                "VALUES ($1, $2, $3, $4)",
                "0x2222222222222222222222222222222222222222", "YAC", "Yet Another Currency Token", 2
            )
            await con.executemany(
                "INSERT INTO token_balances "
                "(contract_address, eth_address, balance) "
                "VALUES ($1, $2, $3)",
                [("0x1111111111111111111111111111111111111111", TEST_ADDRESS, hex(2 * 10 ** 18)),
                 ("0x2222222222222222222222222222222222222222", TEST_ADDRESS, hex(10 ** 18))])
            await con.executemany(
                "INSERT INTO token_registrations "
                "(eth_address) VALUES ($1)",
                [(TEST_ADDRESS,), (TEST_ADDRESS_2,)])

        resp = await self.fetch(
            "/tokens/{}".format(TEST_ADDRESS), method="GET"
        )

        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(len(body['tokens']), 2)

        for token in body['tokens']:

            # check single token balance endpoint
            resp = await self.fetch("/tokens/{}/{}".format(TEST_ADDRESS, token['contract_address']))
            self.assertResponseCodeEqual(resp, 200)
            single = json_decode(resp.body)

            icon_url = token['icon']
            if token['symbol'] == "YAC":
                self.assertEqual(single['value'], hex(10 ** 18))
                self.assertEqual(token['value'], hex(10 ** 18))
                self.assertIsNone(icon_url)
            else:
                self.assertEqual(single['value'], hex(2 * 10 ** 18))
                self.assertEqual(token['value'], hex(2 * 10 ** 18))
                self.assertEqual(self.get_url("/token/{}.png".format(token['contract_address'])),
                                 icon_url)
                resp = await self.fetch(
                    icon_url, method="GET"
                )
                self.assertResponseCodeEqual(resp, 200)
                self.assertEqual(resp.headers.get('Content-Type'),
                                 'image/png')
                self.assertEqual(resp.body, image)

        # make sure single token balance is 0 for other addresses
        resp = await self.fetch("/tokens/{}/{}".format(TEST_ADDRESS_2, "0x1111111111111111111111111111111111111111"))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(body['value'], "0x0")

        # make sure single token balance for non-existent token 404s
        resp = await self.fetch("/tokens/{}/{}".format(TEST_ADDRESS_2, "0x3333333333333333333333333333333333333333"))
        self.assertResponseCodeEqual(resp, 404)
