# -*- coding: utf-8 -*-
from toshi.handlers import BaseHandler
from toshi.errors import JSONHTTPError
from toshi.jsonrpc.errors import JsonRPCInternalError
from toshi.database import DatabaseMixin
from toshi.jsonrpc.errors import JsonRPCError
from toshi.redis import RedisMixin
from toshi.analytics import AnalyticsMixin

from toshi.sofa import SofaPayment
from toshi.handlers import RequestVerificationMixin, SimpleFileHandler
from toshi.utils import validate_address, parse_int
from toshi.log import log, log_headers_on_error

from toshi.config import config
from toshieth.mixins import BalanceMixin
from toshieth.jsonrpc import ToshiEthJsonRPC
from toshieth.utils import database_transaction_to_rlp_transaction
from toshi.ethereum.tx import transaction_to_json, DEFAULT_GASPRICE
from tornado.escape import json_encode
from tornado.web import HTTPError

class TokenIconHandler(DatabaseMixin, SimpleFileHandler):

    async def get(self, address, format):

        async with self.db:
            row = await self.db.fetchrow(
                "SELECT * FROM tokens WHERE contract_address = $1 AND format = lower($2)",
                address, format
            )

        if row is None:
            raise HTTPError(404)

        await self.handle_file_response(
            data=row['icon'],
            content_type="image/png",
            etag=row['hash'],
            last_modified=row['last_modified']
        )


class TokenListHandler(DatabaseMixin, BaseHandler):

    async def get(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        async with self.db:
            count = await self.db.fetchval("SELECT COUNT(*) FROM tokens")
        limit = 100
        results = []
        for offset in range(0, count, limit):
            async with self.db:
                rows = await self.db.fetch("SELECT symbol, name, contract_address, decimals, format FROM tokens OFFSET $1 LIMIT $2", offset, limit)
            for row in rows:
                token = {
                    'symbol': row['symbol'],
                    'name': row['name'],
                    'contract_address': row['contract_address'],
                    'decimals': row['decimals']
                }
                if row['format'] is not None:
                    token['icon'] = "{}://{}/token/{}.{}".format(self.request.protocol, self.request.host,
                                                                 token['contract_address'], row['format'])
                else:
                    token['icon'] = None
                results.append(token)

        self.write({"tokens": results})


class TokenBalanceHandler(DatabaseMixin, BaseHandler):

    async def get(self, eth_address, token_address=None):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        force_update = self.get_argument('force_update', None)

        try:
            result = await ToshiEthJsonRPC(None, self.application, self.request).get_token_balances(
                eth_address, token_address=token_address, force_update=force_update)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        if token_address:
            if result is None:
                raise JSONHTTPError(404)
            self.write(result)
        else:
            self.write({"tokens": result})

class TokenHandler(DatabaseMixin, RequestVerificationMixin, BaseHandler):

    async def get(self, contract_address):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        try:
            result = await ToshiEthJsonRPC(None, self.application, self.request).get_token(contract_address)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        if result is None:
            raise JSONHTTPError(404, body={'errors': [{'id': 'not_found', 'message': 'Not Found'}]})
        self.write(result)

    async def post(self):

        eth_address = self.verify_request()

        try:
            result = await ToshiEthJsonRPC(eth_address, self.application, self.request).add_token(**self.json)
        except JsonRPCInternalError as e:
            raise JSONHTTPError(500, body={'errors': [e.data]})
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})
        except TypeError:
            log.exception("bad_arguments")
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        self.write(result)

    async def delete(self, contract_address):

        eth_address = self.verify_request()

        try:
            await ToshiEthJsonRPC(eth_address, self.application, self.request).remove_token(contract_address=contract_address)
        except JsonRPCInternalError as e:
            raise JSONHTTPError(500, body={'errors': [e.data]})
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})
        except TypeError:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        self.set_status(204)


class CollectiblesHandler(DatabaseMixin, BaseHandler):

    async def get(self, address, contract_address=None):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        try:
            result = await ToshiEthJsonRPC(None, self.application, self.request).get_collectibles(address, contract_address)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        if result is None:
            raise JSONHTTPError(404, body={'errors': [{'id': 'not_found', 'message': 'Not Found'}]})
        self.write(result)

class BalanceHandler(DatabaseMixin, BaseHandler):

    async def get(self, address):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        try:
            result = await ToshiEthJsonRPC(None, self.application, self.request).get_balance(address)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        self.write(result)

class TransactionSkeletonHandler(RedisMixin, BaseHandler):

    async def post(self):

        try:
            # normalize inputs
            if 'from' in self.json:
                self.json['from_address'] = self.json.pop('from')
            if 'to' in self.json:
                self.json['to_address'] = self.json.pop('to')
            elif 'to_address' not in self.json:
                self.json['to_address'] = None
            # the following are to deal with different representations
            # of the same concept from different places
            if 'gasPrice' in self.json:
                self.json['gas_price'] = self.json.pop('gasPrice')
            if 'gasprice' in self.json:
                self.json['gas_price'] = self.json.pop('gasprice')
            if 'startgas' in self.json:
                self.json['gas'] = self.json.pop('startgas')
            result = await ToshiEthJsonRPC(None, self.application, self.request).create_transaction_skeleton(**self.json)
        except JsonRPCError as e:
            log.warning("/tx/skel failed: " + json_encode(e.data) + "\" -> arguments: " + json_encode(self.json) + "\"")
            raise JSONHTTPError(400, body={'errors': [e.data]})
        except TypeError:
            log.warning("/tx/skel failed: bad arguments \"" + json_encode(self.json) + "\"")
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        self.write(result)

class SendTransactionHandler(BalanceMixin, DatabaseMixin, RedisMixin, RequestVerificationMixin, BaseHandler):

    async def post(self):

        if self.is_request_signed():
            sender_toshi_id = self.verify_request()
        else:
            # this is an anonymous transaction
            sender_toshi_id = None

        try:
            result = await ToshiEthJsonRPC(sender_toshi_id, self.application, self.request).send_transaction(**self.json)
        except JsonRPCInternalError as e:
            raise JSONHTTPError(500, body={'errors': [e.data]})
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})
        except TypeError:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        self.write({
            "tx_hash": result
        })

class TransactionHandler(DatabaseMixin, BaseHandler):

    async def get(self, tx_hash):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        format = self.get_query_argument('format', 'rpc').lower()

        try:
            tx = await ToshiEthJsonRPC(None, self.application, self.request).get_transaction(tx_hash)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        if tx is None and format != 'sofa':
            raise JSONHTTPError(404, body={'errors': [{'id': 'not_found', 'message': 'Not Found'}]})

        if format == 'sofa':

            async with self.db:
                row = await self.db.fetchrow(
                    "SELECT * FROM transactions where hash = $1 ORDER BY transaction_id DESC",
                    tx_hash)
            if row is None:
                raise JSONHTTPError(404, body={'errors': [{'id': 'not_found', 'message': 'Not Found'}]})
            if tx is None:
                tx = transaction_to_json(database_transaction_to_rlp_transaction(row))
            if row['status'] == 'error':
                tx['error'] = True
            payment = SofaPayment.from_transaction(tx, networkId=config['ethereum']['network_id'])
            message = payment.render()
            self.set_header('Content-Type', 'text/plain')
            self.write(message.encode('utf-8'))

        else:

            self.write(tx)

class CancelTransactionHandler(DatabaseMixin, BaseHandler):

    async def post(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST')

        if 'tx_hash' not in self.json or 'signature' not in self.json:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        tx_hash = self.json['tx_hash']
        signature = self.json['signature']

        try:
            await ToshiEthJsonRPC(None, self.application, self.request).cancel_queued_transaction(tx_hash, signature)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        self.set_status(204)

class AddressHandler(DatabaseMixin, BaseHandler):

    async def get(self, address):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        offset = parse_int(self.get_argument('offset', '0'))
        limit = parse_int(self.get_argument('limit', '10'))
        status = set([s.lower() for s in self.get_arguments('status')])
        direction = set([d.lower() for d in self.get_arguments('direction')])
        order = self.get_argument('order', 'desc').upper()

        if not validate_address(address) or \
           offset is None or \
           limit is None or \
           (status and not status.issubset(['confirmed', 'unconfirmed', 'queued', 'error'])) or \
           (direction and not direction.issubset(['in', 'out'])) or \
           (order not in ['DESC', 'ASC']):
            raise JSONHTTPError(400, body={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        query = "SELECT * FROM transactions WHERE "
        args = [address, offset, limit]

        if len(direction) == 0 or len(direction) == 2:
            query += "(from_address = $1 OR to_address = $1) "
        elif 'in' in direction:
            query += "to_address = $1 "
        elif 'out' in direction:
            query += "from_address = $1 "

        if len(status) == 0:
            query += "AND (status != $4 OR status IS NULL) "
            args.append('error')
        else:
            status_query = []
            for s in status:
                if s == 'queued':
                    status_query.extend(["status = ${}".format(len(args) + 1), "status IS NULL"])
                else:
                    status_query.append("status = ${}".format(len(args) + 1))
                args.append(s)
            query += "AND (" + " OR ".join(status_query) + ") "

        query += "ORDER BY created {} OFFSET $2 LIMIT $3".format(order)

        async with self.db:
            rows = await self.db.fetch(query, *args)

        transactions = []
        for row in rows:
            transactions.append({
                "hash": row['hash'],
                "to": row['to_address'],
                "from": row['from_address'],
                "nonce": hex(row['nonce']),
                "value": row['value'],
                "gas": row['gas'],
                "gas_price": row['gas_price'],
                "created_data": row['created'].isoformat(),
                "confirmed_data": row['updated'].isoformat() if row['blocknumber'] else None,
                "status": row['status'] if row['status'] is not None else 'queued',
                "data": row['data']
            })
        resp = {
            "transactions": transactions,
            "offset": offset,
            "limit": limit,
            "order": order
        }
        if len(direction) == 1:
            resp['direction'] = direction.pop()
        if status:
            resp['status'] = "&".join(status)
        self.write(resp)

class GasPriceHandler(RedisMixin, BaseHandler):

    async def get(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        gas_station_gas_price = await self.redis.get('gas_station_standard_gas_price')
        if gas_station_gas_price is None:
            gas_station_gas_price = hex(config['ethereum'].getint('default_gasprice', DEFAULT_GASPRICE))
        else:
            gas_station_gas_price = gas_station_gas_price.decode('utf-8')
        self.write({
            "gas_price": gas_station_gas_price
        })


class PNRegistrationHandler(RequestVerificationMixin, DatabaseMixin, BaseHandler):

    @log_headers_on_error
    async def post(self, service):
        toshi_id = self.verify_request()
        payload = self.json

        if not all(arg in payload for arg in ['registration_id']):
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        # eth address verification (default to toshi_id if eth_address is not supplied)
        if 'address' in payload:
            eth_addresses = [payload['address']]
        elif 'addresses' in payload:
            eth_addresses = payload['addresses']
            if not type(eth_addresses) == list:
                raise JSONHTTPError(400, data={'id': 'bad_arguments', 'message': '`addresses` must be a list'})
        else:
            raise JSONHTTPError(400, data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        if not all(validate_address(eth_address) for eth_address in eth_addresses):
            raise JSONHTTPError(400, data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        async with self.db:

            await self.db.executemany(
                "INSERT INTO notification_registrations (toshi_id, service, registration_id, eth_address) "
                "VALUES ($1, $2, $3, $4) ON CONFLICT (toshi_id, service, registration_id, eth_address) DO NOTHING",
                [(toshi_id, service, payload['registration_id'], eth_address) for eth_address in eth_addresses])

            await self.db.commit()

        self.set_status(204)

class PNDeregistrationHandler(RequestVerificationMixin, AnalyticsMixin, DatabaseMixin, BaseHandler):

    async def post(self, service):

        toshi_id = self.verify_request()
        payload = self.json

        if 'registration_id' not in payload:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        # eth address verification (if none is supplied, delete all the matching addresses)
        if 'address' in payload:
            eth_addresses = [payload['address']]
        elif 'addresses' in payload:
            eth_addresses = payload['addresses']
            if not type(eth_addresses) == list:
                raise JSONHTTPError(400, data={'id': 'bad_arguments', 'message': '`addresses` must be a list'})
        else:
            eth_addresses = []

        if not all(validate_address(eth_address) for eth_address in eth_addresses):
            raise JSONHTTPError(400, data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        async with self.db:
            if eth_addresses:
                await self.db.executemany(
                    "DELETE FROM notification_registrations WHERE toshi_id = $1 AND service = $2 AND registration_id = $3 and eth_address = $4",
                    [(toshi_id, service, payload['registration_id'], eth_address) for eth_address in eth_addresses])
            else:
                await self.db.execute(
                    "DELETE FROM notification_registrations WHERE toshi_id = $1 AND service = $2 AND registration_id = $3",
                    toshi_id, service, payload['registration_id'])

            await self.db.commit()

        self.set_status(204)
        self.track(toshi_id, "Deregistered ETH notifications")

class StatusHandler(RedisMixin, BaseHandler):

    async def get(self):
        status = await self.redis.get("monitor_sanity_check_ok")
        if status == b"OK":
            self.write("OK")
        else:
            self.write("MONITOR SANITY CHECK FAILED")

class LegacyRegistrationHandler(RequestVerificationMixin, DatabaseMixin, BaseHandler):
    """backwards compatibility for old pn registration"""

    async def post(self):

        toshi_id = self.verify_request()
        payload = self.json

        if 'addresses' not in payload or len(payload['addresses']) == 0:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        addresses = payload['addresses']

        for address in addresses:
            if not validate_address(address):
                raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        async with self.db:

            # see if this toshi_id is already registered, listening to it's own toshi_id
            rows = await self.db.fetch("SELECT * FROM notification_registrations "
                                       "WHERE toshi_id = $1 AND eth_address = $1 AND service != 'ws'",
                                       toshi_id)
            if rows:
                if len(rows) > 1:
                    log.warning("LEGACY REGISTRATION FOR '{}' HAS MORE THAN ONE DEVICE OR SERVICE".format(toshi_id))
                registration_id = rows[0]['registration_id']
                service = rows[0]['service']
            else:
                service = 'LEGACY'
                registration_id = 'LEGACY'

            # simply store all the entered addresses with no service/registrations id
            for address in addresses:
                await self.db.execute(
                    "INSERT INTO notification_registrations (toshi_id, service, registration_id, eth_address) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (toshi_id, service, registration_id, eth_address) DO NOTHING",
                    toshi_id, service, registration_id, address)

            await self.db.commit()

        self.set_status(204)

class LegacyDeregistrationHandler(RequestVerificationMixin, AnalyticsMixin, DatabaseMixin, BaseHandler):

    async def post(self):

        toshi_id = self.verify_request()
        payload = self.json

        if 'addresses' not in payload or len(payload['addresses']) == 0:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        addresses = payload['addresses']

        for address in addresses:
            if not validate_address(address):
                raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        async with self.db:

            await self.db.execute(
                "DELETE FROM notification_registrations WHERE service != 'ws' AND toshi_id = $1 AND ({})".format(
                    ' OR '.join('eth_address = ${}'.format(i + 2) for i, _ in enumerate(addresses))),
                toshi_id, *addresses)

            await self.db.commit()

        self.set_status(204)
        self.track(toshi_id, "Deregistered ETH notifications")
