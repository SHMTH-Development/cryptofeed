'''
Copyright (C) 2024 Huyen Ha - hathehuyen@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
from datetime import datetime as dt
from typing import Tuple

import acsylla
import time
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue
from cryptofeed.defines import CANDLES, FUNDING, OPEN_INTEREST, TICKER, TRADES, LIQUIDATIONS, INDEX


class ScyllaCallback(BackendQueue):
    def __init__(self, cluster_addresses=['172.17.0.2', '172.17.0.3', '172.17.0.4'], key_space='my_keyspace', user=None, pw=None, db=None, port=None, table=None, custom_columns: dict = None, none_to=None, numeric_type=float, **kwargs):
        """
        cluster_addresses: Array
            Database cluster addresses
        user: str
            The name of the database role used for authentication.
        db: str
            The name of the database to connect to.
        pw: str
            Password to be used for authentication, if the server requires one.
        table: str
            Table name to insert into. Defaults to default_table that should be specified in child class
        custom_columns: dict
            A dictionary which maps Cryptofeed's data type fields to Scylla's table column names, e.g. {'symbol': 'instrument', 'price': 'price', 'amount': 'size'}
            Can be a subset of Cryptofeed's available fields (see the cdefs listed under each data type in types.pyx). Can be listed any order.
            Note: to store BOOK data in a JSONB column, include a 'data' field, e.g. {'symbol': 'symbol', 'data': 'json_data'}
        """
        self.session = None
        self.table = table if table else self.default_table
        self.custom_columns = custom_columns
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.cluster_addresses = cluster_addresses
        self.key_space = key_space
        self.insert_statement = f"INSERT INTO {self.key_space}.{self.table} ({','.join([v for v in self.custom_columns.values()])}) VALUES " if custom_columns else None
        self.CQL = f"""
            INSERT INTO {self.key_space}.{self.table}
            (exchange, symbol, timestamp, receipt, data)
            VALUES (?, ?, ?, ?, ?)
            """
        self.window = 1
        self.last_update = 0
        self.running = True

    async def _connect(self):
        if self.session is None:
            self.cluster = acsylla.create_cluster(self.cluster_addresses)
            self.session = await self.cluster.create_session(keyspace=self.key_space)

    def format(self, data: Tuple):
        feed = data[0]
        symbol = data[1]
        timestamp = data[2]
        receipt_timestamp = data[3]
        data = data[4]

        return f"(DEFAULT,'{timestamp}','{receipt_timestamp}','{feed}','{symbol}','{json.dumps(data)}')"

    def _custom_format(self, data: Tuple):

        d = {
            **data[4],
            **{
                'exchange': data[0],
                'symbol': data[1],
                'timestamp': data[2],
                'receipt': data[3],
            }
        }

        # Cross-ref data dict with user column names from custom_columns dict, inserting NULL if requested data point not present
        sequence_gen = (d[field] if d[field] else 'NULL' for field in self.custom_columns.keys())
        # Iterate through the generator and surround everything except floats and NULL in single quotes
        sql_string = ','.join(str(s) if isinstance(s, float) or s == 'NULL' else "'" + str(s) + "'" for s in sequence_gen)
        return f"({sql_string})"

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                if len(updates) > 0:
                    batch = []
                    for data in updates:
                        ts = dt.utcfromtimestamp(data['timestamp']) if data['timestamp'] else None
                        rts = dt.utcfromtimestamp(data['receipt_timestamp'])
                        batch.append((data['exchange'], data['symbol'], ts, rts, data))
                    await self.write_batch(batch)

    async def write_batch(self, updates: list):
        try:
            await self._connect()
            prepaired = await self.session.create_prepared(self.CQL)
            statement = prepaired.bind()
            print('Write batch scylla:', self.CQL)
            for update in updates:
                statement.bind_list(self.format(update))
                await self.session.execute(statement)
                print('Write update scylla:', self.format(update))
        except Exception as e:
            print(e)


class CandlesScylla(ScyllaCallback, BackendCallback):
    default_table = CANDLES
    
    def format(self, data: Tuple):
        if self.custom_columns:
            return self._custom_format(data)
        else:
            start = dt.utcfromtimestamp(data[4]['start'])
            stop = dt.utcfromtimestamp(data[4]['stop'])
            exchange, symbol, timestamp, receipt, data = data
            return [exchange, symbol, timestamp, receipt, start, stop, data]

    async def write_batch(self, updates: list):
        try:
            await self._connect()
            self.CQL = f"""
                INSERT INTO {self.key_space}.{self.table}
                (exchange, symbol, timestamp, receipt, start, stop, data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            prepaired = await self.session.create_prepared(self.CQL)
            statement = prepaired.bind()
            print('Write batch candles', self.CQL)
            for update in updates:
                statement.bind_list(self.format(update))
                await self.session.execute(statement)
                print('Write update candles:', self.format(update))
        except Exception as e:
            print(e)


class TradeScylla(ScyllaCallback, BackendCallback):
    default_table = TRADES

    def format(self, data: Tuple):
        if self.custom_columns:
            return self._custom_format(data)
        else:
            exchange, symbol, timestamp, receipt, data = data
            data_minimized = [
                data['id'] if data['id'] else 'NULL',
                data['side'] if data['side'] else 'NULL',
                data['amount'] if data['amount'] else 'NULL',
                data['price'] if data['price'] else 'NULL',
                data['type'] if data['type'] else 'NULL'
            ]
            return [exchange, symbol, timestamp, receipt, data_minimized]


class OpenInterestScylla(ScyllaCallback, BackendCallback):
    default_table = OPEN_INTEREST

    def format(self, data: Tuple):
        if self.custom_columns:
            return self._custom_format(data)
        else:
            exchange, symbol, timestamp, receipt, data = data
            data_minimized = data['open_interest']
            return [exchange, symbol, timestamp, receipt, data_minimized]


class FundingScylla(ScyllaCallback, BackendCallback):
    default_table = FUNDING

    def format(self, data: Tuple):
        if self.custom_columns:
            if data[4]['next_funding_time']:
                data[4]['next_funding_time'] = dt.utcfromtimestamp(data[4]['next_funding_time'])
            return self._custom_format(data)
        else:
            exchange, symbol, timestamp, receipt, data = data
            data_minimized = [
                data['mark_price'],
                data['rate'],
                data['next_funding_time'],
                data['predicted_rate']
            ]
            return [exchange, symbol, timestamp, receipt, data_minimized]


class LiquidationsScylla(ScyllaCallback, BackendCallback):
    default_table = LIQUIDATIONS

    def format(self, data: Tuple):
        if self.custom_columns:
            return self._custom_format(data)
        else:
            exchange, symbol, timestamp, receipt, data = data
            # data_minimized = [
            #     data['mark_price'],
            #     data['rate'],
            #     data['next_funding_time'],
            #     data['predicted_rate']
            # ]
            return [exchange, symbol, timestamp, receipt, data]


class TickerScylla(ScyllaCallback, BackendCallback):
    default_table = TICKER

    def format(self, data: Tuple):
        if self.custom_columns:
            return self._custom_format(data)
        else:
            exchange, symbol, timestamp, receipt, data = data
            data_minimized = [
                data['bid'],
                data['ask']
            ]
            return [exchange, symbol, timestamp, receipt, data_minimized]
    
    # Throttle updates based on `window`. Will allow 1 update per `window` interval; all others are dropped
    async def __call__(self, data, receipt_timestamp):
        now = time.time()
        if now - self.last_update > self.window:
            self.last_update = now
            await super().__call__(data, receipt_timestamp)


class IndexScylla(ScyllaCallback, BackendCallback):
    default_table = INDEX

    def format(self, data: Tuple):
        if self.custom_columns:
            return self._custom_format(data)
        else:
            exchange, symbol, timestamp, receipt, data = data
            # data_minimized = [
            #     data['mark_price'],
            #     data['rate'],
            #     data['next_funding_time'],
            #     data['predicted_rate']
            # ]
            return [exchange, symbol, timestamp, receipt, data]


class BookScylla(ScyllaCallback, BackendBookCallback):
    default_table = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

    def format(self, data: Tuple):
        if self.custom_columns:
            if 'book' in data[4]:
                data[4]['data'] = json.dumps({'snapshot': data[4]['book']})
            else:
                data[4]['data'] = json.dumps({'delta': data[4]['delta']})
            return self._custom_format(data)
        else:
            feed = data[0]
            symbol = data[1]
            timestamp = data[2]
            receipt_timestamp = data[3]
            data = data[4]
            if 'book' in data:
                data = {'snapshot': data['book']}
            else:
                data = {'delta': data['delta']}

            return f"(DEFAULT,'{timestamp}','{receipt_timestamp}','{feed}','{symbol}','{json.dumps(data)}')"

