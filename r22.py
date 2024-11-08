#!/usr/bin/env python3

import sys
import argparse
import logging
import time
import json
import asyncio
import multiprocessing
from pathlib import Path
from collections import defaultdict

from psycopg2 import sql
from common.logger import setup_logger
from common.warehouse import Warehouse
from config import verafin_defaults as vd

FILTER_TABLES = "bk|bkp|backup|bak|delete|remove|temp|tmp|stg"
SCRIPT_DESCRIPTION = "ReIndex warehouse tables"
LINE_SIZE = 80

# New configurable limit on the number of servers processed concurrently
MAX_CONCURRENT_SERVERS = 2  # Default limit, can be set by the user in arguments
MAX_CONCURRENT_DBS = 1
MAX_CONCURRENT_TABLES = 2


class JsonLogFormatter(logging.Formatter):
    """Custom formatter for JSON logging"""
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'name': record.name,
            'category': getattr(record, 'category', None),
            'percentage': getattr(record, 'percentage', None),
            'db': getattr(record, 'db', None),
            'host': getattr(record, 'host', None),
            'message': record.getMessage()
        }
        return json.dumps(log_record)


class LoggerMixin:
    """Mixin class for setting up logging"""
    def __init__(self, name, log_file, log_level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self._setup_handler(log_file)

    def _setup_handler(self, log_file):
        handler = logging.FileHandler(log_file)
        handler.setFormatter(JsonLogFormatter())
        if not self.logger.handlers:
            self.logger.addHandler(handler)


class Database(Warehouse):
    """Database to reindex"""
    def __init__(self, name, log_level):
        super().__init__(name)
        self.tables = []
        server_name = self.server_name.split('.')[0].replace("prod-db-warehouse-us-east-1-", "").replace("-cluster", "")
        log_file = f"reindex_{server_name}_{self.name}.log"
        self.logger = LoggerMixin(self.name, log_file, log_level).logger


class TableProcessor:
    """Handles reindexing a single table"""
    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.indexes = []
        self.script_logger = logging.getLogger("main_logger")

    async def get_indexes(self):
        """Get table indexes"""
        query_stmt = f"""
            SELECT indexrelname
            FROM pg_stat_all_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            AND relname NOT SIMILAR TO '%({FILTER_TABLES})%'
            AND relname = '{self.table_name}'
            ORDER BY idx_scan DESC
        """
        try:
            self.script_logger.debug(f'Getting indexes for table {self.table_name}')
            indexes = self.db.query(query_stmt)
            self.indexes = [index[0] for index in indexes]
        except Exception as e:
            self.script_logger.error(f"Error retrieving indexes for {self.table_name}: {e}")

    async def reindex(self):
        """Reindex table"""
        set_work_mem = "SET maintenance_work_mem = '6GB'"
        reindex_stmt = 'REINDEX INDEX CONCURRENTLY "{}";'
        await self.db.ddl(set_work_mem)
        await self.get_indexes()

        for idx, index in enumerate(self.indexes, start=1):
            self.script_logger.info(f'Reindexing {self.db.name}.{index}')
            try:
                self.db.logger.info(f"Reindexing {index}",
                                    extra={'phase': 'start_reindex',
                                           'table': self.table_name,
                                           'index': index,
                                           'index_processed': f"{idx}/{len(self.indexes)}"})
                # Actual reindexing process (uncomment below line when ready)
                # self.db.ddl(reindex_stmt.format(index))
                self.db.logger.info(f"Reindexing {index}",
                                    extra={'phase': 'end_reindex',
                                           'table': self.table_name,
                                           'index': index,
                                           'index_processed': f"{idx}/{len(self.indexes)}"})
            except Exception as e:
                self.script_logger.error(f"Error reindexing {self.table_name}({index}): {e}")


class PostgresReindexer:
    """Orchestrates PostgreSQL reindexing for a particular DB"""
    def __init__(self, db, limit=None):
        self.db = db
        self.limit = limit
        self.script_logger = logging.getLogger("main_logger")
        self.progress_logger = logging.getLogger("progress")

    async def get_tables(self):
        """Retrieve top N tables in the database"""
        query_stmt = """
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'  
            ORDER BY pg_total_relation_size(quote_ident(tablename)) - pg_relation_size(quote_ident(tablename)) DESC
        """
        if self.limit:
            query_stmt = f"{query_stmt} LIMIT {self.limit}"

        try:
            self.script_logger.debug(f'Getting tables for database {self.db.name}')
            tables = self.db.query(query_stmt)
            self.db.tables = [table[0] for table in tables]
        except Exception as e:
            self.script_logger.error(f"Error retrieving tables for {self.db.name}: {e}")

    async def process_table(self, table_name):
        """Reindex all indexes of a table"""
        table_processor = TableProcessor(self.db, table_name)
        await table_processor.reindex()

    async def process_db(self, progress_tracker):
        """Reindex the selected database"""
        await self.get_tables()
        total_tables = len(self.db.tables)
        if total_tables == 0:
            self.db.logger.warning(f"No tables to reindex in {self.db.name}")
            return

        tasks = []
        for table_name in self.db.tables:
            tasks.append(self.process_table(table_name))

            # Progress tracking
            with progress_tracker.get_lock():
                progress_tracker.value += 1
                percentage = (progress_tracker.value / total_tables) * 100
                self.progress_logger.info(f"{self.db.name}: {percentage:.2f}% reindexed.")

        await asyncio.gather(*tasks)


class PostgresServerManager:
    """Handles processing multiple servers concurrently"""
    def __init__(self, parameters):
        self.parameters = parameters
        self.script_logger = logging.getLogger("main_logger")
        self.database_queue = asyncio.Queue()
        self.max_concurrent_servers = parameters.get("max_concurrent_servers", MAX_CONCURRENT_SERVERS)

    async def organize_databases(self):
        """Organize databases for processing"""
        for db_name in self.parameters['database_list']:
            db = Database(db_name, logging.DEBUG if self.parameters["log_level"] else logging.INFO)
            await self.database_queue.put(db)

    async def process_db(self, db, progress_tracker):
        """Reindex a database's top N tables"""
        db_reindexer = PostgresReindexer(db, self.parameters.get('top_n_tables'))
        await db_reindexer.process_db(progress_tracker)

    async def process_all_servers(self):
        """Process servers concurrently with a limit on the number of servers"""
        server_semaphore = asyncio.Semaphore(self.max_concurrent_servers)  # Limit concurrent servers

        await self.organize_databases()

        async def process_databases():
            while not self.database_queue.empty():
                db = await self.database_queue.get()

                # Use a semaphore to limit the number of concurrent servers being processed
                async with server_semaphore:
                    progress_tracker = multiprocessing.Value('i', 0)
                    await self.process_db(db, progress_tracker)

        await process_databases()


def parameter_parser():
    """Setup and parse parameters"""
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="operation", help="Operation to execute.")
    
    table_parser = subparsers.add_parser("tables", help="ReIndex a list of tables.")
    table_parser.add_argument("-w", "--warehouse", required=True, dest="database_name", help="Database name.")
    table_parser.add_argument("-t", "--table-list", required=True, nargs="+", dest="table_list", help="Tables to reindex.")

    db_parser = subparsers.add_parser("databases", help="ReIndex a database's top N tables.")
    db_parser.add_argument("-w", "--warehouse", required=True, nargs="+", dest="database_list", help="Databases to reindex.")
    db_parser.add_argument("-n", "--top-n", required=True, dest="top_n_tables", default=10, help="Top N tables to reindex.")
    db_parser.add_argument("-m", "--max-concurrent-servers", required=False, type=int, default=MAX_CONCURRENT_SERVERS,
                           help="Limit the number of
