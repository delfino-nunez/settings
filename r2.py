#!/usr/bin/env python3
#
# Reindex a warehouse tables
#
#

import sys
import argparse
import logging
import time
import json
import queue
import multiprocessing
import asyncio

from pathlib import Path
from collections import defaultdict

from psycopg2 import sql
from common.logger import setup_logger
from common.warehouse import Warehouse
from config import verafin_defaults as vd
from concurrent.futures import ThreadPoolExecutor

FILTER_TABLES = "bk|bkp|backup|bak|delete|remove|temp|tmp|stg"
SCRIPT_DESCRIPTION = "ReIndex warehouse tables"
LINE_SIZE = 80

MAX_CONCURRENT_SERVERS = 2
MAX_CONCURRENT_DBS = 1
MAX_CONCURRENT_TABLES = 2

class MainJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'name': record.name,
            'category': record.category,
            'percentage': getattr(record, 'percentage', None),
            'db': getattr(record, 'db', None),
            'host': getattr(record, 'host', None),
            'message': record.getMessage()
        }
        return json.dumps(log_record)

class ProgressLogger:
    def __init__(self, name, log_file):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self._setup_handler(log_file)

    def _setup_handler(self, log_file):
        handler = logging.FileHandler(log_file)
        handler.setFormatter(MainJsonFormatter())
        if not self.logger.handlers:
            self.logger.addHandler(handler)


class DbJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'phase': getattr(record, 'phase', None),
            'table': getattr(record, 'table', None),
            'index': getattr(record, 'index', None),
            'size': getattr(record, 'size', None),
            'space_released': getattr(record, 'space_released', None),
            'tables_processed': getattr(record, 'tables_processed', None),
            'index_processed': getattr(record, 'index_processed', None),
            'host': getattr(record, 'host', None),
            'message': record.getMessage()
        }
        return json.dumps(log_record)

class DBLogger:
    def __init__(self, name, log_file,log_level):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self._setup_handler(log_file,log_level)

    def _setup_handler(self, log_file,log_level):
        self.logger.setLevel(log_level)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)

        db_json_formatter = DbJsonFormatter()
        file_handler.setFormatter(db_json_formatter)

        if not self.logger.handlers:  # Avoid adding multiple handlers
            self.logger.addHandler(file_handler)


class Database(Warehouse):
    """Database to reindex"""

    def __init__(self,name, log_level):
        super().__init__(name)
        self.logger = None
        self.tables = []
        self.__setup_logger(log_level)


    def __setup_logger(self, log_level):
        server_name = (self.server_name.split('.')[0]
                       .replace("prod-db-warehouse-us-east-1-","")
                       .replace("-cluster","")
                       )
        log_file = f"reindex_{server_name}_{self.name}.log"

        self.logger = DBLogger(self.name, log_file, log_level).logger


class TableProcessor:
    """Handles reindexing a single table"""

    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.indexes = []
        self.script_logger = log = logging.getLogger("main_logger")

    async def get_indexes(self):
        """Get tables indexes"""
        query_stmt = """
        select indexrelname
          from pg_stat_all_indexes
          where schemaname NOT IN ('pg_catalog', 'information_schema','pg_toast')
            and relname not similar to '%({})%'
            and relname = '{}'
          order by idx_scan desc
        """.format(FILTER_TABLES, self.table_name)

        try:
            index_list = []
            self.script_logger.debug(f'Getting table indexes for {self.table_name}')
            indexes = self.db.query(query_stmt)
            for index in indexes:
                self.script_logger.debug(f'Adding index {index} to list')
                self.indexes.append(index[0])
        except Exception as e:
            self.script_logger.error(f"Error retrieving indexes for table {self.table_name}: {e}")

    async def reindex(self):
        """Reindex table"""
        set_work_mem = "set maintenance_work_mem = '6GB'"
        reindex_stmt = 'reindex index concurrently "{}";'
        indexes_processed = 0


        self.script_logger.debug(f'Setting session settings:{set_work_mem}')
        self.db.ddl(set_work_mem)
        await self.get_indexes()
        total_table_indexes = len(self.indexes)

        for index in self.indexes:
            self.script_logger.info(f'Reindexing {self.db.name}.{index}')
            self.script_logger.debug(f'SQL: {reindex_stmt.format(index)}')
            try:
                self.db.logger.info(f"Reindexing {index}",
                               extra={'phase': 'start_reindex',
                                      'table': self.table_name,
                                      'index': index,
                                      'size': 0,
                                      'tables_processed': f"{0}",
                                      'index_processed': f"{str(indexes_processed)}/{str(total_table_indexes)}"
                                      })

                # self.db.ddl(reindex_stmt.format(index))
                indexes_processed += 1

                self.db.logger.info(f"Reindexing {index}",
                               extra={'phase': 'end_reindex',
                                      'table': self.table_name,
                                      'index': index,
                                      'size': 0,
                                      'tables_processed': f"{0}",
                                      'index_processed': f"{str(indexes_processed)}/{str(total_table_indexes)}"
                                      })

            except Exception as e:
                self.script_logger.error(f"Error reindexing {self.table_name}({index}): {e}")

            # await self.db.ddl(reindex_stmt.format(index))


class PostgresReindexer:
    """Orchestrates postgres reindexing a particular DB"""
    def __init__(self, db, limit=None):
        self.db = db
        self.script_logger = log = logging.getLogger("main_logger")
        self.progress_logger = log = logging.getLogger("progress")

    async def get_tables(self, limit=None):
        """ Retrieve the top N tables in the database"""
        query_stmt = """
            select tablename 
              from pg_tables 
             where schemaname = 'public'  
             order by
                 pg_total_relation_size(quote_ident(tablename)) - pg_relation_size(quote_ident(tablename)) desc
            """
        if limit is not None:
            query_stmt = f"{query_stmt} LIMIT {limit}"

        try:
            self.script_logger.debug(f'Getting tables in database: {self.db.name}')
            self.script_logger.debug(f'SQL: {query_stmt}')
            tables = self.db.query(query_stmt)

            for table in tables:
                self.script_logger.debug(f'Adding table {table} to database list.')
                self.db.tables.append(table[0])
        except Exception as e:
            self.script_logger.error(f"Error retrieving tables for {self.name}: {e}")

    async def process_table(self, table_name: str):
        """Reindex all table indexes"""
        table_processor = TableProcessor(self.db, table_name)
        self.script_logger.debug(f'Processing table: {table_name}')
        await table_processor.reindex()

    async def process_db(self, progress_tracker):
        """Reindex the selected database"""
        self.script_logger.debug(f'Getting tables for {self.db.name}')
        await self.get_tables()

        total_tables_to_reindex = len(self.db.tables)
        tables_processed = f"{str(progress_tracker.value)} / {str(total_tables_to_reindex)}"
        tables_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABLES)

        if total_tables_to_reindex == 0:
            self.db.logger.warning(f"No tables to reindex in {self.db.name}",
                              extra={'host': self.db.server_name.split('.')[0]})
            return

        async def process_table_with_semaphore(table_name: str):
            async with tables_semaphore:
                self.script_logger.debug(f'Processing table {table_name}')
                await self.process_table(table_name)


        tasks = []
        # process tables in parallel
        for table_name in self.db.tables:
            # table_size_before = db.get_table_size(table)[0][0]
            table_size_before = 0
            self.db.logger.info(f"Reindexing table {table_name}",
                                extra={'phase': 'start_table_reindex',
                                       'table': table_name,
                                       'size': table_size_before,
                                       'tables_processed': tables_processed,
                                       'host': self.db.server_name.split('.')[0],
                                       })
            task = asyncio.create_task(process_table_with_semaphore(table_name))
            tasks.append(task)

            with progress_tracker.get_lock():
                progress_tracker.value += 1
                percentage = round((progress_tracker.value / total_tables_to_reindex) * 100,2)
                tables_processed = f"{str(progress_tracker.value)} / {str(total_tables_to_reindex)}"

                self.progress_logger.info(f"{self.db.name}: {percentage:.2f}% of tables reindexed.",
                                          extra={'category': 'progress',
                                                 'percentage': percentage,
                                                 'tables_processed': tables_processed,
                                                 'db': self.db.name,
                                                 'host': self.db.server_name.split('.')[0]})

            # table_size_after = db.get_table_size(table)[0][0]
            table_size_after = 0

            space_released = table_size_before - table_size_after
            self.db.logger.info(f"Reindexed table {table_name}",
                           extra={'phase': 'end_table_reindex',
                                  'table': table_name,
                                  'size': table_size_after,
                                  'tables_processed': tables_processed,
                                  'space_released': space_released,
                                  'host': self.db.server_name.split('.')[0],
                                  })

        await asyncio.gather(*tasks)
        # await asyncio.gather(*[process_table_with_semaphore(table_name) for table_name in self.db.tables])

class PostgresServerManager:
    """Process multiple servers concurrently"""
    def __init__(self, parameters):
        self.parameters = parameters
        self.script_logger = log = logging.getLogger("main_logger")
        self.progress_logger = None
        self.database_queue = asyncio.Queue()
        self.processing_servers = {}

        self.__setup_progress_logger()

    def __setup_progress_logger(self):
        """
        Setup the progress logging
        """
        # log_file = f"reindex_progress_{time.strftime('%Y%m%d-%H%M%S')}.log"
        log_file = f"reindex_progress.log"

        log_level = logging.DEBUG if self.parameters["log_level"] else logging.INFO

        progress_logger = logging.getLogger("progress")
        progress_logger.setLevel(log_level)

        main_file_handler = logging.FileHandler(log_file)
        main_file_handler.setLevel(log_level)

        main_json_formatter = MainJsonFormatter()
        main_file_handler.setFormatter(main_json_formatter)

        if not progress_logger.handlers:  # Avoid adding multiple handlers
            progress_logger.addHandler(main_file_handler)

        self.progress_logger = progress_logger

    async def organize_databases(self):
        """
        Organize which databases and tables will be reindexed
        """
        log_level = logging.DEBUG if self.parameters["log_level"] else logging.INFO
        self.script_logger.info('Organizing databases...')
        for db in self.parameters['database_list']:
            self.script_logger.debug(f'Creating {db} object')
            database = Database(db, log_level)
            self.script_logger.debug(f'Adding {database.name} to database queue.')
            await self.database_queue.put(database)

    async def process_db(self, db):
        top_n_tables = self.parameters['top_n_tables']
        self.script_logger.debug(f'Reindexing {db.name} top {top_n_tables} tables...')
        db_reindexer = PostgresReindexer(db, top_n_tables)
        self.script_logger.debug(f'Reindexing {db.name}')

        progress_tracker = multiprocessing.Value('i', 0)
        self.progress_logger.info(f"Reindexing {db.name}",
                                  extra={'category': 'progress',
                                         'percentage': 0,
                                         'db': db.name,
                                         'host': db.server_name, })
        await db_reindexer.process_db(progress_tracker)

    async def process_all_servers(self):
        """Process servers concurrently wit a limit of N servers at a time"""
        server_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SERVERS)

        await self.organize_databases()

        while not self.database_queue.empty():
            self.script_logger.debug(f'Getting {db.name} from database queue for processing...')
            db = await self.database_queue.get()

            #if server is not processing a DB start doing it else place it back in the queue
            if db.server_name not in self.processing_servers:
                self.script_logger.debug(f'Adding {db.server_name} server to server processing queue...')
                self.processing_servers[db.server_name] = True

                # process the DB in the server
                async with server_semaphore:
                    self.script_logger.debug(f'Processing db {db.name}...')
                    await self.process_db(db)
                    # remove server to allow another DB from the same server to process
                del self.processing_servers[db.server_name]
                self.script_logger.debug(f'Removed {db.server_name} from server process queue')
            else:
                # requeue the DB since the server is already processing one
                await self.database_queue.put_nowait(db)
                self.script_logger.debug(f'Re-queue server {db.server_name}')


def parameter_parser():
    """
    Setup and parse parameters
    :return:
        configuration for parameters
    """

    main_parser = argparse.ArgumentParser(
        description=SCRIPT_DESCRIPTION,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparser = main_parser.add_subparsers(
        title="Operations",
        dest="operation",
        help="Operation to execute."
    )

    #--------
    #-- INITIAL PARSER
    #--------

    table_parser = subparser.add_parser(name="tables",
                                        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                        help="ReIndex a list of tables.")

    table_parser.add_argument("-w","--warehouse",
                              required=True,
                              dest="database_name",
                              help="Name of the database where the table exists.")

    table_parser.add_argument("-t","--table-list",
                              required=True,
                              nargs="+",
                              dest="table_list",
                              help="List of tables to reindex. Example: -t T1 T2 Tx"
                              )

    database_parser = subparser.add_parser(name="databases",
                                           formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                           help="ReIndex a database top N tables.")

    database_parser.add_argument("-w","--warehouse",
                                 required=True,
                                 nargs="+",
                                 dest="database_list",
                                 help="List of databases to reindex, separated by space. Example: -w DB1 DB2 DBx")

    database_parser.add_argument("-n","--top-n ",
                                 required=True,
                                 dest="top_n_tables",
                                 default=10,
                                 help="Reindex the top N tables. Example -n 5"
                                 )

    #--------
    #-- Standard options
    #--------
    default_logfile = Path(__file__).stem + "_" + time.strftime("%Y%m%d-%H%M%S") + ".log"

    main_parser.add_argument("-l", "--logfile",
                             required=False,
                             dest="log_filename",
                             default=default_logfile,
                             help="File name where logs are stored.")

    main_parser.add_argument("-v", "--verbose",
                             required=False,
                             dest="verbose",
                             action="store_true",
                             default=False,
                             help="Turn on console output.")

    main_parser.add_argument("-d", "--debug",
                             required=False,
                             dest="log_level",
                             action="store_true",
                             default=False,
                             help="Log DEBUG messages.")

    main_parser.add_argument("-p", "--profile",
                             required=False,
                             default=vd.PROFILE,
                             dest="aws_profile",
                             help="The target object ARN where to paste the tags.")

    main_parser.add_argument("-r", "--region",
                             required=False,
                             default=vd.REGION_NAME,
                             dest="aws_region",
                             help="The target object ARN where to paste the tags.")

    args = main_parser.parse_args()
    config = vars(args)
    return config


async def main(argv):
    parameters = parameter_parser()
    log = setup_logger(parameters["log_filename"], parameters["verbose"], parameters["log_level"])
    operation = parameters["operation"]

    log.info("=" * LINE_SIZE)
    log.info("Executing script settings")
    log.info(f"Operation: {operation}")

    if operation == "tables":
        log.info(f"Database: {parameters['database_name']}")
        log.info(f"Table(s): {parameters['table_list']}")

    elif operation == "databases":
        log.info(f"Database: {parameters['database_list']}")
        log.info(f"Top N tables: {parameters['top_n_tables']}")

    log.info("=" * LINE_SIZE)

    server_manager = PostgresServerManager(parameters)

    await server_manager.process_all_servers()


if __name__ == "__main__":
    asyncio.run(main(sys.argv))
