import asyncio
import psycopg2
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor
import queue
import logging
import multiprocessing
import json
from collections import defaultdict


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'message': record.getMessage(),
            'name': record.name,
            'category': getattr(record, 'category', None),
            'percentage': getattr(record, 'percentage', None),
            'dbname': getattr(record, 'dbname', None),
            'table_name': getattr(record, 'table_name', None),
            'table_size': getattr(record, 'table_size', None),
            'phase': getattr(record, 'phase', None),
            'blocking': getattr(record, 'blocking', None),
        }
        return json.dumps(log_record)


class Logger:
    def __init__(self, name, log_file):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self._setup_handler(log_file)

    def _setup_handler(self, log_file):
        handler = logging.FileHandler(log_file)
        handler.setFormatter(JsonFormatter())
        if not self.logger.handlers:
            self.logger.addHandler(handler)


class TableVacuum:
    def __init__(self, db_config):
        self.db_config = db_config
        self.logger = Logger(db_config['dbname'], f"{db_config['dbname']}_vacuum.log").logger

    def check_blocking(self):
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT pid, blocked_pid
                    FROM pg_locks l
                    JOIN pg_stat_activity a ON l.pid = a.pid
                    WHERE NOT l.granted;
                """)
                blocking_info = cursor.fetchall()
                return blocking_info
        except Exception as e:
            self.logger.error(f"Error checking blocking: {e}")
            return []
        finally:
            conn.close()

    def terminate_process(self, pid):
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT pg_terminate_backend({});").format(sql.Literal(pid)))
                self.logger.info(f"Terminated process with PID: {pid}")
        except Exception as e:
            self.logger.error(f"Error terminating process {pid}: {e}")
        finally:
            conn.close()

    async def vacuum(self, table_name, progress_tracker):
        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = True
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT pg_total_relation_size('{}');").format(sql.Identifier(table_name)))
                table_size = cursor.fetchone()[0]

                self.logger.info(f"Starting vacuum on table: {table_name}",
                                 extra={'table_name': table_name, 'table_size': table_size, 'phase': 'start'})

                blocking_task = asyncio.create_task(self.monitor_blocking(cursor))

                cursor.execute(sql.SQL("VACUUM {};").format(sql.Identifier(table_name)))

                # Cancel blocking monitor after vacuuming is done
                blocking_task.cancel()
                try:
                    await blocking_task
                except asyncio.CancelledError:
                    pass

                self.logger.info(f"Finished vacuuming table: {table_name}",
                                 extra={'table_name': table_name, 'table_size': table_size, 'phase': 'end'})

                with progress_tracker.get_lock():
                    progress_tracker.value += 1
        except Exception as e:
            self.logger.error(f"Error vacuuming table {table_name}: {e}", extra={'table_name': table_name})
        finally:
            conn.close()

    async def monitor_blocking(self, cursor):
        blocking_threshold = 5  # seconds before considering the vacuum as blocking
        while True:
            blocking_info = self.check_blocking()
            if blocking_info:
                for pid, blocked_pid in blocking_info:
                    self.logger.warning(f"Blocking detected: PID {pid} is blocking PID {blocked_pid}")
                    # Terminate the vacuum process if it is blocking
                    self.terminate_process(pid)
            await asyncio.sleep(2)  # Check every 2 seconds


class Database:
    def __init__(self, config):
        self.config = config
        self.logger = Logger(config['dbname'], f"{config['dbname']}_vacuum.log").logger

    def get_tables(self):
        conn = psycopg2.connect(**self.config)
        tables = []
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                """)
                tables = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error retrieving tables from {self.config['dbname']}: {e}")
        finally:
            conn.close()
        return tables


class VacuumManager:
    def __init__(self, db_configs):
        self.db_configs = db_configs
        self.main_logger = Logger("main", "vacuum_progress.log").logger
        self.host_queues = defaultdict(queue.Queue)

    def organize_databases(self):
        for config in self.db_configs:
            db = Database(config)
            tables_to_vacuum = db.get_tables()
            for table in tables_to_vacuum:
                self.host_queues[config['host']].put((db, table))

    async def manage_vacuuming(self):
        tasks = []
        for host, host_queue in self.host_queues.items():
            while not host_queue.empty():
                db, table_name = host_queue.get()
                progress_tracker = multiprocessing.Value('i', 0)
                total_tables = len(host_queue.queue)

                task = asyncio.create_task(self.run_vacuuming(db, table_name, progress_tracker, total_tables))
                tasks.append(task)

        await asyncio.gather(*tasks)

    async def run_vacuuming(self, db, table_name, progress_tracker, total_tables):
        vacuumer = TableVacuum(db.config)
        await vacuumer.vacuum(table_name, progress_tracker)


async def main():
    db_configs = [
        {
            'dbname': 'demo1',
            'user': 'demo',
            'password': 'demo',
            'host': 'localhost',
            'port': '5432'
        },
        {
            'dbname': 'demo2',
            'user': 'demo',
            'password': 'demo',
            'host': 'localhost',
            'port': '5432'
        },
        {
            'dbname': 'demo3',
            'user': 'demo',
            'password': 'demo',
            'host': 'localhost',
            'port': '5432'
        }
    ]

    vacuum_manager = VacuumManager(db_configs)
    vacuum_manager.organize_databases()
    await vacuum_manager.manage_vacuuming()


if __name__ == "__main__":
    asyncio.run(main())
