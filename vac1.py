import asyncio
import psycopg2
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor
import queue
import logging
import multiprocessing
import json

# jq -c '[select(.dbname == "demo3") | {time: .time, dbname: .dbname, percentage: .percentage}] | .[] | select(. != null)' vacuum_progress.log

# Custom JSON formatter for main logger
class MainJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'message': record.getMessage(),
            'name': record.name,
            'category': record.category,
            'percentage': getattr(record, 'percentage', None),
            'dbname': getattr(record, 'dbname', None),
            'hostname': getattr(record, 'hostname', None)  # Added hostname field
        }
        return json.dumps(log_record)


# Custom JSON formatter for database logger
class DbJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'message': record.getMessage(),
            'hostname': record.hostname,  # Added hostname field
            'dbname': record.name,
            'table_name': record.table_name,
            'table_size': getattr(record, 'table_size', None),
            'phase': getattr(record, 'phase', None)
        }
        return json.dumps(log_record)


class VacuumManager:
    def __init__(self):
        self.main_logger = self.setup_main_logger()
        self.db_queue = asyncio.Queue()

    def setup_main_logger(self):
        main_logger = logging.getLogger("main")
        main_logger.setLevel(logging.INFO)

        main_file_handler = logging.FileHandler("vacuum_progress.log")
        main_file_handler.setLevel(logging.INFO)

        main_json_formatter = MainJsonFormatter()
        main_file_handler.setFormatter(main_json_formatter)

        if not main_logger.handlers:  # Avoid adding multiple handlers
            main_logger.addHandler(main_file_handler)

        return main_logger

    def setup_logging(self, db_config):
        logger = logging.getLogger(db_config['dbname'])
        logger.setLevel(logging.INFO)

        log_file = f"{db_config['host']}_{db_config['dbname']}_vacuum.log"  # Include hostname in log file name
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        db_json_formatter = DbJsonFormatter()
        file_handler.setFormatter(db_json_formatter)

        if not logger.handlers:  # Avoid adding multiple handlers
            logger.addHandler(file_handler)

        return logger

    def get_tables(self, db_config):
        conn = psycopg2.connect(**db_config)
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
            print(f"Error retrieving tables from {db_config['dbname']}: {e}")
        finally:
            conn.close()
        return tables

    def vacuum_table(self, db_config, table_name, logger, main_logger, db_name, total_tables, progress_tracker):
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET work_mem = '6GB';")
                cursor.execute("SET maintenance_work_mem = '6GB';")

                cursor.execute(sql.SQL("SELECT pg_total_relation_size('{}');").format(sql.Identifier(table_name)))
                table_size = cursor.fetchone()[0]

                logger.info(f"Starting vacuum on table: {table_name}",
                            extra={'table_name': table_name, 'table_size': table_size, 'phase': 'start', 'hostname': db_config['host']})

                cursor.execute(sql.SQL("VACUUM {};").format(sql.Identifier(table_name)))

                logger.info(f"Finished vacuuming table: {table_name}",
                            extra={'table_name': table_name, 'table_size': table_size, 'phase': 'end', 'hostname': db_config['host']})

                with progress_tracker.get_lock():
                    progress_tracker.value += 1
                    percentage = (progress_tracker.value / total_tables) * 100
                    main_logger.info(f"{db_name}: {percentage:.2f}% of tables vacuumed.",
                                     extra={'category': 'progress', 'percentage': round(percentage, 2), 'dbname': db_name, 'hostname': db_config['host']})
        except Exception as e:
            logger.error(f"Error vacuuming table {table_name}: {e}", extra={'table_name': table_name})
        finally:
            conn.close()

    async def manage_vacuuming(self, db_config, logger):
        tables_to_vacuum = self.get_tables(db_config)
        total_tables = len(tables_to_vacuum)

        if total_tables == 0:
            logger.warning(f"No tables found to vacuum in {db_config['dbname']}.")
            return

        progress_tracker = multiprocessing.Value('i', 0)

        table_queue = queue.Queue()
        for table in tables_to_vacuum:
            table_queue.put(table)

        with ThreadPoolExecutor(max_workers=5) as executor:
            while not table_queue.empty():
                table_name = table_queue.get()
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(executor, self.vacuum_table, db_config, table_name, logger,
                                           self.main_logger, db_config['dbname'], total_tables, progress_tracker)
                table_queue.task_done()

    async def run(self, db_configs):
        for db_config in db_configs:
            await self.db_queue.put(db_config)

        tasks = []
        while not self.db_queue.empty():
            for _ in range(min(5, self.db_queue.qsize())):  # Process up to 5 databases at a time
                db_config = await self.db_queue.get()
                logger = self.setup_logging(db_config)
                tasks.append(self.manage_vacuuming(db_config, logger))

            await asyncio.gather(*tasks)
            tasks.clear()  # Clear tasks for the next round

if __name__ == "__main__":
    DB_CONFIGS = [
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
        },
        # Add more databases as needed
    ]

    manager = VacuumManager()
    asyncio.run(manager.run(DB_CONFIGS))
