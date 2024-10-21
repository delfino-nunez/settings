import asyncio
import psycopg2
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor
import queue
import logging
import multiprocessing
import json


# Database connection details for multiple databases
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
    }

]


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
            'dbname': getattr(record, 'dbname', None)
        }
        return json.dumps(log_record)


# Custom JSON formatter for database logger
class DbJsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record),
            'level': record.levelname,
            'message': record.getMessage(),
            'dbname': record.name,  # Database name
            'table_name': record.table_name,
            'table_size': getattr(record, 'table_size', None),  # Table size
            'phase': getattr(record, 'phase', None)  # Add phase
        }
        return json.dumps(log_record)




# Function to configure logging for each database
def setup_logging(db_name):
    logger = logging.getLogger(db_name)
    logger.setLevel(logging.INFO)

    # Create a file handler for the database logger
    log_file = f"{db_name}_vacuum.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Set the JSON formatter for the database logger
    db_json_formatter = DbJsonFormatter()
    file_handler.setFormatter(db_json_formatter)

    # Add the file handler to the logger
    if not logger.handlers:  # Avoid adding multiple handlers
        logger.addHandler(file_handler)

    return logger


# Function to setup the main progress logger
def setup_main_logger():
    main_logger = logging.getLogger("main")
    main_logger.setLevel(logging.INFO)

    # Create a file handler for the main log
    main_file_handler = logging.FileHandler("vacuum_progress.log")
    main_file_handler.setLevel(logging.INFO)

    # Set the JSON formatter for the main logger
    main_json_formatter = MainJsonFormatter()
    main_file_handler.setFormatter(main_json_formatter)

    if not main_logger.handlers:  # Avoid adding multiple handlers
        main_logger.addHandler(main_file_handler)

    return main_logger


def vacuum_table(db_config, table_name, logger, main_logger, db_name, total_tables, progress_tracker):
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True  # Enable autocommit mode
    try:
        with conn.cursor() as cursor:
            # Get the table size before vacuuming with single quotes
            cursor.execute(sql.SQL("SELECT pg_total_relation_size('{}');").format(sql.Identifier(table_name)))
            table_size = cursor.fetchone()[0]

            # Log starting phase
            logger.info(f"Starting vacuum on table: {table_name}",
                        extra={'table_name': table_name, 'table_size': table_size, 'phase': 'start'})

            # Perform the vacuum
            cursor.execute(sql.SQL("VACUUM {};").format(sql.Identifier(table_name)))

            # Log ending phase
            logger.info(f"Finished vacuuming table: {table_name}",
                        extra={'table_name': table_name, 'table_size': table_size, 'phase': 'end'})

            # Update progress
            with progress_tracker.get_lock():
                progress_tracker.value += 1
                percentage = (progress_tracker.value / total_tables) * 100
                main_logger.info(f"{db_name}: {percentage:.2f}% of tables vacuumed.",
                                 extra={'category': 'progress', 'percentage': round(percentage, 2), 'dbname': db_name})
    except Exception as e:
        logger.error(f"Error vacuuming table {table_name}: {e}", extra={'table_name': table_name})
    finally:
        conn.close()


# Asynchronous function to manage vacuuming with a thread pool
async def manage_vacuuming(db_config, table_queue, logger, main_logger, db_name, total_tables, progress_tracker):
    with ThreadPoolExecutor(max_workers=5) as executor:
        while not table_queue.empty():
            table_name = table_queue.get()
            if table_name is None:  # Exit signal
                break
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, vacuum_table, db_config, table_name, logger, main_logger, db_name,
                                       total_tables, progress_tracker)
            table_queue.task_done()


# Function to retrieve tables dynamically from a database
def get_tables(db_config):
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


async def main():
    main_logger = setup_main_logger()  # Set up main progress logger

    # Summary of databases and tables to vacuum
    summary = []
    for db_config in DB_CONFIGS:
        tables_to_vacuum = get_tables(db_config)
        num_tables = len(tables_to_vacuum)
        summary.append(f"{db_config['dbname']}: {num_tables} tables to vacuum.")

    # Log the summary
    main_logger.info("Vacuuming Summary:", extra={'category': 'summary'})
    for db_config in DB_CONFIGS:
        num_tables = len(get_tables(db_config))
        main_logger.info(f"{db_config['dbname']}: {num_tables} tables to vacuum.",
                         extra={'category': 'summary', 'dbname': db_config['dbname']})

    tasks = []
    for db_config in DB_CONFIGS:
        # Setup logging for this database
        logger = setup_logging(db_config['dbname'])

        # Retrieve tables dynamically for each database
        tables_to_vacuum = get_tables(db_config)
        if not tables_to_vacuum:
            logger.warning(f"No tables found to vacuum in {db_config['dbname']}.")
            continue

        total_tables = len(tables_to_vacuum)
        progress_tracker = multiprocessing.Value('i', 0)  # To track progress

        # Create a queue and populate it with table names
        table_queue = queue.Queue()
        for table in tables_to_vacuum:
            table_queue.put(table)

        # Create a vacuuming task for this database
        tasks.append(manage_vacuuming(db_config, table_queue, logger, main_logger, db_config['dbname'], total_tables,
                                      progress_tracker))

    # Run all vacuuming tasks concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
