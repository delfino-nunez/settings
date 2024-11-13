import asyncio
import psycopg2
import logging
import json
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler

# Dictionary to track each server's availability status
server_status = {}

# Create loggers
main_logger = logging.getLogger("MainLogger")
individual_loggers = {}

# Main log file setup
main_handler = RotatingFileHandler('main_log.json', maxBytes=5 * 1024 * 1024, backupCount=3)
main_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))  # Include timestamp in logs
main_logger.addHandler(main_handler)
main_logger.setLevel(logging.INFO)


# Function to create and get individual database loggers
def get_individual_logger(host, dbname):
    log_filename = f"{host}_{dbname}_database_log.json"
    if log_filename not in individual_loggers:
        # Create individual log files for each database
        handler = RotatingFileHandler(log_filename, maxBytes=5 * 1024 * 1024, backupCount=3)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))  # Include timestamp in logs
        logger = logging.getLogger(f"{host}_{dbname}Logger")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        individual_loggers[log_filename] = logger
    return individual_loggers[log_filename]


def is_server_available(host):
    """Check if a server is available for processing."""
    return server_status.get(host, True)


def set_server_status(host, status):
    """Set the server's processing status."""
    server_status[host] = status


def get_top_10_tables(conn):
    """
    Fetch the top 10 largest tables by size.
    """
    query = """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(quote_ident(tablename)) DESC
        LIMIT 10;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
    return tables


def reindex_table(conn, table, host, dbname, index, total_tables, logger):
    """
    Perform reindex on a single table and log progress immediately after reindexing.
    """
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("REINDEX TABLE {};").format(sql.Identifier(table)))
    conn.commit()

    # Log each table being processed in the individual database log
    logger.info(json.dumps({
        "event": "TableReindexed",
        "host": host,
        "dbname": dbname,
        "table": table,
        "table_index": index,
        "total_tables": total_tables
    }))

    # Log the progress in the main log
    main_logger.info(json.dumps({
        "event": "Progress",
        "host": host,
        "dbname": dbname,
        "table": table,
        "processed_tables": index,
        "total_tables": total_tables,
        "progress_percentage": (index / total_tables) * 100
    }))


def process_server(server_config):
    """
    Connect to the server, fetch the top 10 tables, and reindex them.
    """
    host = server_config['host']
    dbname = server_config['dbname']
    if not is_server_available(host):
        return False  # Server is busy

    set_server_status(host, False)  # Mark server as busy
    logger = get_individual_logger(host, dbname)  # Get the logger for the individual server

    try:
        # Connect to the database
        conn = psycopg2.connect(**server_config)
        tables = get_top_10_tables(conn)

        if not tables:
            return True  # No tables to reindex

        total_tables = len(tables)

        # Log the start of the reindexing process
        main_logger.info(json.dumps({
            "event": "ReindexingStarted",
            "host": host,
            "dbname": dbname,
            "total_tables": total_tables
        }))

        # Create a thread pool for reindexing tables concurrently
        with ThreadPoolExecutor() as executor:
            futures = []
            for index, table in enumerate(tables, start=1):
                # Submit reindexing task for each table to the thread pool
                futures.append(executor.submit(reindex_table, conn, table, host, dbname, index, total_tables, logger))

            # Wait for all threads to finish and log any exceptions if any occurred
            for future in as_completed(futures):
                future.result()  # This ensures exceptions are raised if any occurred

        return True
    except Exception as e:
        return False
    finally:
        conn.close()
        set_server_status(host, True)  # Mark server as available again


async def reindex_servers_parallel(server_list):
    """
    Process a list of servers, reindexing two servers at a time.
    If a server is busy, it is re-queued.
    """
    server_queue = asyncio.Queue()
    for server in server_list:
        await server_queue.put(server)
        main_logger.info(json.dumps({
            "event": "ServerQueued",
            "host": server['host'],
            "dbname": server['dbname']
        }))

    semaphore = asyncio.Semaphore(2)  # Limit to 2 concurrent server tasks

    # Define a ThreadPoolExecutor for running synchronous functions concurrently
    with ThreadPoolExecutor() as executor:

        async def worker():
            while not server_queue.empty():
                server_config = await server_queue.get()
                host = server_config['host']
                dbname = server_config['dbname']

                # Acquire the semaphore to ensure only 2 servers run at a time
                async with semaphore:
                    loop = asyncio.get_running_loop()
                    # Process the server using the thread pool
                    success = await loop.run_in_executor(executor, process_server, server_config)

                    if not success:
                        await server_queue.put(server_config)  # Re-queue the server if it was busy

                server_queue.task_done()

        # Start the worker tasks
        num_workers = 2
        main_logger.info(json.dumps({
            "event": "WorkerTasksStarting",
            "workers": num_workers
        }))

        tasks = [asyncio.create_task(worker()) for _ in range(num_workers)]
        await server_queue.join()

        # Cancel tasks once queue is empty and all tasks are completed
        for task in tasks:
            task.cancel()

    main_logger.info(json.dumps({
        "event": "AllServersProcessed"
    }))


# Example server configuration
servers = [
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

# Start reindexing
asyncio.run(reindex_servers_parallel(servers))
