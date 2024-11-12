import asyncio
import logging
from collections import defaultdict
from queue import Queue
from typing import List, Tuple
import asyncpg

# Configure main logging
logging.basicConfig(filename='progress.log', level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('MainLogger')

# Function to setup database-specific logger
def setup_db_logger(db_name: str):
    db_logger = logging.getLogger(db_name)
    handler = logging.FileHandler(f'{db_name}_progress.log')
    formatter = logging.Formatter('%(asctime)s %(message)s')
    handler.setFormatter(formatter)
    db_logger.addHandler(handler)
    db_logger.setLevel(logging.INFO)
    return db_logger

class IndexReindexer:
    def __init__(self, server_db_list: List[Tuple[str, str]], max_servers: int, max_tables: int, db_config: dict):
        self.server_db_list = server_db_list
        self.max_servers = max_servers
        self.max_tables = max_tables
        self.db_config = db_config
        self.server_queue = Queue()
        self.in_progress_servers = set()
        self.db_loggers = {}

        # Initialize server queue with server-database pairs
        self._initialize_server_queue()

    def _initialize_server_queue(self):
        """ Initialize the server queue with server-database pairs. """
        for server, db in self.server_db_list:
            self.server_queue.put((server, db))

    async def _get_index_names(self, conn, table_name: str) -> List[str]:
        """ Retrieve index names for a specific table. """
        query = """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = $1;
        """
        indexes = await conn.fetch(query, table_name)
        return [index['indexname'] for index in indexes]

    async def _reindex_index(self, conn, db_logger, index_name: str):
        """ Reindex an index concurrently. """
        db_logger.info(f"Started reindexing index {index_name} CONCURRENTLY")
        try:
            await conn.execute(f"REINDEX INDEX CONCURRENTLY {index_name};")
            db_logger.info(f"Finished reindexing index {index_name} CONCURRENTLY")
        except Exception as e:
            db_logger.error(f"Error reindexing index {index_name} CONCURRENTLY: {e}")

    async def _reindex_table(self, conn, db_logger, table_name: str):
        """ Reindex all indexes for a table concurrently. """
        db_logger.info(f"Started reindexing indexes in table {table_name}")
        index_names = await self._get_index_names(conn, table_name)

        # Reindex all indexes concurrently
        tasks = [self._reindex_index(conn, db_logger, index_name) for index_name in index_names]
        await asyncio.gather(*tasks)
        db_logger.info(f"Finished reindexing all indexes in table {table_name}")

    async def _reindex_database(self, server: str, db_name: str):
        """ Reindex all tables in the database concurrently. """
        db_logger = self.db_loggers[db_name]
        db_logger.info(f"Started reindexing database {db_name} on server {server}")

        # Establish connection to the PostgreSQL database
        db_config = self.db_config.get(server, {})
        conn = await asyncpg.connect(**db_config, database=db_name)

        try:
            # Fetch table names from the database
            tables = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            table_names = [table['table_name'] for table in tables]

            # Limit to max_tables and reindex each table concurrently
            tasks = [self._reindex_table(conn, db_logger, table_name) for table_name in table_names[:self.max_tables]]
            await asyncio.gather(*tasks)

            db_logger.info(f"Finished reindexing database {db_name} on server {server}")

        finally:
            await conn.close()

    async def _process_database(self, server: str, db_name: str):
        """ Process the reindexing for a single database. """
        db_logger = self.db_loggers.get(db_name) or setup_db_logger(db_name)
        self.db_loggers[db_name] = db_logger

        # Run the reindexing for the database
        await self._reindex_database(server, db_name)

    async def _process_server_queue(self):
        """ Process databases from the server queue. """
        while not self.server_queue.empty():
            server, db_name = self.server_queue.get()

            if server in self.in_progress_servers:
                logger.info(f"Server {server} is busy, re-queuing database {db_name}")
                self.server_queue.put((server, db_name))
                await asyncio.sleep(1)  # Avoid tight loop
            else:
                self.in_progress_servers.add(server)
                logger.info(f"Processing database {db_name} on server {server}")

                # Process the database
                await self._process_database(server, db_name)

                # After processing, mark server as not in progress
                self.in_progress_servers.remove(server)
                logger.info(f"Finished processing database {db_name} on server {server}")

    async def start_processing(self):
        """ Start processing the queue with max_servers concurrently. """
        tasks = [self._process_server_queue() for _ in range(self.max_servers)]
        await asyncio.gather(*tasks)

# Main function to initialize and run the reindexer
async def main():
    # Sample list of (server, database) pairs
    server_db_list = [
        ('server1', 'db1'), ('server1', 'db2'),
        ('server2', 'db3'), ('server2', 'db4'),
        ('server3', 'db5'), ('server3', 'db6')
    ]
    
    # PostgreSQL database connection configurations
    db_config = {
        'server1': {
            'user': 'your_user',
            'password': 'your_password',
            'host': 'your_host',
            'port': 5432
        },
        'server2': {
            'user': 'your_user',
            'password': 'your_password',
            'host': 'your_host',
            'port': 5432
        },
        'server3': {
            'user': 'your_user',
            'password': 'your_password',
            'host': 'your_host',
            'port': 5432
        }
    }
    
    reindexer = IndexReindexer(server_db_list, max_servers=2, max_tables=2, db_config=db_config)
    await reindexer.start_processing()

# Run the main function
asyncio.run(main())
