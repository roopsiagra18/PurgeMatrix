from src.essentials import load_config
import mysql.connector
from mysql.connector import Error
from faker import Faker
import logging
import psutil
import time
import re
from os import path
import logging.config

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../','config/logging.config')
logging.config.fileConfig(log_file_path, disable_existing_loggers=True)
log = logging.getLogger("PurgeMatrixLogger")

class DatabaseOperations:
    def __init__(self):
        self.config = load_config()
        self.host = self.config['DATABASE']['HOST']
        self.username = self.config['DATABASE']['USERNAME']
        self.password = self.config['DATABASE']['PASSWORD']
        self.db_port = self.config['DATABASE']['PORT']
        self.database_name = self.config['DATABASE']['NAME']
        self.table_name = self.config['DATABASE']['TABLE_NAME'][1]
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = self.create_mysql_session()
        return self

    def __exit__(self, jhgc, jhfg, jhfhj):
        pass
        #self.close_mysql_session

    def create_mysql_session(self):
        try:
            # log.info("Trying to create MYSQL database connection")
            connection = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.db_port,
                database=self.database_name
            )
            if connection.is_connected():
                #log.info("Connection to MySQL server successful")
                return connection
        except Error as e:
            log.error(f"Error while connecting to MySQL: {e}")

    def close_mysql_session(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            log.info("MySQL connection closed")

    def create_cursor(self):
        try:
            cursor = self.connection.cursor()
            #log.info("Created a cursor successfully")
            return cursor
        except Error as e:
            if self.cursor is None:
                log.error(f"Error creating cursor {cursor}: {e}")

    def monitor_lock_contention(self,db_ops):
        log.info(f"Trying to fetch row locks and struct locks for {db_ops.database_name} to calculate optimal batch size.")
        cursor = self.connection.cursor()
        cursor.execute(f""" SHOW ENGINE INNODB STATUS""")
        result = cursor.fetchone()
        if result:
            status_output = result[2]
            for line in status_output.split('\n'):
                if "lock" in line or "locks" in line:
                    lock_structs_match = re.search(r'(\d+) lock struct\(s\)',line)
                    row_locks_match = re.search(r'(\d+) row lock\(s\)', line)
                    if lock_structs_match and row_locks_match:
                        lock_structs_count = int(lock_structs_match.group(1))
                        row_locks_count = int(row_locks_match.group(1))
                        if lock_structs_count > 0 or row_locks_count > 0:
                            print("oh Nooo !!!Locks are there.....")
                            if lock_structs_count > 0:
                                log.warning(f"{lock_structs_count} lock structs ")
                            elif row_locks_count > 0:
                                log.warning(f"{row_locks_count} row locks ")
                            else:
                                log.warning(f" There are{lock_structs_count} lock structs and {row_locks_count} row locks ")
                            return True
                        else:
                            log.info(f"{lock_structs_count} lock structs and {row_locks_count} row locks found")
        else:
            log.warning(f" OOPS!!!!!! Not able to fetch locks data ")
            print("No status Info Found")
            return False

    def get_status_variable(self, variable_name):
        try:
            cursor = self.connection.cursor()
            query = f"SHOW STATUS LIKE '{variable_name}'"
            cursor.execute(query)
            result = cursor.fetchone()
            return int(result[1]) if result else None
        except Error as e:
            log.error(f"Error fetching status variable {variable_name}: {e}")

    def get_processlist(self):
        try:
            cursor = self.connection.cursor()
            query = "SHOW PROCESSLIST"
            cursor.execute(query)
            return cursor.fetchall()
        except Error as e:
            log.error(f"Error fetching process list: {e}")

    def get_table_sizes(self, database):
        try:
            #log.info(f"Trying to fetch table sizes in database {database}")
            cursor = self.connection.cursor()
            query = f"""
            SELECT table_schema,
                   table_name,
                   engine,
                   table_rows,
                   ROUND(data_length / (1024 * 1024), 2) as data_length_mb,
                   ROUND(index_length / (1024 * 1024), 2) as index_length_mb
            FROM information_schema.tables
            WHERE table_schema = '{database}'
            ORDER BY data_length + index_length DESC
            """
            cursor.execute(query)
            return cursor.fetchall()
        except Error as e:
            log.error(f"Error fetching table sizes: {e}")

    def get_performance_schema_events(self):
        try:
            cursor = self.connection.cursor()
            query = """
            SELECT event_name,
                   COUNT_STAR as count,
                   SUM_TIMER_WAIT as total_wait_time,
                   AVG_TIMER_WAIT as avg_wait_time
            FROM performance_schema.events_waits_summary_global_by_event_name
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT 10
            """
            cursor.execute(query)
            return cursor.fetchall()
        except Error as e:
            log.error(f"Error fetching performance schema events: {e}")

    def get_server_configuration(self):
        try:
            cursor = self.connection.cursor()
            server_config = {}
            variables = ['max_connections', 'innodb_buffer_pool_size', 'innodb_log_file_size', 'innodb_flush_log_at_trx_commit']
            for variable in variables:
                cursor.execute(f"SHOW VARIABLES LIKE '{variable}'")
                result = cursor.fetchone()
                if result:
                    server_config[variable] = int(result[1])
            return server_config
        except Error as e:
            log.error(f"Error fetching server configuration: {e}")

    def insert_sample_data(self, num_records=100000):
        if self.connection is None or not self.connection.is_connected():
            log.error("No connection to MySQL server. Please create a session first.")
            return
        try:
            cursor = self.connection.cursor()
            fake = Faker()
            for _ in range(num_records):
                name = fake.name()
                email = fake.email()
                age = fake.random_int(min=18, max=80)
                query = f"INSERT INTO {self.table_name} (name, email, age) VALUES (%s, %s, %s)"
                cursor.execute(query, (name, email, age))
            self.connection.commit()
            log.info(f"{num_records} sample rows inserted successfully")
        except Error as e:
            log.error(f"Error while inserting sample data: {e}")

    def list_mysql_databases(self):
        if self.connection is None or not self.connection.is_connected():
            log.error("No connection to MySQL server. Please create a session first.")
            return
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            for db in databases:
                print(db[0])
        except Error as e:
            log.error(f"Error while listing databases: {e}")

    def list_mysql_table(self, table_name, batch_size):
        if self.connection is None or not self.connection.is_connected():
            log.error("No connection to MySQL server. Please create a session first.")
        try:
            self.connection = self.create_mysql_session()
            query = f"""SELECT * FROM {table_name}"""
            cursor = self.connection.cursor()
            start_time = time.time()
            cursor.execute(query)
            rows = cursor.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            for row in rows:
                return execution_time
        except Error as e:
            log.error(f"Error while listing databases: {e}")

    def get_cpu_usage(self):
        return psutil.cpu_percent(interval=1)

    def get_active_connections(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
            result = cursor.fetchone()
            return int(result[1]) if result else None
        except Error as e:
            log.error(f"Error fetching active connections: {e}")
            return None