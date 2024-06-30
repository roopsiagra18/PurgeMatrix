import logging.config
from os import path
from src.essentials import get_logger
# log_file_path = path.join(path.dirname(path.abspath(__file__)), '../','config/logging.config')
# logging.config.fileConfig(log_file_path, disable_existing_loggers=True)
# log = logging.getLogger("PurgeMatrixLogger")
log = get_logger()

def calculate_dynamic_thresholds(server_config):
    log.info("Calculating Dynamic Thresholds for Optimal System Metrics.")
    max_connections = server_config['max_connections']
    innodb_buffer_pool_size = server_config['innodb_buffer_pool_size'] // (1024 * 1024)  # in MB
    thresholds = {
        'threads_running': max(50, int(max_connections * 0.2)),
        'threads_connected': max(200, int(max_connections * 0.8)),
        'slow_queries': 100,  # This can be dynamic based on query analysis
        'locked_threads': 10  # This can also be dynamic based on past lock contention
    }
    return thresholds

def check_high_load(db_ops, database):
    '''
    This function is returning a boolean value, specifically the result of the expression `any(high_load_conditions)`.

    The `any()` function returns `True` if at least one element in the iterable `high_load_conditions` is `True`, and `False` otherwise. In this case, `high_load_conditions` is a list of boolean values representing different high load conditions. If any of these conditions are `True`, indicating a high load, then the function will return `True`. Otherwise, if all conditions are `False`, indicating no high load, the function will return `False`.
    Basically if this returns True, server pe high load h and False aaya toh load nai h !
    '''
    with db_ops as ops:
        # Get key status variables
        threads_running = ops.get_status_variable('Threads_running')
        threads_connected = ops.get_status_variable('Threads_connected')
        slow_queries = ops.get_status_variable('Slow_queries')
        queries = ops.get_status_variable('Queries')
        # Get process list and table sizes
        processlist = ops.get_processlist()
        locked_threads = len([p for p in processlist if p[4] == 'Locked'])
        table_sizes = ops.get_table_sizes(database)
        # Get performance schema events and server configuration
        performance_events = ops.get_performance_schema_events()
        server_config = ops.get_server_configuration()
        # Calculate dynamic thresholds
        thresholds = calculate_dynamic_thresholds(server_config)
        # Determine high load conditions
        high_load_conditions = [
            threads_running > thresholds['threads_running'],
            threads_connected > thresholds['threads_connected'],
            slow_queries > thresholds['slow_queries'],
            locked_threads > thresholds['locked_threads'],
        ]
        # # Output details
        # print(f"Threads Running: {threads_running}")
        # print(f"Threads Connected: {threads_connected}")
        # print(f"Slow Queries: {slow_queries}")
        # print(f"Total Queries: {queries}")
        # print(f"Locked Threads: {locked_threads}")
        # print(f"Table Sizes: {table_sizes[:5]}")  # Show top 5 largest tables
        # print(f"Performance Schema Events: {performance_events}")
        # print(f"Thresholds: {thresholds}")
        if any(high_load_conditions):
            log.warning("The server is under High Load.")
        else:
            log.info("The Server Load is Normal according to the thresholds set.")
        return any(high_load_conditions)