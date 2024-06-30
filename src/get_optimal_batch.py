import psutil
from src.database_operations import *
from src.essentials import load_config, get_logger
secrets = load_config()

log = get_logger()

def get_system_metrics(db_ops):
    log.info(f"Trying to fetch system metrices for {db_ops.database_name} to calculate optimal batch size.")
    cpu_usage = psutil.cpu_percent(interval=None)
    memory_usage = psutil.virtual_memory().percent
    io_counters = psutil.disk_io_counters()
    io_load = ((io_counters.read_bytes + io_counters.write_bytes)// (1024 * 1024 * 1024))/ 100
    log.info(f"Fetched system metrices for {db_ops.database_name} => Memory Usage {memory_usage}%, CPU Usage {cpu_usage}%, IO load {io_load}%")
    return cpu_usage, memory_usage, io_load

def get_execution_time(db_ops, batch_size):
    with db_ops as ops:
        log.info(f"Trying to fetch table sizes in database {ops.database_name}")
        max_table_size_record = ops.get_table_sizes(ops.database_name)
    for table_record in max_table_size_record:
        max_table_name = table_record[1]
        log.info(f"{max_table_name} is the table with maximum records.")
        log.info(f"Trying to fetch execution time for {max_table_name} to calculate optimal batch size now.")
        execution_time = ops.list_mysql_table(max_table_name, batch_size)
        log.info(f"Fetched {execution_time} as execution time for {max_table_name} to calculate optimal batch size.")
        break
    return execution_time

def find_optimal_batch_size(db_ops, table_name, min_batch_size, max_batch_size,step_size, max_time_per_batch):
    execution_time = 0
    optimal_batch_size = min_batch_size
    min_execution_time = float('inf')
    log.info(f"Trying to fetch Optimal batch size for {table_name} to purge data safely.")
    for batch_size in range(min_batch_size,max_batch_size+1,step_size):
        log.info(f"Trying to fetch Optimal batch size for {table_name} with batch size as {batch_size}")
        cpu_usage, mem_usage, io_load = get_system_metrics(db_ops)
        execution_time = get_execution_time(db_ops, batch_size)
        lock_contention = (db_ops.monitor_lock_contention(db_ops))
        if execution_time <= max_time_per_batch and cpu_usage < secrets['THRESHOLDS']['CPU_THRESHOLD'] and mem_usage < secrets['THRESHOLDS']['MEMORY_THRESHOLD'] and io_load < secrets['THRESHOLDS']['IO_THRESHOLD'] and not lock_contention:
            if execution_time < min_execution_time:
                optimal_batch_size = batch_size
                min_execution_time = execution_time
    log.info(f"Fetched Optimal batch size for {table_name} as {optimal_batch_size}")
    return optimal_batch_size