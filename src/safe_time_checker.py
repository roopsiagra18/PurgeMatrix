import time
from datetime import datetime, timedelta
from src.essentials import load_config, get_logger

log = get_logger()

def is_safe_to_delete(db_ops, cpu_threshold=30, connections_threshold=50):
    with db_ops as ops:
        log.info("Trying to fetch metrices in order to check safe time")
        cpu_usage = ops.get_cpu_usage()
        active_connections = ops.get_active_connections()
        if cpu_usage is None or active_connections is None:
            log.warning("OOopss!!! Not able to fetch cpu metrices.")
            return False
        # print(f"CPU Usage: {cpu_usage}%")
        # print(f"Active Connections: {active_connections}")
        return cpu_usage < cpu_threshold and active_connections < connections_threshold

def is_maintenance_time():
    log.info("Trying to check maintenance window")
    config = load_config()
    current_time = datetime.utcnow()
    maintenance_start_date = datetime.strptime(str(config['MAINTENANCE_WINDOW']['MW_START_DATE']), '%d-%m-%Y').date()
    maintenance_start_time = datetime.strptime(str(config['MAINTENANCE_WINDOW']['MW_START_TIME']), '%H:%M:%S').time()
    maintenance_duration = config['MAINTENANCE_WINDOW']['MW_DURATION']
    maintenance_start_datetime = datetime.combine(maintenance_start_date, maintenance_start_time)
    maintenance_end_time = maintenance_start_datetime + timedelta(seconds=maintenance_duration)
    return maintenance_start_datetime <= current_time < maintenance_end_time

def wait_for_safe_time(db_ops, check_interval=60):
    config = load_config()
    cpu_threshold = config['THRESHOLDS']['CPU_THRESHOLD']
    connections_threshold = config['THRESHOLDS']['CONNECTIONS_THRESHOLD']
    while True:
        log.info("Trying to check safe time now.")
        if is_safe_to_delete(db_ops, cpu_threshold, connections_threshold):
            log.info("Yeah!!!, It's safe time for purging....")
            return True
        else:
            log.warning("Ooops!!!, Current load is high. Waiting for the next check...")
            print("Current load is high. Waiting for the next check...")
            time.sleep(check_interval)