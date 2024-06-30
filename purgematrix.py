from src.database_operations import DatabaseOperations
from src.safe_time_checker import wait_for_safe_time, is_maintenance_time
from src.database_load_checker import check_high_load
from src.essentials import load_config, prprint
import logging
from logging import config
import os
from os import path
import warnings
from urllib3.exceptions import InsecureRequestWarning
from src.get_optimal_batch import *
import sys

log = get_logger()

def main():
    db_ops = DatabaseOperations()
    table_name = secrets['DATABASE']['TABLE_NAME'][1]
    minimum_batch_size = secrets['BATCH_SIZE_VALUES']['MIN_BATCH_SIZE']
    maximum_batch_size = secrets['BATCH_SIZE_VALUES']['MAX_BATCH_SIZE']
    step_size = secrets['BATCH_SIZE_VALUES']['STEP_SIZE']
    max_time_per_batch = secrets['BATCH_SIZE_VALUES']['MAX_TIME_PER_BATCH']
    log.info("Checking if you're under Maintenance Window or Not.")
    prprint("Checking if you're under Maintenance Window or Not.")
    is_maintenance_on = is_maintenance_time()
    if is_maintenance_on:
        log.info("\t - Under Maintenance Window. Moving on.")
        print("\t - Under Maintenance Window. Moving on.")
        prprint("Checking if the server is under High Load.")
        log.info("Checking if the server is under High Load.")
        is_high_load = check_high_load(db_ops, db_ops.database_name)
        if is_high_load:
            print("\t - The server is under High Load. Hence, Cannot move forward with the Purging.")
            sys.exit(1)
        else:
            print("\t - The Server Load is Normal according to the thresholds set. Hence, Moving on.")
            log.info("Checking if this time is Safe Time for Data Purging now.")
            prprint("Checking if this time is Safe Time for Data Purging now.")
            safe_time_check_interval = secrets['SAFE_TIME_VALUES']['CHECK_INTERVAL']
            is_it_safe_time = wait_for_safe_time(db_ops, safe_time_check_interval)
            if is_it_safe_time:
                print("\t - Safe Time Detected for Data Purging. Moving on.")
                log.info("Trying to get the most Optimal Batch size for Data Purging now.")
                prprint("Trying to get the most Optimal Batch size for Data Purging now.")
                optimal_batch_size = find_optimal_batch_size(db_ops, table_name, minimum_batch_size,maximum_batch_size,step_size,max_time_per_batch)
                print(f"\t - Optimal Batch Size Found - {optimal_batch_size}")
            else:
                log.warning("\t - Failed to detect the Safe Time for Data Purging. Try again later !")
                print("\t - Failed to detect the Safe Time for Data Purging. Try again later !")
                sys.exit(1)
    else:
        log.warning("\t - NOT under Maintenance Window. Exiting PurgeMatrix.")
        print("\t - NOT under Maintenance Window. Exiting PurgeMatrix.")
        sys.exit(1)

    # Proceed  with data deletion
    # Implement your data deletion logic here

if __name__ == '__main__':
    main()
