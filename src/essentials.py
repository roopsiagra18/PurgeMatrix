import yaml
import os
import datetime
import logging.config
from os import path

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
    with open(config_path) as file:
        return yaml.safe_load(file)

def prprint(any_string):
    now = datetime.datetime.now()
    format = "%a %b %d %H:%M:%S %Z %Y"
    formatted_datetime = now.strftime(format)
    print(f"[ {formatted_datetime} ] - {any_string}")

def get_logger():
    log_file_path = path.join(path.dirname(path.abspath(__file__)), '../','config/logging.config')
    logging.config.fileConfig(log_file_path, disable_existing_loggers=True)
    log = logging.getLogger("PurgeMatrixLogger")
    return log