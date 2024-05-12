import logging
import os

def init_log():
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=log_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
