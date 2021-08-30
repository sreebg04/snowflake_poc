# Logging including the timestamp, thread and the source code location
import logging
# import snowflake_main

logger = logging.getLogger("snowflake_main")
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('log.txt')
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
logger.addHandler(ch)
