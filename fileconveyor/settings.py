__author__ = "Wim Leers (work@wimleers.com)"
__version__ = "$Rev$"
__date__ = "$Date$"
__license__ = "GPL"


import logging

# SQLite Database
DB_SOURCE = 'sqlite'
PERSISTENT_DATA_DB = './persistent_data.db'
SYNCED_FILES_DB = './synced_files.db'
FSMONITOR_DATA_DB = './fsmonitor.db'

# MySQL Database
# DB_SOURCE = 'mysql'
# DB_HOST = 'localhost'
# DB_PORT = 3306
# DB_USERNAME = 'user'
# DB_PASSWORD = 'pass'
# DB_DATABASE = 'fileconveyor'
# DB_PREFIX = ''
# PERSISTENT_DATA_PREFIX = 'a_' # This should be different for each machine
# FSMONITOR_PREFIX = 'a_' # This should be different for each machine

RESTART_AFTER_UNHANDLED_EXCEPTION = True
RESTART_INTERVAL = 10
LOG_FILE = './fileconveyor.log'
PID_FILE = '~/.fileconveyor.pid'
WORKING_DIR = '/tmp/fileconveyor'
MAX_FILES_IN_PIPELINE = 50
MAX_SIMULTANEOUS_PROCESSORCHAINS = 1
MAX_SIMULTANEOUS_TRANSPORTERS = 10
MAX_TRANSPORTER_QUEUE_SIZE = 1
QUEUE_PROCESS_BATCH_SIZE = 20
CALLBACKS_CONSOLE_OUTPUT = False
CONSOLE_LOGGER_LEVEL = logging.WARNING
FILE_LOGGER_LEVEL = logging.INFO
RETRY_INTERVAL = 30
