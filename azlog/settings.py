import re
import os
from datetime import datetime, timedelta
from utils.env import Folder

from common_settings import *

LOCAL_STORAGE_DIR = env("LOCAL_STORAGE_DIR",vtype=Folder,required=True)
RESOURCE_NAME = env("RESOURCE_NAME",vtype=str,required=True)

WORKSPACE = env("AZLOG_WORKSPACE",vtype=str,required=True)
QUERY = env("AZLOG_QUERY",vtype=str,required=True)
QUERY_DURATION = env("AZLOG_QUERY_DURATION",vtype=timedelta,required=True)# configure in seconds
QUERY_START = env("AZLOG_QUERY_START",vtype=datetime,required=True)# configure in 'yyyy/mm/dd HH:mm:ss' in local time

USER =  env("AZLOG_USER",vtype=str,required=True)
PASSWORD =  env("AZLOG_PASSWORD",vtype=str,required=True)

MAX_ARCHIVE_TIMES_PER_RUN = env("MAX_ARCHIVE_TIMES_PER_RUN",vtype=int)

PROCESS_LOCKFILE = os.path.join(LOCAL_STORAGE_DIR,".azlog_{}.lock".format(RESOURCE_NAME.lower()))
