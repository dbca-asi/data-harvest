import re
import os
from datetime import datetime, timedelta
from utils.env import Folder

from common_settings import *

LOCAL_STORAGE_DIR = env("CONTAINERLOG_STORAGE_DIR",vtype=Folder,required=True)
ARCHIVE_LIFESPAN = env("CONTAINERLOG_ARCHIVE_LIFESPAN",vtype=int) #in months
#The following are comman settings which must be set by all azlog related harvester
RESOURCE_NAME = env("CONTAINERLOG_RESOURCE_NAME",vtype=str,default="containerlog")
WORKSPACE = env("CONTAINERLOG_AZLOG_WORKSPACE",vtype=str,required=True)
QUERY = env("CONTAINERLOG_AZLOG_QUERY",vtype=str,required=True)
QUERY_DURATION = env("CONTAINERLOG_QUERY_DURATION",vtype=timedelta,required=True)# configure in seconds
LOG_DELAY_TIME = env("CONTAINERLOG_DELAY_TIME",vtype=timedelta,default=timedelta(seconds=300))# configure in seconds
MAX_ARCHIVE_TIME_PER_LOG = env("CONTAINERLOG_MAX_ARCHIVE_TIME_PER_LOG",vtype=int,default=1500)# configure in seconds
QUERY_START = env("CONTAINERLOG_QUERY_START",vtype=datetime,required=True)# configure in 'yyyy/mm/dd HH:mm:ss' in local time
USER =  env("CONTAINERLOG_AZLOG_USER",vtype=str,required=True)
PASSWORD =  env("CONTAINERLOG_AZLOG_PASSWORD",vtype=str,required=True)
TENANT =  env("CONTAINERLOG_AZLOG_TENANT",vtype=str)
MAX_ARCHIVE_TIMES_PER_RUN = env("CONTAINERLOG_MAX_ARCHIVE_TIMES_PER_RUN",vtype=int)
