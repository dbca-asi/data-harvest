import re
import os
from datetime import datetime, timedelta
from utils.env import Folder

from common_settings import *

LOCAL_STORAGE_DIR = env("PODSTATUS_STORAGE_DIR",vtype=Folder,required=True)
ARCHIVE_LIFESPAN = env("PODSTATUS_ARCHIVE_LIFESPAN",vtype=int) #in months
#The following are comman settings which must be set by all azlog related harvester
RESOURCE_NAME = env("PODSTATUS_RESOURCE_NAME",vtype=str,default="containerstatus")
WORKSPACE = env("PODSTATUS_AZLOG_WORKSPACE",vtype=str,required=True)
QUERY = env("PODSTATUS_AZLOG_QUERY",vtype=str,required=True)
QUERY_DURATION = env("PODSTATUS_QUERY_DURATION",vtype=timedelta,required=True)# configure in seconds
LOG_DELAY_TIME = env("PODSTATUS_DELAY_TIME",vtype=timedelta,default=timedelta(seconds=300))# configure in seconds
MAX_ARCHIVE_TIME_PER_LOG = env("PODSTATUS_MAX_ARCHIVE_TIME_PER_LOG",vtype=int,default=1500)# configure in seconds
QUERY_START = env("PODSTATUS_QUERY_START",vtype=datetime,required=True)# configure in 'yyyy/mm/dd HH:mm:ss' in local time
USER =  env("PODSTATUS_AZLOG_USER",vtype=str,required=True)
PASSWORD =  env("PODSTATUS_AZLOG_PASSWORD",vtype=str,required=True)
TENANT =  env("PODSTATUS_AZLOG_TENANT",vtype=str)
MAX_ARCHIVE_TIMES_PER_RUN = env("PODSTATUS_MAX_ARCHIVE_TIMES_PER_RUN",vtype=int)
