import socket
import logging

from common_settings import *
from utils import parse_db_connection_string,classproperty
from db.database import PostgreSQL

import psycopg2

DATABASE = PostgreSQL(env("RESOURCE_TRACKING_DATABASE_URL",vtype=str,required=True))

AZURE_CONNECTION_STRING = env("RESOURCE_TRACKING_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("RESOURCE_TRACKING_CONTAINER",vtype=str,required=True)
LOGGEDPOINT_RESOURCE_NAME = env("LOGGEDPOINT_RESOURCE_NAME",vtype=str,required=True)
LOGGEDPOINT_ARCHIVE_DELETE_DISABLED = env("LOGGEDPOINT_ARCHIVE_DELETE_DISABLED",default=True)

LOGGEDPOINT_ACTIVE_DAYS = env("LOGGEDPOINT_ACTIVE_DAYS",vtype=int,default=30)

START_WORKING_HOUR =  env("START_WORKING_HOUR",vtype=int)
END_WORKING_HOUR =  env("END_WORKING_HOUR",vtype=int)

