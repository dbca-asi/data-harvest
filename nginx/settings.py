from common_settings import *
from db.database import PostgreSQL

AZURE_CONNECTION_STRING = env("NGINX_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("NGINX_CONTAINER",vtype=str,required=True)
NGINX_RESOURCE_NAME = env("NGINX_RESOURCE_NAME",vtype=str,required=True)

NGINX_ARCHIVE_FOLDER = env("NGINX_ARCHIVE_FOLDER",vtype=str,required=True)
RESERVE_FOLDER = env("RESERVE_FOLDER",default=True)
