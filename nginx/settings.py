from common_settings import *

AZURE_CONNECTION_STRING = env("NGINX_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("NGINX_CONTAINER",vtype=str,required=True)
RESOURCE_NAME = env("NGINX_RESOURCE_NAME",vtype=str,required=True)

ARCHIVE_FOLDER = env("NGINX_ARCHIVE_FOLDER",vtype=str,required=True)
RESERVE_FOLDER = env("NGINX_RESERVE_FOLDER",default=True)
