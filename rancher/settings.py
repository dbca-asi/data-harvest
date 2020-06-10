from common_settings import *

AZURE_CONNECTION_STRING = env("RANCHER_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("RANCHER_CONTAINER",vtype=str,required=True)
RESOURCE_NAME = env("RANCHER_RESOURCE_NAME",vtype=str,required=True)
ARCHIVE_FOLDER = env("RANCHER_ARCHIVE_FOLDER",vtype=str,required=True)
RESERVE_FOLDER = env("RANCHER_RESERVE_FOLDER",default=True)
RANCHER_CLUSTER = env("RANCHER_CLUSTER",vtype=str,required=True)
