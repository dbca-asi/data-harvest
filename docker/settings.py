import os

from common_settings import *

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

AZURE_CONNECTION_STRING = env("DOCKER_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("DOCKER_CONTAINER",vtype=str,required=True)
DOCKER_RESOURCE_NAME = env("DOCKER_RESOURCE_NAME",vtype=str,required=True)


