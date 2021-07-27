import os
from datetime import timedelta

from data_storage import ResourceRepository,AzureBlobStorage,ResourceConstant,LockSession
from data_storage.exceptions import ResourceAlreadyExist

from . import settings
import files
from utils import timezone

_resource_repository = None
def get_resource_repository(reuse=True):
    """
    Return the blob resource client
    """
    global _resource_repository
    if _resource_repository is None or not reuse:
        _resource_repository = ResourceRepository(
            AzureBlobStorage(settings.AZURE_CONNECTION_STRING,settings.AZURE_CONTAINER),
            settings.RESOURCE_NAME,
            archive=False,
            resource_base_path="{}/{}".format(settings.RESOURCE_NAME,settings.RANCHER_CLUSTER),
            logical_delete=True
        )
    return _resource_repository

def need_archive(path):
    file_folder,file_name = os.path.split(path)
    if file_name[0] == ".":
        return False
    else:
        return settings.FILE_RE.search(file_name)

DELETED_RESROURCE_EXPIRED = timedelta(days=28)
def archive():
    with LockSession(get_resource_repository(),3600,3000) as lock_session:
        #archive the latest files
        files.archive(get_resource_repository(),folder=settings.ARCHIVE_FOLDER,recursive=True,reserve_folder=settings.RESERVE_FOLDER,archive=False,file_filter=need_archive)
        #clean expired deleted resources from storage
        files.clean_expired_deleted_resources(get_resource_repository(),DELETED_RESROURCE_EXPIRED)

def need_delete(meta):
    resourceid = meta["resource_id"]
    file_folder,file_name = os.path.split(resourceid)
    if file_name[0] == ".":
        return True
    else:
        if not settings.FILE_RE.search(file_name):
            return True

    if meta.get(ResourceConstant.DELETED_KEY,False) and meta.get(ResourceConstant.DELETE_TIME_KEY) and timezone.now() > meta.get(ResourceConstant.DELETE_TIME_KEY) + DELETED_RESROURCE_EXPIRED:
        return True

    return False

def clean_resources(batch=40000):
    files.clean_resources(get_resource_repository(),need_delete,batch=batch)
