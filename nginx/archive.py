import os

from data_storage import ResourceRepository,AzureBlobStorage
from data_storage.exceptions import ResourceAlreadyExist

from . import settings
import files

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
            archive=False
        )
    return _resource_repository

def need_archive(path):
    file_folder,file_name = os.path.split(path)
    if file_name[0] == ".":
        return False
    elif file_name.endswith(".edit"):
        return False

    return True


def archive():
    files.archive(get_resource_repository(),folder=settings.ARCHIVE_FOLDER,recursive=True,reserve_folder=settings.RESERVE_FOLDER,archive=False,file_filter=need_archive)
