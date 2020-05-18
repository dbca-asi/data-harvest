from data_storage.azure_blob import AzureBlobResource
from data_storage.exceptions import ResourceAlreadyExist

from . import settings
import files

_blob_resource = None
def get_blob_resource():
    """
    Return the blob resource client
    """
    global _blob_resource
    if _blob_resource is None:
        _blob_resource = AzureBlobResource(
            settings.NGINX_RESOURCE_NAME,
            settings.AZURE_CONNECTION_STRING,
            settings.AZURE_CONTAINER,
            archive=False
        )
    return _blob_resource

def archive():
    files.archive(get_blob_resource(),folder=settings.NGINX_ARCHIVE_FOLDER,recursive=True,reserve_folder=settings.RESERVE_FOLDER,archive=False)
