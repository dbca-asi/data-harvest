import json
import tempfile
import logging
import os
import shutil
import traceback

from azure.storage.blob import  BlobServiceClient,BlobClient,BlobType
from azure.core.exceptions import (ResourceNotFoundError,)

from .storage import ResourceStorage
from . import settings
from utils import JSONEncoder,JSONDecoder,timezone

logger = logging.getLogger(__name__)

class AzureBlob(object):
    """
    A blob client to get/update a blob resource
    """
    def __init__(self,blob_path,connection_string,container_name):
        self._blob_path = blob_path
        self._blob_client = BlobClient.from_connection_string(connection_string,container_name,blob_path,**settings.AZURE_BLOG_CLIENT_KWARGS)

    def delete(self):
        try:
            self._blob_client.delete_blob()
        except:
            logger.error("Failed to delete the resource from blob storage.{}".format(self._blob_path,traceback.format_exc()))


    def download(self,filename=None,overwrite=False):
        """
        Return the downloaded local resource file
        """
        if filename:
            if os.path.exists(filename):
                if not os.path.isfile(filename):
                    #is a folder
                    raise Exception("The path({}) is not a file.".format(filename))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(filename))
        else:
            with tempfile.NamedTemporaryFile(prefix=self.resourcename) as f:
                filename = f.name

        with open(filename,'wb') as f:
            blob_data = self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return filename
        

    def update(self,blob_data):
        """
        Update the blob data
        """
        if blob_data is None:
            #delete the blob resource
            self._blob_client.delete_blob(delete_snapshots="include")
        else:
            if not isinstance(blob_data,bytes):
                #blob_data is not byte array, convert it to json string
                raise Exception("Updated data must be bytes type.")
            #self._blob_client.stage_block("main",blob_data)
            #self._blob_client.commit_block_list(["main"])
            self._blob_client.upload_blob(blob_data,overwrite=True,timeout=3600)

class AzureJsonBlob(AzureBlob):
    """
    A blob client to get/update a json blob resource
    """
    @property
    def json(self):
        """
        Return resource data as dict object.
        Return None if resource is not found
        """
        try:
            data = self._blob_client.download_blob().readall()
            return json.loads(data.decode(),cls=JSONDecoder)
        except ResourceNotFoundError as e:
            #blob not found
            return None

    def update(self,blob_data):
        """
        Update the blob data
        """
        blob_data = {} if blob_data is None else blob_data
        if not isinstance(blob_data,bytes):
            #blob_data is not byte array, convert it to json string
            blob_data = json.dumps(blob_data,cls=JSONEncoder).encode()
        super().update(blob_data)

class AzureBlobResourceMetadata(AzureJsonBlob):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,metaname="metadata"):
        filename = "{}.json".format(metaname or "metadata") 
        if resource_base_path:
            metadata_file = "{}/{}".format(resource_base_path,filename)
        else:
            metadata_file = filename
        logger.debug("container={}, metadata file={}".format(container_name,metadata_file))
        super().__init__(metadata_file,connection_string,container_name)
        self._cache = cache

    @property
    def json(self):
        """
        Return the resource's meta data as dict object.
        Return None if resource's metadata is not found
        """
        if self._cache and hasattr(self,"_json"):
            #json data is already cached
            return self._json

        json_data = super().json

        if self._cache and json_data is not None:
            #cache the json data
            self._json = json_data

        return json_data

    def update(self,metadata):
        if metadata is None:
            metadata = {}
        super().update(metadata)
        if self._cache:
            #cache the result
            self._json = metadata

    def delete(self):
        super().delete()
        if self._cache:
            self._json = {}

class AzureBlobResourceClient(AzureBlobResourceMetadata):
    """
    A client to track the non group resource consuming status of a client
    """
    def __init__(self,connection_string,container_name,clientid,resource_base_path=None,cache=False):
        metadata_filename = ".json".format(clientid)
        if resource_base_path:
            client_base_path = "{}/clients".format(resource_base_path)
        else:
            client_base_path = "clients"
        super().__init__(metadata_file,connection_string,container_name,resource_base_path=client_base_path,metadata_filename=metadata_filename,cache=cache)
        self._metadata_client = AzureBlobResourceMetadata(connection_string,container_name,resource_base_path=resource_base_path,cache=False)


    @property
    def status(self):
        """
        Return tuple(True if the latest resource was consumed else False,(latest_resource_id,latest_resource's publish_date),(consumed_resurce_id,consumed_resource's published_date,consumed_date))
        """
        client_metadata = self.json
        resource_metadata = self._metadata_client.json
        if not client_metadata or not client_metadata.get("resource_id"):
            #this client doesn't consume the resource before
            if not resource_metadata or not resource_metadata.get("current",{}).get("resource_id"):
                #not resource was published
                return (True,None,None)
            else:
                #some resource hase been published
                return (False,(resource_metadata.get("current",{}).get("resource_id"),resource_metadata.get("current",{}).get("publish_date")),None)
        elif not resource_metadata or not resource_metadata.get("current",{}).get("resource_id"):
            #no resource was published
            return (True,None,(client_metadata.get("resource_id"),client_metadata.get("publish_date"),client_metadata.get("consume_date")))
        elif client_metadata.get("resource_id") == resource_metadata.get("current",{}).get("resource_id"):
            #the client has consumed the latest resource
            return (
                True,
                (resource_metadata.get("current",{}).get("resource_id"),resource_metadata.get("current",{}).get("publish_date")),
                (client_metadata.get("resource_id"),client_metadata.get("publish_date"),client_metadata.get("consume_date"))
            )
        else:
            return (
                False,
                (resource_metadata.get("current",{}).get("resource_id"),resource_metadata.get("current",{}).get("publish_date")),
                (client_metadata.get("resource_id"),client_metadata.get("publish_date"),client_metadata.get("consume_date"))
            )

    @property
    def isbehind(self):
        """
        Return true if consumed resurce is not the latest resource; otherwise return False
        """
        return not self.status[0]

    def consume(self,callback,isjson=True):
        """
        Return True if some resource has been consumed; otherwise return False
        """
        status = self.status
        if status[0]:
            #the latest resource has been consumed
            return False

        resource_client = AzureBlob(status[1][0],connection_string,container_name)
        if isjson:
            callback(resource_client.json)
        else:
            res_file = resource_client.download()
            try:
                with open(res_file,'rb') as f:
                    callback(f)
            finally:
                #after processing,remove the downloaded local resource file
                os.remove(res_file)
        #update the client consume data
        client_metdata = {
            "resource_id" : status[1][0],
            "publish_date" : status[1][1],
            "consume_date": timezone.now()
        }

        self.update(client_metadata)

        return True


class AzureBlobResourceBase(object):
    """
    A base client to manage a Azure Resourcet
    """
    _f_resourceid = staticmethod(lambda resource_name:resource_name)
    _f_resource_file = staticmethod(lambda resourceid:"{0}_{1}.json".format(resourceid,timezone.now().strftime("%Y-%m-%d-%H-%M-%S")))
    _f_resource_path = staticmethod(lambda data_path,resource_group,resource_file:"{0}/{1}/{2}".format(data_path,resource_group,resource_file) if resource_group else "{0}/{1}".format(data_path,resource_file))
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,group_resource=False,archive=True,metaname=None,f_resourceid=None,f_resource_file=None):
        self._resource_name = resource_name
        self._resource_base_path = resource_name if resource_base_path is None else resource_base_path
        if self._resource_base_path:
            self._resource_data_path = "{}/data".format(self._resource_base_path)
        else:
            self._resource_data_path = "data"
        self._connection_string = connection_string
        self._container_name = container_name
        self._metadata_client = AzureBlobResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,metaname=metaname,cache=True)
        self._archive = archive
        self.group_resource = group_resource
        if not f_resourceid:
            self._f_resourceid = staticmethod(f_resourceid)
        if not f_resource_file:
            self._f_resource_file = staticmethod(f_resource_file)

    def get_blob_client(self,blob_name):
        return BlobClient.from_connection_string(self._connection_string,self._container_name,blob_name,**settings.AZURE_BLOG_CLIENT_KWARGS)

    @property
    def resourcename(self):
        return self._resource_name

    @property
    def resourcemetadata(self):
        """
        Return resource metadata.
        Return None if resource metadata is not found
        """
        return self._metadata_client.json

    def get_metadata(self,resourceid=None,resource_group=None,resource_file="current",throw_exception=False):
        """
        if resource_file is 'current', it means the latest archive of the specific resource
        Return 
            if resourceid is none and (resource_group is none or resource is not a group resource), return all resource metadata
            if resourceid is none and resource_group is not none annd resource is a group resource, return group metadata
            if resource_file is not none, and resource is archive resource, return the specific resource archivement's metadata, if exists, otherwise return None or throw exception
            if archived is none or resource is non archive resource, return the specific resource's metadata, if exists, otherwise return None or throw exception

        """
        if not resourceid and (not resource_group or not self.group_resource):
            return self.resourcemetadata

        resourcemetadata = self.resourcemetadata or {}
        if self.group_resource:
            if resource_group:
                groupmetadata = resourcemetadata.get(resource_group)
                if not groupmetadata:
                    if throw_exception:
                        raise ResourceNotFoundError("The resource group({}.{}) Not Found".format(self.resourcename,resource_group))
                    else:
                        return None
                if not resourceid:
                    return groupmetadata
                elif resourceid in groupmetadata:
                    metadata =  groupmetadata[resourceid]
                elif throw_exception:
                    raise ResourceNotFoundError("The resource({}.{}.{}) Not Found".format(self.resourcename,resource_group,resourceid))
                else:
                    return None
            else:
                raise Exception("Must provide resource group to get specific resource's metadata from group resource({}) ".format(self.resourcename))
        else:
            if resourceid in self.resourcemetadata:
                metadata =  self.resourcemetadata[resourceid]
            elif throw_exception:
                raise ResourceNotFoundError("The resource({}.{}.{}) Not Found".format(self.resourcename,resource_group,resourceid))
            else:
                return None
    
        if self._archive and resource_file:
            if not metadata.get("current") or metadata["current"].get("resource_file"):
                if throw_exception:
                    if resource_group:
                        raise ResourceNotFoundError("Can't find any archived resource in {}.{}.{}".format(self.resourcename,resource_group,resourceid))
                    else:
                        raise ResourceNotFoundError("Can't find any archived resource in {}.{}".format(self.resourcename,resourceid))
                else:
                    return None
            if resource_file == "current" or metadata["current"]["resource_file"] == resource_file:
                metadata = metadata["current"]
            else:
                try:
                    metadata = next(m for m in metadata.get("histories",[]) if m["resource_file"] == resource_file)
                except StopIteration as es:
                    if throw_exception:
                        if resource_group:
                            raise ResourceNotFoundError("The resource({}.{}.{}.{}) Not Found".format(self.resourcename,resource_group,resourceid,resource_file))
                        else:
                            raise ResourceNotFoundError("The resource({}.{}.{}) Not Found".format(self.resourcename,resourceid,resource_file))
                    else:
                        return None

        return metadata


    def delete_resource(self,resourceid=None,resource_group=None):
        """
        delete the resource_group or specified resource 
        return the metadata of deleted resources
        """
        if not resourceid and not resource_group:
            #delete all resources
            if self.group_resource:
                #group resource, delete each group
                for gmetadata in [m for m in self.resourcemetadata.values()]:
                    #delete each group resource
                    for rmetadata in [m for m in gmetadata.values()]:
                        #delete each resource in group resource
                        self._delete_resource(rmetadata)
            else:
                #non group resource
                for rmetadata in [m for m in self.resourcemetadata.values()]:
                    #delete each resource in resource
                    self._delete_resource(rmetadata)

            #delete the resource metadata
            metadata = self.resourcemetadata
            self._metadata_client.delete()
            return metadata


        if self.group_resource:
            if not resourceid and not resource_group:
                raise Exception("Please specify the resource id or the resource_group to delete")
        elif not resourceid:
            raise Exception("Please specify the resource id of the resource you want to delete")

        metadata = self.get_metadata(resourceid=resourceid,resource_group=resource_group,throw_exception=False)
        if not metadata:
            #resource doesn't exist
            if resource_group:
                logger.debug("Resource({}.{}) does not exist".format(resource_group,resourceid))
            else:
                logger.debug("Resource() does not exist".format(resourceid))
            return None

        if resourceid:
            self._delete_resource(metadata)
        else:
            metadata = dict(metadata)
            for m in metadata.values():
                self._delete_resource(m)

        return metadata

    def _delete_resource(self,metadata):
        """
        The metadata of the specific resource you want to delete
        Delete the current archive and all histories archives for archive resource. 
        """
        if metadata["resource_group"]:
            logger.debug("Delete the resource({}.{}.{})".format(self.resourcename,metadata["resource_group"],metadata["resource_id"]))
        else:
            logger.debug("Delete the resource({}.{})".format(self.resourcename,metadata["resource_id"]))
        #delete the resource file from storage
        if self._archive:
            #archive resource
            #delete the current archive
            blob_client = self.get_blob_client(metadata["current"]["resource_path"])
            try:
                blob_client.delete_blob()
            except:
                logger.error("Failed to delete the current resource({}) from blob storage.{}".format(metadata["current"]["resource_path"],traceback.format_exc()))
            #delete all history arvhives
            for m in metadata.get("histroies") or []:
                blob_client = self.get_blob_client(m["resource_path"])
                try:
                    blob_client.delete_blob()
                except:
                    logger.error("Failed to delete the history resource({}) from blob storage.{}".format(m["resource_path"],traceback.format_exc()))

            
        else:
            blob_client = self.get_blob_client(metadata["resource_path"])
            try:
                blob_client.delete_blob()
            except:
                logger.error("Failed to delete the resource({}) from blob storage.{}".format(metadata["resource_path"],traceback.format_exc()))
            

        #delete the deleted resource's metadata from resource metadata file
        resourcemetadata = self.resourcemetadata
        if self.group_resource:
            del resourcemetadata[metadata["resource_group"]][metadata["resource_id"]]
        else:
            del resourcemetadata[metadata["resource_id"]]
        #push the latest metadata to storage
        self._metadata_client.update(resourcemetadata)
        

    def download_group(self,resource_group,folder=None,overwrite=False):
        """
        Only available for group resource
        """
        if not self.group_resource:
            raise Exception("{} is not a group resource.".format(self.resourcename))

        if folder:
            if os.path.exists(folder):
                if not os.path.isdir(folder):
                    #is a folder
                    raise Exception("The path({}) is not a folder.".format(folder))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(folder))
                else:
                    #remove the existing folder
                    shutil.rmtree(folder)

            #create the folder
            os.makedirs(folder)
        else:
            folder = tempfile.mkdtemp(prefix=resource_group)

        groupmetadata = self.get_metadata(resource_group=resource_group,throw_exception=True)
        for metadata in groupmetadata.values():
            if self._archive:
                metadata = metadata.get("current")
            if not metadata:
                continue
            if metadata.get("resource_file") and metadata.get("resource_path"):
                with open(os.path.join(folder,metadata["resource_file"]),'wb') as f:
                    self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (groupmetadata,folder)

    def download(self,resourceid,filename=None,overwrite=False,resource_group=None,resource_file="current"):
        """
        Download the resource with resourceid, and return the filename 
        remove the existing file or folder if overwrite is True
        """
        if filename:
            if os.path.exists(filename):
                if not os.path.isfile(filename):
                    #is a folder
                    raise Exception("The path({}) is not a file.".format(filename))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(filename))
        
        metadata = self.get_metadata(resourceid=resourceid,resource_group=resource_group,throw_exception=True,resource_file=resource_file)
    
        if not filename:
            with tempfile.NamedTemporaryFile(prefix=resourceid) as f:
                filename = f.name

        with open(filename,'wb') as f:
            self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (metadata,filename)


class AzureBlobResource(AzureBlobResourceBase,ResourceStorage):
    """
    A client to upload/download azure resource
    the resource can be a single AzureBlobResource or a group of AzureBlobResource
    if incremental is Ture, this azure resource is uploaded incrementally
    Each AzureResource has a corresponding metadata, the metadata has different structure
    for archive resource:
        each resource_id keeps the latest resource and history resources. resource_id has a list of resource_file
    for non archive resource:
        each resource only keep the latest resource. 
    for grouped resource.
        group resource metadata is a dict between group name and a list of individual resource metata

    """

    def push_resource(self,data,metadata=None,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        #populute the latest resource metadata
        if not metadata:
            metadata = {}
        #get the resourceid
        if self.group_resource and not metadata.get("resource_group"):
            raise Exception("Missing resource group in metadata")

        resource_group = metadata.get("resource_group")
        resourceid = metadata.get("resource_id") or self._f_resourceid(self._resource_name)
        resource_file = metadata.get("resource_file") or self._f_resource_file(resourceid)
        resource_path = self._f_resource_path(self._resource_data_path,resource_group,resource_file)
        if not resourceid:
            raise Exception("Missing resource_id in metadata")

        metadata["publish_date"] = timezone.now()
        metadata["resource_id"] = resourceid
        metadata["resource_file"] = resource_file
        metadata["resource_path"] = resource_path

        resourcemetadata = self.resourcemetadata
        resource_existed = False

        if resource_group:
            if not resourcemetadata:
                resourcemetadata = {}

            if resource_group in resourcemetadata:
                groupmetadata = resourcemetadata[resource_group]
            else:
                groupmetadata = {}
                resourcemetadata[resource_group] = groupmetadata
            
        else:
            if not resourcemetadata:
                resourcemetadata = {}
            
            groupmetadata = resourcemetadata

        #check whether the existing resource exist or not
        if resourceid in groupmetadata:
            #resource already exists
            currentmetadata = groupmetadata[resourceid]
            resource_existed = True
        else:
            currentmetadata = {}
            groupmetadata[resourceid] = currentmetadata


        if self._archive:
            if "histories" not in currentmetadata:
                currentmetadata["histories"] = []
            if currentmetadata.get("current"):
                currentmetadata["histories"].insert(0,currentmetadata["current"])

        #push the resource to azure storage
        blob_client = self.get_blob_client(resource_path)
        blob_client.upload_blob(data,blob_type=BlobType.BlockBlob,overwrite=True,timeout=3600,max_concurrency=5,length=length)
        #update the resource metadata
        if f_post_push:
            f_post_push(metadata)

        if self._archive:
            currentmetadata["current"] = metadata
        else:
            currentmetadata.update(metadata)

        self._metadata_client.update(resourcemetadata)

        return resourcemetadata
        
