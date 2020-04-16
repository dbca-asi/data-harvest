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
        self._connection_string = connection_string
        self._container_name = container_name
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

class AzureBlobMetadataBase(AzureJsonBlob):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,metaname="metadata"):
        self._metadata_name = metaname or "metadata"
        metadata_file = "{}.json".format(self._metadata_name) 
        self._resource_base_path = resource_base_path
        if resource_base_path:
            metadata_filepath = "{}/{}".format(resource_base_path,metadata_file)
        else:
            metadata_filepath = metadata_file

        super().__init__(metadata_filepath,connection_string,container_name)
        self._cache = cache

    @property
    def metadata_name(self):
        return self._metadata_name

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

class AzureBlobMetadataIndex(AzureBlobMetadataBase):
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False):
        return super().__init__(connection_string,container_name,resource_base_path=resource_base_path,cache=cache,metaname="_metadata_index")

    def metadata_clients(self,cache=True):
        for name,path in self.json.items():
            yield AzureBlobResourceMetadata(self._connection_string,self._container_name,resource_base_path=self._resource_base_path,cache=cache,metaname=name)

    @property
    def metadatas(self):
        return [AzureBlobResourceMetadata(self._connection_string,self._container_name,resource_base_path=self._resource_base_path,cache=False,metaname=name).json for name in self.json.keys()]

    def add(self,metadata_name,metadata_filepath):
        index_json = self.json

        if index_json is None:
            index_json = {metadata_name:metadata_filepath}
        elif self._metadata_file not in index_json:
            index_json[metadata_name] = metadata_filepath
        else:
            return
        self.update(index_json)

    def remove(self,metadata_name):
        #remove this metadata file from index metadata file
        index_json = self.json
        if index_json is None:
            return
        elif metadata_name in index_json:
            del index_json[metadata_name]
        else:
            return

        if index_json:
            self.update(index_json)
        else:
            #no more metadata files, remove the index metadata file
            self.delete()


class AzureBlobResourceMetadataBase(AzureBlobMetadataBase):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_id"]

    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,metaname="metadata",archive=False):
        super().__init__(connection_string,container_name,resource_base_path=resource_base_path,cache=cache,metaname=metaname)
        self._archive = True if archive else False

    def _get_pushed_resource_metadata(self,metadata,resource_file="current"):
        """
        get metadata from resource's metadata against resource_file
        """
        if self._archive and resource_file:
            if metadata.get("current",{}).get("resource_file") and (metadata.get("current",{}).get("resource_file") == resource_file or resource_file == "current"):
                return metadata["current"]

            if metadata.get("histories") and resource_file != "current":
                try:
                    return next(m for m in metadata.get("histories",[]) if m["resource_file"] == resource_file)
                except StopIteration as ex:
                    raise ResourceNotFoundError("Resource({},resource_file={}) Not Found".format(".".join(metadata[k] for k in self.resource_keys),resource_file))
            else:
                raise ResourceNotFoundError("Resource({},resource_file={}) Not Found".format(".".join(metadata[k] for k in self.resource_keys),resource_file))
        else:
            return metadata

    def get_resource_metadatas(self,**kwargs):
        """
        Return a iterator object to navigate the metadata of all pushed individual resources or specified resource; if not exist, return a empty list.
        """
        metadata = self.json or {}
        index = 0
        while index < len(self.resource_keys):
            key = self.resource_keys[index]
            if kwargs.get(key):
                index += 1
                if metadata.get(kwargs[key]):
                    metadata = metadata[kwargs[key]]
                else:
                    raise ResourceNotFoundError("Resource({}) Not Found".format(".".join(kwargs[k] for k in self.resource_keys[0:index])))
            else:
                break

        resource_file = kwargs.get("resource_file")
        if index == len(self.resource_keys):
            yield self._get_pushed_resource_metadata(metadata,resource_file)
        else:
            for m1 in metadata.values():
                if (index + 1) == len(self.resource_keys):
                    yield self._get_pushed_resource_metadata(m1,resource_file)
                else:
                    for m2 in m1.values():
                        if (index + 2) == len(self.resource_keys):
                            yield self._get_pushed_resource_metadata(m2,resource_file)
                        else:
                            for m3 in m2.values():
                                if (index + 2) == len(self.resource_keys):
                                    yield self._get_pushed_resource_metadata(m3,resource_file)
                                else:
                                    raise Exception("Not implemented")


    def get_resource_metadata(self,*args,resource_file="current"):
        """
        Return a iterate object to navigate the metadata of all pushed individual resources or specified resource; if not exist, throw exception
        """
        return next(self.get_resource_metadatas(resource_file=resource_file,**dict(zip(self.resource_keys,args))))

    def remove_resource(self,*args):
        """
        Remove the resource's metadata. 
        Return the metadata of the remove resource; if not found, return None
        """
        metadata = self.json
        p_metadata = metadata
        if len(self.resource_keys) != len(args):
            raise Exception("Invalid args({})".format(args))

        for key in args[:-1]:
            p_metadata = p_metadata.get(key)
            if not p_metadata:
                #not exist
                return None

        if args[-1] not in p_metadata:
            #not exist
            return None
        else:
            resource_metadata = p_metadata[args[-1]]
            del p_metadata[args[-1]]
            
            last_index = len(args) - 2
            while last_index >= 0:
                p_metadata = metadata
                if last_index > 0:
                    for key in args[0:last_index]:
                        p_metadata = p_metadata[key]
                if args[last_index] in p_metadata and not p_metadata[args[last_index]]:
                    del p_metadata[args[last_index]]
                last_index -= 1

            if metadata:
                self.update(metadata)
            else:
                self.delete()
        return resource_metadata

    def add_resource(self,resource_metadata):
        """
        Add the resource's metadata
        Return a tuple(the whole  metadata,created?)
        """
        metadata = self.json or {}
        exist_metadata = metadata
        existed = True
        for k in self.resource_keys:
            val = resource_metadata.get(k)
            if not val:
                raise Exception("Missing key({}) in resource metadata".format(k))
            if val not in exist_metadata:
                existed = False
                exist_metadata[val] = {}
            exist_metadata = exist_metadata[val]


        if self._archive:
            if existed:
                if exist_metadata.get("histories"):
                    exist_metadata["histories"].append(exist_metadata["current"])
                else:
                    exist_metadata["histories"] = [exist_metadata["current"]]
            exist_metadata["current"] = resource_metadata
        else:
            exist_metadata.update(resource_metadata)
        self.update(metadata)
        return (metadata,not existed)

class AzureBlobResourceMetadata(AzureBlobResourceMetadataBase):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_id"]

class AzureBlobGroupResourceMetadata(AzureBlobResourceMetadataBase):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_group","resource_id"]

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


class AzureBlobResourceBase(ResourceStorage):
    """
    A base client to manage a Azure Resourcet
    """
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,archive=True):
        self._resource_name = resource_name
        self._resource_base_path = resource_name if resource_base_path is None else resource_base_path
        if self._resource_base_path:
            self._resource_data_path = "{}/data".format(self._resource_base_path)
        else:
            self._resource_data_path = "data"
        self._connection_string = connection_string
        self._container_name = container_name
        self._archive = archive

    @property
    def resourcename(self):
        return self._resource_name

    def _get_resource_file(self,resourceid):
        """
        Get a default resource file from resourceid
        """
        if self._archive:
            file_name,file_ext = os.path.splitext(resourceid)
            return "{0}_{1}{2}".format(file_name,timezone.now().strftime("%Y-%m-%d-%H-%M-%S"),file_ext)
        else:
            return resourceid

    def _get_resource_path(self,metadata):
        """
        Get the resoure path for resource_file
        """
        if len(self._metadata_client.resource_keys) > 1:
            return "{0}/{1}/{2}".format(self._resource_data_path,"/".join(metadata[k] for k in self._metadata_client.resource_keys[:-1]),metadata["resource_file"])
        else:
            return "{0}/{1}".format(self._resource_data_path,metadata["resource_file"])


    def get_resource_metadata(self,*args,resource_file="current"):
        """
        if resurce_file is "current", it means the latest archive of the specific resource; otherwise, it should be a resource's resource file ; only meaningful for archived resource
        throw exception if not found
        Return the resource's metadata
        """
        return self._metadata_client.get_resource_metadata(resource_file=resource_file,*args)

    def get_resource_metadatas(self,throw_exception=False,**kwargs):
        """
        if resurce_file is "current", it means the latest archive of the specific resource; otherwise, it should be a resource's resource file ; only meaningful for archived resource
        throw exception if not found if throw_exception is True;otherwise return empty iterator
        Return a iterator to narvigate the metadata of the filtered resource
        """
        try:
            return self._metadata_client.get_resource_metadatas(**kwargs)
        except ResourceNotFoundError as ex:
            if throw_exception:
                raise
            else:
                return []

    def is_exist(self,*args):
        try:
            return True if self._metadata_client.get_resource_metadata(resource_file=None,*args) else False
        except ResourceNotFoundError as ex:
            return False
        

    def delete_resource(self,**kwargs):
        """
        delete the resource_group or specified resource 
        return the list of the metadata of deleted resources
        """
        if "resource_file" in kwargs:
            raise Exception("Parameter(resource_file) Not Support")
        metadatas = [ m for m in self.get_resource_metadatas(resource_file=None,throw_exception=True,**kwargs)]
        for m in metadatas:
            self._delete_resource(m)

        return metadatas

    def _delete_resource(self,metadata):
        """
        The metadata of the specific resource you want to delete
        Delete the current archive and all histories archives for archive resource. 
        """
        logger.debug("Delete the resource({}.{})".format(self.resourcename,".".join(metadata[k] for k in self._metadata_client.resource_keys)))
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
            
        #remove the resource from metadata
        self._metadata_client.remove_resource(*[metadata[k] for k in self._metadata_client.resource_keys])
        

    def download_resources(self,folder=None,overwrite=False,**kwargs):
        """
        Only available for group resource
        """
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

        kwargs["resource_file"] = "current"
        metadatas = [m for m in self.get_resource_metadatas(throw_exception=True,**kwargs)]
        for metadata in metadatas:
            if metadata.get("resource_file") and metadata.get("resource_path"):
                logger.debug("Download resource {}".format(metadata["resource_path"]))
                with open(os.path.join(folder,metadata["resource_file"]),'wb') as f:
                    self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (metadatas,folder)

    def download_resource(self,*args,filename=None,overwrite=False):
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
        
        metadata = self.get_resource_metadata(resource_file="current",*args)
    
        if not filename:
            with tempfile.NamedTemporaryFile(prefix=resourceid) as f:
                filename = f.name

        logger.debug("Download resource {}".format(metadata["resource_path"]))
        with open(filename,'wb') as f:
            self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (metadata,filename)

    def get_blob_client(self,resource_path):
        return BlobClient.from_connection_string(self._connection_string,self._container_name,resource_path,**settings.AZURE_BLOG_CLIENT_KWARGS)


    def push_resource(self,data,metadata,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        #populute the latest resource metadata
        for key in self._metadata_client.resource_keys:
            if key not in metadata:
                raise Exception("Missing resource key({}) in metadata".format(key))

        if "resource_file" not in metadata:
            metadata["resource_file"] = self._get_resource_file(metadata["resource_id"])
        metadata["resource_path"] = self._get_resource_path(metadata)     
        metadata["publish_date"] = timezone.now()

        #push the resource to azure storage
        blob_client = self.get_blob_client(metadata["resource_path"])
        blob_client.upload_blob(data,blob_type=BlobType.BlockBlob,overwrite=True,timeout=3600,max_concurrency=5,length=length)
        #update the resource metadata
        if f_post_push:
            f_post_push(metadata)

        self._metadata_client.add_resource(metadata)

        return self._metadata_client.json
        
class AzureBlobResource(AzureBlobResourceBase):
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,archive=True,metaname="metadata"):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=archive)
        self._metadata_client = AzureBlobResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,cache=True,metaname=metaname,archive=archive)

class AzureBlobGroupResource(AzureBlobResourceBase):
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,archive=True,metaname="metadata"):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=archive)
        self._metadata_client = AzureBlobGroupResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,cache=True,metaname=metaname,archive=archive)



