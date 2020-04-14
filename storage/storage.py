import json
import tempfile
import os

from utils import JSONEncoder,JSONDecoder,file_size

class ResourceStorage(object):
    """
    A interface to list/upload/get a resource 
    """

    @property
    def resourcename(self):
        """
        the resource name.
        """
        raise NotImplementedError("Property 'resourcename' is not implemented.")

    @property
    def resource(self):
        """
        Return (the current resourceid,the current resource)
        Return (The current resurceid,None) if resource with the current resourceid is not found
        Return (None,None) if can't find the current resourceid
        """
        return self.get_resource()

    @property
    def json(self):
        """
        Return (the current resourceid,the current resource as dist object)
        Return (The current resurceid,None) if resource with the current resourceid is not found
        Return (None,None) if can't find the current resourceid
        """
        return self.get_json()


    @property
    def resourcemetadata(self):
        """
        Return resource metadata.
        including 
            current resourse metadata
            resource history list
        """
        raise NotImplementedError("Property 'resourcemetadata' is not implemented.")

    def get_metadata(self,resourceid=None,resource_group=None,resource_file="current",throw_exception=False):
        """
        if resource_file is 'current', it means the latest archive of the specific resource
        Return 
            if resourceid is none and (resource_group is none or resource is not a group resource), return all resource metadata
            if resourceid is none and resource_group is not none annd resource is a group resource, return group metadata
            if resource_file is not none, and resource is archive resource, return the specific resource archivement's metadata, if exists, otherwise return None or throw exception
            if archived is none or resource is non archive resource, return the specific resource's metadata, if exists, otherwise return None or throw exception
        """
        raise NotImplementedError("Method 'resource_exist' is not implemented.")

    def is_exist(self,resourceid,resource_group=None):
        """
        Check whether resource exists or not
        """
        return True if self.get_metadata(resourceid=resourceid,resource_group=resource_group) else False

    def delete_resource(self,resourceid=None,resource_group=None):
        """
        delete the resource_group or specified resource , or all resources
        return the meta data of the deleted resource
        """
        raise NotImplementedError("Method 'delete_resource' is not implemented.")

    def download(self,resourceid=None,filename=None,overwrite=False,resource_group=None):
        """
        Download the resource with resourceid, and return (resource metadata,local resource's filename)
        overwrite: remove the existing file or folder if overwrite is True
        if resourceid is None, return (the current resourceid, the current resource)
        raise exception if failed or can't find the resource
        """
        raise NotImplementedError("Method 'get_resource' is not implemented.")

    def get_json(self,resourceid=None):
        """
        Return (resource_metadata,resource as dict object)
        raise exception if failed or can't find the resource
        """
        metadata,filename = self.download_resource(resourceid)
        try:
            with open(filename,'r') as f:
                return (metadata,json.loads(f.read(),cls=JSONDecoder))
        finally:
            os.remove(filename)

    def push_resource(self,data,metadata=None,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        raise NotImplementedError("Method 'push_resource' is not implemented.")

        
    def push_json(self,obj,metadata=None,f_post_push=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        return self.push_resource(json.dumps(obj,cls=JSONEncoder).encode(),metadata=metadata,f_post_push=f_post_push)

    def push_file(self,filename,metadata=None,f_post_push=None):
        """
        Push the resource from file to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        file_length = file_size(filename)
        with open(filename,'rb') as f:
            return self.push_resource(f,metadata=metadata,f_post_push=f_post_push,length=file_length)
