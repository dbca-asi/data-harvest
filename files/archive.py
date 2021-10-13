import os
import logging


from data_storage.exceptions import ResourceNotFound
from data_storage import ResourceConstant,MetadataSession,LockSession

from utils import timezone
import utils


logger = logging.getLogger(__name__)

FILE_MD5 = 1
FILE_MODIFY_DATE = 2
FILE_SIZE = 3

def _archive_file(repository,f,resource_id,checking_policy,check_md5,metadata={}):
    #push the updated or new files into repository
    file_status = os.stat(f)
    file_modify_date = file_status.st_mtime_ns
    file_size = file_status.st_size
    if check_md5:
        file_md5 = utils.file_md5(f)
    else:
        file_md5 = None

    try:
        res_metadata = repository.get_resource_metadata(resource_id)
    except ResourceNotFound as ex:
        res_metadata = None

    is_changed = False
    for policy in checking_policy:
        if policy == FILE_MD5:
            if not res_metadata or res_metadata.get("file_md5") != file_md5:
                is_changed = True
                break
        elif policy == FILE_MODIFY_DATE:
            if not res_metadata or res_metadata.get("file_modify_date") != file_modify_date:
                is_changed = True
                break
        elif policy == FILE_SIZE:
            if not res_metadata or res_metadata.get("file_msize") != file_size:
                is_changed = True
                break
        else:
            raise Exception("Checking policy({}) Not Support".format(policy))

    if not is_changed:
        logger.debug("File({},{}) is not changed, no need to archive again".format(f,resource_id))
        return False

    metadata["archive_time"] = timezone.now()
    metadata["resource_id"] = resource_id
    metadata["file_modify_date"] = file_modify_date
    metadata["file_size"] = file_size
    if check_md5:
        metadata["file_md5"] = file_md5

    repository.push_file(f,metadata=metadata)
    logger.debug("File({},{}) was archived successfully.".format(f,resource_id))
    return True

def archive(repository,files=None,folder=None,recursive=False,file_filter=None,reserve_folder=True,archive=True,checking_policy=[FILE_MD5]):
    """
    Archive the files or files in folder and push it to azure blob resource
    files: the file or list of files for archive
    folder: all the files in the folder will be archived
    recursive: only used for folder, if true, all the files in the folder and nested folder will be archived.
    file_filter: only used for folder, if not none, only the files which satisfy the filter will be archived
    reserve_folder: only used for folder, if true, the relative folder in folder will be reserved when push to repository
    archive: if true, each file version will be saved in repository
    checking_policy: the policy to check whether file is modified or not. can be single policy or list of policy
    """

    if not files and not folder:
        raise Exception("Either files or folder must be specified. ")

    if files and folder:
        raise Exception("Can't set files or folder at the same time ")

    if not checking_policy:
        checking_policy = [FILE_MD5]
    elif not isinstance(checking_policy,(list,tuple)):
        checking_policy = [checking_policy]
    check_md5 = FILE_MD5 in checking_policy
 
    with LockSession(repository,3600,3000) as lock_session:
        with MetadataSession() as session:
            if files:
                if not isinstance(files,(tuple,list)):
                    archive_files = [(os.path.abspath(files),os.path.split(files)[1])]
                else:
                    archive_files = [(os.path.abspath(f),os.path.split(f)[1]) for f in files ]
        
                #check whether file exist or not.
                for f,resource_id in archive_files:
                    if os.path.exists(f):
                        raise Exception("File {} does not exist".format(f))
                    elif not os.path.isfile(f):
                        raise Exception("{} is not a file".format(f))
                    else:
                        _archive_file(repository,f,resource_id,checking_policy,check_md5)
                        lock_session.renew_if_needed()
            else:
                non_exist_resourceids = {}
                for meta in repository.resource_metadatas(throw_exception=False,current_resource=True,resource_status=ResourceConstant.ALL_RESOURCE):
                    non_exist_resourceids[meta["resource_id"]] = meta.get(ResourceConstant.DELETED_KEY,False)
        
                folder = os.path.abspath(folder)
                folders = [folder]
                f_path = None
                resource_id = None
                while folders:
                    cur_folder = folders.pop(0)
                    for f in os.listdir(cur_folder):
                        f_path = os.path.join(cur_folder,f)
                        if os.path.isfile(f_path):
                            if not file_filter or file_filter(os.path.relpath(f_path,folder)):
                                if reserve_folder:
                                    resource_id = os.path.relpath(f_path,folder)
                                else:
                                    resource_id = os.path.split(f_path)[1]
                                _archive_file(repository,f_path,resource_id,checking_policy,check_md5,metadata={"folder":folder})
                                lock_session.renew_if_needed()
                                if resource_id in non_exist_resourceids:
                                    del non_exist_resourceids[resource_id]
                            else:
                                pass
                                #logger.debug("File({}) is filtered out by file filter,ignore".format(f_path))
        
                        elif os.path.isdir(f_path):
                            if recursive:
                                folders.append(f_path)
                            else:
                                logger.debug("Recursive is False and {} is a sub folder,ignore".format(f_path))
        
                        else:
                            logger.debug("{} is not a regular file and folder,ignore".format(f_path))
        
        
                for resourceid,is_deleted in non_exist_resourceids.items():
                    if not file_filter or file_filter(resourceid):
                        if not is_deleted:
                            repository.delete_resource(resourceid,permanent_delete=False)
                            lock_session.renew_if_needed()
                            logger.debug("Logically delete the file({}) from repository because it doesn't exist anymore".format(resourceid))
                    else:
                        repository.delete_resource(resourceid,permanent_delete=True)
                        logger.debug("Permanently delete the file({}) from repository because it doesn't meet the filter condition".format(resourceid))
        


def clean_expired_deleted_resources(repository,expire_time):
    """
    clean resources which is satisified with deleted_resource_filter
    """

    logger.info("Begin to find all expired deleted resources")
    expired_resourceids = set()
    with LockSession(repository,3600,3000) as lock_session:
        total_resources = 0
        now = timezone.now()
        for meta in repository.resource_metadatas(throw_exception=False,current_resource=True,resource_status=ResourceConstant.DELETED_RESOURCE):
            total_resources += 1
            if ResourceConstant.DELETE_TIME_KEY in meta and now > meta[ResourceConstant.DELETE_TIME_KEY] + expire_time:
                expired_resourceids.add(meta["resource_id"])

        lock_session.renew()
                
        total = len(expired_resourceids)
        logger.info("Found {}/{} expired deleted resources".format(total,total_resources))
        
        deleted = 0
        
        with MetadataSession() as session:
            for resourceid in expired_resourceids:
                repository.delete_resource(resourceid,permanent_delete=True)
                logger.debug("Permanently delete the file({}) from repository because it doesn't meet the filter condition".format(resourceid))
                deleted += 1
                lock_session.renew_if_needed()
        logger.info("Permanently delete {}/{} resources".format(deleted,total))


def clean_resources(repository,delete_resource_filter,batch=None):
    """
    clean resources which is satisified with delete_resource_filter
    """

    logger.info("Begin to find all deleted resources")
    delete_resourceids = set()

    with LockSession(repository,3600,3000) as lock_session:
        total_resources = 0
        for meta in repository.resource_metadatas(throw_exception=False,current_resource=True,resource_status=ResourceConstant.ALL_RESOURCE):
            total_resources += 1
            if delete_resource_filter(meta): 
                delete_resourceids.add(meta["resource_id"])
                
        total = len(delete_resourceids)
        logger.info("Found {}/{} deleted resources".format(total,total_resources))
        
        lock_session.renew()

        deleted = 0
        while deleted < total:
            with MetadataSession() as session:
                while deleted < total:
                    resourceid = delete_resourceids.pop()
                    repository.delete_resource(resourceid,permanent_delete=True)
                    logger.info("Permanently delete the file({}) from repository because it doesn't meet the filter condition".format(resourceid))
                    deleted += 1
                    lock_session.renew_if_needed()
                    if batch and deleted % batch == 0:
                        break
        logger.info("Permanently delete {}/{} resources".format(deleted,total))

        clean_orphan_resources(repository)



def clean_orphan_resources(repository):
    with LockSession(repository,3600,3000) as lock_session:
        all_resourceids = set()
        for meta in repository.resource_metadatas(throw_exception=False,current_resource=True,resource_status=ResourceConstant.ALL_RESOURCE):
            all_resourceids.add(meta["resource_id"])
                
        total = len(all_resourceids)
        logger.info("Found {} resources".format(total))
        
        lock_session.renew()
        data_path = repository.resource_data_path
        if data_path[-1] != "/":
            data_path = "{}/".format(data_path)

        orphan_resources = []
        for resource in repository.storage.list_resources(data_path):
            name = resource.name[len(data_path):]
            if name not in all_resourceids:
                orphan_resources.append(resource.name)


        logger.info("Found {} orphan resources".format(len(orphan_resources)))

        for resource in orphan_resources:
            repository.storage.delete(resource)
            logger.info("Delete orphan resource '{}' from repository".format(resource))
            


        logger.info("Deleted {} orphan resources".format(len(orphan_resources)))
