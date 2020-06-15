import os
import logging


from data_storage.exceptions import ResourceNotFound

from utils import timezone
import utils


logger = logging.getLogger(__name__)

FILE_MD5 = 1
FILE_MODIFY_DATE = 2
FILE_SIZE = 3

def _archive_file(storage,f,resource_id,checking_policy,check_md5,metadata={}):
    #push the updated or new files into storage
    file_status = os.stat(f)
    file_modify_date = file_status.st_mtime_ns
    file_size = file_status.st_size
    if check_md5:
        file_md5 = utils.file_md5(f)
    else:
        file_md5 = None

    try:
        res_metadata = storage.get_resource_metadata(resource_id)
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

    storage.push_file(f,metadata=metadata)
    logger.debug("File({},{}) was archived successfully.".format(f,resource_id))
    return True

def archive(storage,files=None,folder=None,recursive=False,file_filter=None,reserve_folder=True,archive=True,checking_policy=[FILE_MD5]):
    """
    Archive the files or files in folder and push it to azure blob resource
    files: the file or list of files for archive
    folder: all the files in the folder will be archived
    recursive: only used for folder, if true, all the files in the folder and nested folder will be archived.
    file_filter: only used for folder, if not none, only the files which satisfy the filter will be archived
    reserve_folder: only used for folder, if true, the relative folder in folder will be reserved when push to blob storage
    archive: if true, each file version will be saved in blob storage 
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
                _archive_file(storage,f,resource_id,checking_policy,check_md5)
    else:
        non_exist_resourceids = set()
        for meta in storage.resource_metadatas(throw_exception=False,resource_file="current"):
            non_exist_resourceids.add(meta["resource_id"])

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
                        _archive_file(storage,f_path,resource_id,checking_policy,check_md5,metadata={"folder":folder})
                        if resource_id in non_exist_resourceids:
                            non_exist_resourceids.remove(resource_id)
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


        for resourceid in non_exist_resourceids:
            storage.delete_resource(resource_id=resourceid)
            logger.debug("Delete the file({}) from storage because it doesn't exist anymore".format(resourceid))


