import os
import logging


from data_storage.exceptions import ResourceNotFound

from utils import timezone
import utils


logger = logging.getLogger(__name__)

def archive(storage,files=None,folder=None,recursive=False,file_filter=None,reserve_folder=True,archive=True):
    """
    Archive the files or files in folder and push it to azure blob resource
    files: the file or list of files for archive
    folder: all the files in the folder will be archived
    recursive: only used for folder, if true, all the files in the folder and nested folder will be archived.
    file_filter: only used for folder, if not none, only the files which satisfy the filter will be archived
    reserve_folder: only used for folder, if true, the relative folder in folder will be reserved when push to blob storage
    archive: if true, each file version will be saved in blob storage 
    """

    if not files and not folder:
        raise Exception("Either files or folder must be specified. ")

    if files and folder:
        raise Exception("Can't set files or folder at the same time ")
 
    archive_files = None
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
        
        if not archive_files:
            raise Exception("No files in folder({}) are found for archiving.".format(folder))
    else:
        archive_files = []
        folder = os.path.abspath(folder)
        folders = [folder]
        while folders:
            cur_folder = folders.pop(0)
            for f in os.listdir(cur_folder):
                f_path = os.path.join(cur_folder,f)
                if os.path.isfile(f_path):
                    if not file_filter or file_filter(f_path):
                        if reserve_folder:
                            archive_files.append((f_path,os.path.relpath(f_path,folder)))
                        else:
                            archive_files.append((f_path,os.path.split(f_path)[1]))

                elif os.path.isdir(f):
                    if recursive:
                        folders.append(f_path)
                    else:
                        logger.debug("Recursive is False and {} is a sub folder,ignore".format(f_path))

                else:
                    logger.debug("{} is not a regular file and folder,ignore".format(f_path))

    logger.debug("Begin to arvhive files:{}    {}".format(os.linesep,"{}    ".format(os.linesep).join([str(f) for f in archive_files])))

    metadata = None

    #push the updated or new files into storage
    for f,resource_id in archive_files:
        metadata = {}
        try:
            res_metadata = storage.get_resource_metadata(resource_id)
        except ResourceNotFound as ex:
            res_metadata = None

        file_md5 = utils.file_md5(f)
        if res_metadata and res_metadata["file_md5"] == file_md5:
            logger.debug("File({},{}) is not changed, no need to archive again".format(f,resource_id))
            continue


        metadata["archive_time"] = timezone.now()
        metadata["resource_id"] = resource_id
        metadata["file_md5"] = file_md5
        if folder:
            metadata["folder"] = folder

        storage.push_file(f,metadata=metadata)
        logger.debug("File({},{}) was archived successfully.".format(f,resource_id))

    if folder:
        #run in folder mode, remove the deleted file from blob stroage
        non_exist_metas = []
        for meta in storage.resource_metadatas(throw_exception=False,resource_file="current"):
            if meta["folder"] != folder:
                #not belong to the same folder, ignore 
                continue
            if next((f for f in archive_files if f[1] == meta["resource_id"]),None):
                #exist
                continue

            non_exist_metas.append(meta)


        for meta in non_exist_metas:
            storage.delete_resource(resource_id=meta["resource_id"])
            logger.debug("Delete the file({},{}) from storage because it doesn't exist anymore".format(meta["resource_id"],meta["resource_file"]))


