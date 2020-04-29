import os
import traceback
import logging
import tempfile
from datetime import date,timedelta


from utils import timezone,gdal
import utils

from storage.azure_blob import AzureBlobIndexedGroupResource
from storage.exception import ResourceAlreadyExist

from . import settings

logger = logging.getLogger(__name__)

#the sql to find the earliest achiving date
earliest_archive_date = "SELECT min(seen) FROM tracking_loggedpoint"
#the sql to recreate the missing device from loggedpoint archive
missing_device_sql = "INSERT INTO tracking_device (deviceid) SELECT distinct a.deviceid FROM {0} a WHERE NOT EXISTS(SELECT 1 FROM tracking_device b WHERE a.deviceid = b.deviceid)"
#restore the loggedpoint from archive file to tracking_loggedpoint table with orignal id
restore_with_id_sql = """INSERT INTO tracking_loggedpoint (id,device_id,point,heading,velocity,altitude,seen,message,source_device_type,raw)
    SELECT a.id,b.id,a.point,a.heading,a.velocity,a.altitude,to_timestamp(a.seen),a.message,a.source_device_type,a.raw
    FROM {0} a JOIN tracking_device b on a.deviceid = b.deviceid"""

#restore the loggedpoint from archive file to tracking_loggedpoint table with new id
restore_sql = """INSERT INTO tracking_loggedpoint (device_id,point,heading,velocity,altitude,seen,message,source_device_type,raw)
    SELECT b.id,a.point,a.heading,a.velocity,a.altitude,a.seen,a.message,a.source_device_type,a.raw
    FROM {0} a JOIN tracking_device b on a.deviceid = b.deviceid"""

#The sql to return the loggedpoint data to archive
archive_sql = "SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,extract(epoch from a.seen)::bigint as seen,b.deviceid,b.registration FROM tracking_loggedpoint a JOIN tracking_device b ON a.device_id = b.id WHERE a.seen >= '{0}' AND a.seen < '{1}'"
backup_sql_with_create_table = "SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,a.seen,b.deviceid,b.registration INTO \"{2}\" FROM tracking_loggedpoint a JOIN tracking_device b ON a.device_id = b.id WHERE a.seen >= '{0}' AND a.seen < '{1}'"
backup_sql = """INSERT INTO "{2}" (id,point,heading,velocity,altitude,message,source_device_type,raw,seen,deviceid,registration) 
    SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,a.seen,b.deviceid,b.registration 
    FROM tracking_loggedpoint a JOIN tracking_device b ON a.device_id = b.id WHERE a.seen >= '{0}' AND a.seen < '{1}'"""
#the sql to delete the archived loggedpoint from table tracking_loggedpoint
del_sql = "DELETE FROM tracking_loggedpoint WHERE seen >= '{0}' AND seen < '{1}'"
#the datetime pattern used in the sql
datetime_pattern = "%Y-%m-%d %H:%M:%S %Z"
#the vrt pattern to generate a union layer for monthly archive 
vrt = """<OGRVRTDataSource>
    <OGRVRTUnionLayer name="{}">
{}
    </OGRVRTUnionLayer>
</OGRVRTDataSource>"""
#the pattern to populate a individual layer used in the union layer vrt file
individual_layer = """        <OGRVRTLayer name="{}">
            <SrcDataSource>{}</SrcDataSource>
        </OGRVRTLayer>"""

#function to get the archive group name from archive date
get_archive_group = lambda d:d.strftime("%Y-%m")
#function to get the archive id from date from archive date
get_archive_id= lambda d:d.strftime("loggedpoint%Y-%m-%d")

get_vrt_id= lambda archive_group:"loggedpoint{}.vrt".format(archive_group)
get_vrt_layername= lambda archive_group:"loggedpoint{}".format(archive_group)

get_backup_table= lambda d:"tracking_loggedpoint_{}".format(d.strftime('%Y'))

index_metaname = "loggedpoint_index"

get_metaname = lambda archive_group:"loggedpoint{}".format(archive_group.split("-")[0])
_blob_resource = None
def get_blob_resource():
    """
    Return the blob resource client
    """
    global _blob_resource
    if _blob_resource is None:
        _blob_resource = AzureBlobIndexedGroupResource(
            settings.LOGGEDPOINT_RESOURCE_NAME,
            settings.AZURE_CONNECTION_STRING,
            settings.AZURE_CONTAINER,
            get_metaname,
            archive=False,
            index_metaname=index_metaname
        )
    return _blob_resource

def continuous_archive(delete_after_archive=False,check=False,max_archive_days=None,overwrite=False,backup_to_archive_table=True):
    """
    Continuous archiving the loggedpoint.
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    max_archive_days: the maxmium days to arhive
    overwrite: if true, overwrite the existing archived file;if false, throw exception if already archived 
    """
    db = settings.DATABASE
    earliest_date = db.get(earliest_archive_date)[0]
    if earliest_date is None:
        logger.info("No more data to archive")
        return

    earliest_date = timezone.nativetime(earliest_date).date()
    now = timezone.now()
    today = now.date()
    if settings.END_WORKING_HOUR is not None and now.hour <= settings.END_WORKING_HOUR:
        if settings.START_WORKING_HOUR is None or now.hour >= settings.START_WORKING_HOUR:
            raise Exception("Please don't run continuous archive in working hour")

    if settings.START_WORKING_HOUR is not None and now.hour >= settings.START_WORKING_HOUR:
        if settings.END_WORKING_HOUR is None or now.hour <= settings.END_WORKING_HOUR:
            raise Exception("Please don't run continuous archive in working hour")

    last_archive_date = today - timedelta(days=settings.LOGGEDPOINT_ACTIVE_DAYS)
    archive_date = earliest_date
    archived_days = 0
    max_archive_days = max_archive_days if max_archive_days and  max_archive_days > 0 else None

    logger.info("Begin to continuous archiving loggedpoint, earliest archive date={0},last archive date = {1}, delete_after_archive={2}, check={3}, max_archive_days={4}".format(
        earliest_date,last_archive_date,delete_after_archive,check,max_archive_days
    ))
    if archive_date >= last_archive_date:
        logger.info("No more data to archive")
        return

    while archive_date < last_archive_date and (not max_archive_days or archived_days < max_archive_days):
        now = timezone.now()
        if settings.END_WORKING_HOUR is not None and now.hour <= settings.END_WORKING_HOUR:
            if settings.START_WORKING_HOUR is None or now.hour >= settings.START_WORKING_HOUR:
                logger.info("Stop archiving in working hour")
                break

        if settings.START_WORKING_HOUR is not None and now.hour >= settings.START_WORKING_HOUR:
            if settings.END_WORKING_HOUR is None or now.hour <= settings.END_WORKING_HOUR:
                logger.info("Stop archiving in working hour")
                break

        archive_by_date(archive_date,delete_after_archive=delete_after_archive,check=check,overwrite=overwrite,backup_to_archive_table=backup_to_archive_table)
        archive_date += timedelta(days=1)
        archived_days += 1

def archive_by_month(year,month,delete_after_archive=False,check=False,overwrite=False,backup_to_archive_table=True):
    """
    Archive the logged point for the month.
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    overwrite: if true, overwrite the existing archived file;if false, throw exception if already archived 
    """
    now = timezone.now()
    today = now.date()
    archive_date = date(year,month,1)
    #find the first day of next month
    last_archive_date = date(archive_date.year if archive_date.month < 12 else (archive_date.year + 1), (archive_date.month + 1) if archive_date.month < 12 else 1,1)
    if last_archive_date >= today:
        last_archive_date = today
    if archive_date >= today:
        raise Exception("Can only archive the logged points happened before today")

    logger.info("Begin to archive loggedpoint by month, month={0}/{1}, start archive date={2}, end archive date={3}".format(
        year,month,archive_date,last_archive_date
    ))

    while archive_date < last_archive_date:
        archive_by_date(archive_date,delete_after_archive=delete_after_archive,check=check,overwrite=overwrite,backup_to_archive_table=backup_to_archive_table)
        archive_date += timedelta(days=1)

def archive_by_date(d,delete_after_archive=False,check=False,overwrite=False,backup_to_archive_table=True):
    """
    Archive the logged point within the specified date
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    overwrite: if true, overwrite the existing archived file;if false, throw exception if already archived 
    """
    now = timezone.now()
    today = now.date()
    if d >= today:
        raise Exception("Can only archive the logged points happened before today")
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    start_date = timezone.datetime(d.year,d.month,d.day)
    end_date = start_date + timedelta(days=1)
    backup_table = get_backup_table(d) if backup_to_archive_table else None
    return archive(archive_group,archive_id,start_date,end_date,delete_after_archive=delete_after_archive,check=check,overwrite=overwrite,backup_table=backup_table)


def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

def archive(archive_group,archive_id,start_date,end_date,delete_after_archive=False,check=False,overwrite=False,backup_table=None):
    """
    Archive the resouce tracking history by start_date(inclusive), end_date(exclusive)
    archive_id: a unique identity of the archive file. that means different start_date and end_date should have a different archive_id
    overwrite: False: raise exception if archive_id already exists; True: overwrite the existing archive file
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    """
    db = settings.DATABASE
    resource_id = "{}.gpkg".format(archive_id)
    metadata = {
        "start_archive":timezone.now(),
        "resource_id":resource_id,
        "resource_group":archive_group,
        "start_archive_date":start_date,
        "end_archive_date":end_date
    }

    filename = None
    vrt_filename = None
    work_folder = tempfile.mkdtemp(prefix="archive_loggedpoint")
    def set_end_archive(metadata):
        metadata["end_archive"] = timezone.now()
    resourcemetadata = None
    try:
        logger.info("Begin to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        blob_resource = get_blob_resource()
        if not overwrite:
            #check whether achive exist or not
            if blob_resource.is_exist(archive_group,resource_id):
                raise ResourceAlreadyExist("The loggedpoint has already been archived. archive_id={0},start_archive_date={1},end_archive_date={2}".format(archive_id,start_date,end_date))

        #export the archived data as geopackage
        sql = archive_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
        export_result = db.export_spatial_data(sql,filename=os.path.join(work_folder,"loggedpoint.gpkg"),layer=archive_id)
        if not export_result:
            logger.info("No loggedpoints to archive, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
            return

        layer_metadata,filename = export_result
        metadata["file_md5"] = utils.file_md5(filename)
        metadata["layer"] = layer_metadata["layer"]
        metadata["features"] = layer_metadata["features"]
        #upload archive file
        logger.debug("Begin to push loggedpoint archive file to blob storage, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        resourcemetadata = blob_resource.push_file(filename,metadata,f_post_push=_set_end_datetime("end_archive"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether loggedpoint archive file was pushed to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_metadata,d_filename = blob_resource.download_resource(archive_group,resource_id,filename=os.path.join(work_folder,"loggedpoint_download.gpkg"))
            d_file_md5 = utils.file_md5(d_filename)
            if metadata["file_md5"] != d_file_md5:
                raise Exception("Upload loggedpoint archive file failed.source file's md5={}, uploaded file's md5={}".format(metadata["file_md5"],d_file_md5))

            d_layer_metadata = gdal.get_layers(d_filename)[0]
            if d_layer_metadata["features"] != layer_metadata["features"]:
                raise Exception("Upload loggedpoint archive file failed.source file's features={}, uploaded file's features={}".format(layer_metadata["features"],d_layer_metadata["features"]))
        

        #update vrt file
        logger.debug("Begin to update vrt file to union all spatial files in the same group, archive_group={},archive_id={},start_date={},end_date={}".format(
            archive_group,archive_id,start_date,end_date
        ))
        groupmetadata = resourcemetadata[archive_group]
        vrt_id = get_vrt_id(archive_group)
        try:
            vrt_metadata = next(m for m in groupmetadata.values() if m["resource_id"] == vrt_id)
        except StopIteration as ex:
            vrt_metadata = {"resource_id":vrt_id,"resource_file":vrt_id,"resource_group":archive_group}

        vrt_metadata["features"] = 0
        for m in groupmetadata.values():
            if m["resource_id"] == vrt_id:
                continue
            vrt_metadata["features"] += m["features"]

        layers =  [(m["layer"],m["resource_file"]) for m in groupmetadata.values() if m["resource_id"] != vrt_id]
        layers.sort(key=lambda o:o[0])
        layers = os.linesep.join(individual_layer.format(m[0],m[1]) for m in layers )
        vrt_data = vrt.format(get_vrt_layername(archive_group),layers)
        vrt_filename = os.path.join(work_folder,"loggedpoint.vrt")
        with open(vrt_filename,"w") as f:
            f.write(vrt_data)

        vrt_metadata["file_md5"] = utils.file_md5(vrt_filename)

        resourcemetadata = blob_resource.push_file(vrt_filename,vrt_metadata,f_post_push=_set_end_datetime("updated"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether the group vrt file was pused to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_vrt_metadata,d_vrt_filename = blob_resource.download_resource(archive_group,vrt_id,filename=os.path.join(work_folder,"loggedpoint_download.vrt"))
            d_vrt_file_md5 = utils.file_md5(d_vrt_filename)
            if vrt_metadata["file_md5"] != d_vrt_file_md5:
                raise Exception("Upload vrt file failed.source file's md5={}, uploaded file's md5={}".format(vrt_metadata["file_md5"],d_vrt_file_md5))

        if delete_after_archive:
            if backup_table:
                if db.is_table_exist(backup_table):
                    #table already exist
                    sql = backup_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern),backup_table)
                else:
                    #table doesn't exist
                    sql = backup_sql_with_create_table.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern),backup_table)
                count = db.update(sql)
                if count == layer_metadata["features"]:
                    logger.debug("Backup {1} features to backup table {0}".format(backup_table,count))
                else:
                    raise Exception("Only backup {1}/{2} features to backup table {0}".format(backup_table,count,layer_metadata["features"]))

            logger.debug("Begin to delete archived data, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))

            delete_sql = del_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
            deleted_rows = db.update(delete_sql)
            logger.debug("Delete {} rows from table tracking_loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(
                deleted_rows,archive_group,archive_id,start_date,end_date
            ))

        logger.info("End to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={},archived features={}".format(archive_group,archive_id,start_date,end_date,layer_metadata["features"]))


    finally:
        utils.remove_folder(work_folder)
        pass
            
def restore_by_month(year,month,restore_to_origin_table=False,preserve_id=True):
    """
    Restore the loggedpoint from archived files for the month
    restore_to_origin_table: if true, restore the data to table tracking_loggedpoint; otherwise restore the data into a table with layer name
    preserve_id: meaningful if restore_to_origin_table is True.
    """
    d = date(year,month,1)
    archive_group = get_archive_group(d)
    logger.info("Begin to import archived loggedpoint, archive_group={}".format(archive_group))
    blob_resource = get_blob_resource()
    work_folder = tempfile.mkdtemp(prefix="restore_loggedpoint")
    try:
        metadata,filename = blob_resource.download_resources(resource_group=archive_group,folder=work_folder,overwrite=True)
        if metadata:
            imported_table = _restore_data(os.path.join(work_folder,get_vrt_id(archive_group)),restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)
        logger.info("End to import archived loggedpoint, archive_group={},imported_table = {}".format(archive_group,imported_table))
    finally:
        utils.remove_folder(work_folder)
        pass


def restore_by_date(d,restore_to_origin_table=False,preserve_id=True):
    """
    Restore the loggedpoint from archived files for the day
    restore_to_origin_table: if true, restore the data to table tracking_loggedpoint; otherwise restore the data into a table with layer name
    preserve_id: meaningful if restore_to_origin_table is True.
    """
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    resource_id = "{}.gpkg".format(archive_id)
    logger.info("Begin to import archived loggedpoint, archive_group={},archive_id={}".format(archive_group,archive_id))
    blob_resource = get_blob_resource()
    work_folder = tempfile.mkdtemp(prefix="restore_loggedpoint")
    try:
        metadata,filename = blob_resource.download_resource(archive_group,resource_id,filename=os.path.join(work_folder,resource_id))
        imported_table =_restore_data(filename,restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)
        logger.info("End to import archived loggedpoint, archive_group={},archive_id={},imported_table={}".format(archive_group,archive_id,imported_table))
    finally:
        utils.remove_folder(work_folder)
        pass

def _restore_data(filename,restore_to_origin_table=False,preserve_id=True):
    """
    Restore the loggedpoint from the archived files
    restore_to_origin_table: if true, restore the data to table tracking_loggedpoint; otherwise restore the data into a table with layer name
    preserve_id: meaningful if restore_to_origin_table is True.
    """
    db = settings.DATABASE
    imported_table = db.import_spatial_data(filename)

    if restore_to_origin_table:
        #insert the missing device
        logger.debug("Create the missing devices from imported table({0})".format(imported_table))
        sql = missing_device_sql.format(imported_table)
        rows = db.update(sql,autocommit=True)
        if rows :
            logger.info("Created {2} missing devices from imported table({0})".format(imported_table,rows))
        else:
            logger.info("All devices referenced from imported table({0}) exist".format(imported_table,rows))

        logger.info("Restore the logged points from table({0}) to table(tracking_loggedpoint)".format(imported_table))
        if preserve_id:
            sql = restore_with_id_sql
        else:
            sql = restore_sql

        sql = sql.format(imported_table)
        rows = db.update(sql,autocommit=True)
        logger.info("{1} records are restored from from table({0}) to table(tracking_loggedpoint)".format(imported_table,rows))
        try:
            logger.debug("Try to drop the imported table({0})".format(imported_table))
            rows = db.executeDDL("DROP TABLE \"{}\"".format(imported_table))
            logger.debug("Dropped the imported table({0})".format(imported_table))
        except:
            logger.error("Failed to drop the temporary imported table to table({0}). {1}".format(imported_table,traceback.format_exc()))
            pass
        return "tracking_loggedpoint"

    else:
        return imported_table

def download_by_month(year,month,folder=None,overwrite=False):
    """
    download the loggedpoint from archived files for the month
    """
    d = date(year,month,1)
    archive_group = get_archive_group(d)
    logger.info("Begin to download archived loggedpoint, archive_group={}".format(archive_group))
    blob_resource = get_blob_resource()
    folder = folder or tempfile.mkdtemp(prefix="loggedpoint{}".format(d.strftime("%Y-%m")))
    metadata,folder = blob_resource.download_resources(resource_group=archive_group,folder=folder,overwrite=overwrite)
    logger.info("End to download archived loggedpoint, archive_group={},downloaded_folder={}".format(archive_group,folder))

def download_by_date(d,folder=None,overwrite=False):
    """
    Download the loggedpoint from archived files for the day
    """
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    resource_id = "{}.gpkg".format(archive_id)
    logger.info("Begin to download archived loggedpoint, archive_group={},archive_id={}".format(archive_group,archive_id))
    blob_resource = get_blob_resource()
    folder = folder or tempfile.mkdtemp(prefix="loggedpoint{}".format(d.strftime("%Y-%m-%d")))
    metadata,filename = blob_resource.download_resource(archive_group,resource_id,filename=os.path.join(folder,resource_id),overwrite=overwrite)
    logger.info("End to download archived loggedpoint, archive_group={},archive_id={},dowloaded_file={}".format(archive_group,archive_id,filename))

def user_confirm(message,possible_answers,case_sensitive=False):
    """
    Ask the user's confirmation
    if case_sensitvie is False, turn the answer to upper case
    """
    answer = None
    while answer is None:
        answer = input(message)
        if not case_sensitive:
            answer = answer.upper()

        if answer not in possible_answers:
            answer = None

    return answer

def delete_all():
    """
    Delete all archived files from storage,
    must be used with caution
    """
    if settings.LOGGEDPOINT_ARCHIVE_DELETE_DISABLED:
        raise Exception("The feature to delete logged point arhive is disabled.")
    answer = user_confirm("Are you sure you want to delete all loggedpoint archives?(Y/N):",("Y","N"))
    if answer != 'Y':
        return

    blob_resource = get_blob_resource()
    blob_resource.delete_resource(throw_exception=False)

def delete_archive_by_month(year,month):
    """
    Delete the archived files for month from storage,
    must be used with caution
    """
    if settings.LOGGEDPOINT_ARCHIVE_DELETE_DISABLED:
        raise Exception("The feature to delete logged point arhive is disabled.")
    answer = user_confirm("Are you sure you want to delete the loggedpoint archives for the month({}/{})?(Y/N):".format(year,month),("Y","N"))
    if answer != 'Y':
        return
    d = date(year,month,1)
    archive_group = get_archive_group(d)
    blob_resource = get_blob_resource()
    blob_resource.delete_resource(resource_group=archive_group,throw_exception=False)

def delete_archive_by_date(d):
    """
    Delete archived files for the day from storage,
    must be used with caution
    """
    if settings.LOGGEDPOINT_ARCHIVE_DELETE_DISABLED:
        raise Exception("The feature to delete logged point arhive is disabled.")
    answer = user_confirm("Are you sure you want to delete the loggedpoint archives for the day({})?(Y/N):".format(d),("Y","N"))
    if answer != 'Y':
        return
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    resource_id = "{}.gpkg".format(archive_id)
    vrt_id = get_vrt_id(archive_group)

    work_folder = None
    blob_resource = get_blob_resource()
    try:
        del_metadata = blob_resource.delete_resource(resource_group=archive_group,resource_id=resource_id)
        groupmetadatas = [m for m in blob_resource.metadata_client.resource_metadatas(resource_group=archive_group,throw_exception=True)]

        vrt_metadata = next(m for m in groupmetadatas if m["resource_id"] == vrt_id)

        vrt_metadata["features"] = 0
        for m in groupmetadatas:
            if m["resource_id"] == vrt_id:
                continue
            vrt_metadata["features"] += m["features"]

        layers =  [(m["resource_id"],m["resource_file"]) for m in groupmetadatas if m["resource_id"] != vrt_id]
        if layers:
            work_folder = tempfile.mkdtemp(prefix="delete_archive")
            layers.sort(key=lambda o:o[0])
            layers = os.linesep.join(individual_layer.format(m[0],m[1]) for m in layers )
            vrt_data = vrt.format(archive_group,layers)
            vrt_filename = os.path.join(work_folder,"loggedpoint.vrt")
            with open(vrt_filename,"w") as f:
                f.write(vrt_data)

            vrt_metadata["file_md5"] = utils.file_md5(vrt_filename)
            resourcemetadata = blob_resource.push_file(vrt_filename,vrt_metadata,f_post_push=_set_end_datetime("updated"))
        else:
            #all archives in the group were deleted
            blob_resource.delete_resource(resourceid=vrt_id,resource_group=archive_group)
    finally:
        utils.remove_folder(work_folder)
        pass


