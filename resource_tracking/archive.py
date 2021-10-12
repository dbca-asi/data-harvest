import os
import traceback
import logging
import tempfile
from datetime import date,timedelta,datetime


from utils import timezone,gdal
import utils

from data_storage import IndexedGroupResourceRepository,AzureBlobStorage
from data_storage.exceptions import ResourceAlreadyExist

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
archive_sql = """
SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,
    extract(epoch from a.seen)::bigint as seen,
    case when b.id is null then a.device_id::varchar(32) else b.deviceid end as deviceid,
    case when b.id is null then 'N/A'::varchar(32) else b.registration end as registration
FROM {0} a LEFT JOIN tracking_device b ON a.device_id = b.id 
WHERE a.seen >= '{1}' AND a.seen < '{2}'
"""
archive_from_archive_table_sql = """
SELECT id,point,heading,velocity,altitude,message,source_device_type,raw,
    extract(epoch from seen)::bigint as seen,
    deviceid,registration
FROM {0} 
WHERE seen >= '{1}' AND seen < '{2}'
"""


create_backup_table_sql = """
SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,a.seen,b.deviceid,b.registration 
INTO \"{0}\" 
FROM tracking_loggedpoint a JOIN tracking_device b ON a.device_id = b.id WHERE false;
create index "{0}_seen" on "{0}" (seen);
create index "{0}_deviceid_seen" on "{0}" (deviceid,seen);
"""
backup_sql = """
INSERT INTO "{2}" (id,point,heading,velocity,altitude,message,source_device_type,raw,seen,deviceid,registration) 
SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,a.seen,
    case when b.deviceid is null then a.device_id::varchar(32) else b.deviceid end as deviceid,
    case when b.id is null then 'N/A'::varchar(32) else b.registration end as registration
FROM tracking_loggedpoint a LEFT JOIN tracking_device b ON a.device_id = b.id WHERE a.seen >= '{0}' AND a.seen < '{1}'"""
delete_backup_sql = """DELETE FROM "{2}" WHERE seen >= '{0}' AND seen < '{1}'"""
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

get_metaname = """lambda archive_group:"loggedpoint{}".format(archive_group.split("-")[0])"""
_resource_repository = None
def get_resource_repository():
    """
    Return the blob resource client
    """
    global _resource_repository
    if _resource_repository is None:
        _resource_repository = IndexedGroupResourceRepository(
            AzureBlobStorage(settings.AZURE_CONNECTION_STRING,settings.AZURE_CONTAINER),
            settings.LOGGEDPOINT_RESOURCE_NAME,
            get_metaname,
            archive=False,
            index_metaname=index_metaname
        )
    return _resource_repository

def continuous_archive(delete_after_archive=False,check=False,max_archive_days=None,overwrite=False,backup_to_archive_table=True,rearchive=False):
    """
    Continuous archiving the loggedpoint.
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    max_archive_days: the maxmium days to arhive
    overwrite: if true, overwrite the existing archived file;if false, throw exception if already archived 
    rearchive: if true, rearchive the existing archived file;if false, throw exception if already archived 
    """
    db = settings.DATABASE
    earliest_date = db.get(earliest_archive_date)[0]
    if earliest_date is None:
        logger.info("No more data to archive")
        return

    earliest_date = timezone.nativetime(earliest_date).date()
    if timezone.in_working_hour():
        logger.error("Please don't run continuous archive in working hour")
        return 
    today = timezone.now().date()
    last_archive_date = today - timedelta(days=settings.LOGGEDPOINT_ACTIVE_DAYS)
    archive_date = earliest_date
    archived_days = 0
    max_archive_days = max_archive_days if max_archive_days and  max_archive_days > 0 else None

    logger.info("Begin to continuous archive loggedpoint, earliest archive date={0},last archive date = {1}, delete_after_archive={2}, check={3}, max_archive_days={4}".format(
        earliest_date,last_archive_date,delete_after_archive,check,max_archive_days
    ))
    if archive_date >= last_archive_date:
        logger.info("No more data to archive")
        return

    while archive_date < last_archive_date and (not max_archive_days or archived_days < max_archive_days):
        if timezone.in_working_hour():
            logger.info("Stop archiving in working hour")
            break

        archive_by_date(archive_date,delete_after_archive=delete_after_archive,check=check,overwrite=overwrite,rearchive=rearchive,backup_to_archive_table=backup_to_archive_table)
        archive_date += timedelta(days=1)
        archived_days += 1

def archive_by_month(year,month,delete_after_archive=False,check=False,overwrite=False,backup_to_archive_table=True,rearchive=False):
    """
    Archive the logged point for the month.
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    overwrite: if true, overwrite the existing archived file;if false, throw exception if already archived 
    rearchive: if true, rearchive the existing archived file;if false, throw exception if already archived 
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
        archive_by_date(archive_date,delete_after_archive=delete_after_archive,check=check,overwrite=overwrite,rearchive=rearchive,backup_to_archive_table=backup_to_archive_table)
        archive_date += timedelta(days=1)

def archive_by_date(d,delete_after_archive=False,check=False,overwrite=False,backup_to_archive_table=True,rearchive=False):
    """
    Archive the logged point within the specified date
    delete_after_archive: delete the archived data from table tracking_loggedpoint
    check: check whether archiving is succeed or not
    overwrite: if true, overwrite the existing archived file;if false, throw exception if already archived 
    rearchive: if true, rearchive the existing archived file;if false, throw exception if already archived 
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
    return archive(archive_group,archive_id,start_date,end_date,delete_after_archive=delete_after_archive,check=check,overwrite=overwrite,rearchive=rearchive,backup_table=backup_table)


def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

def archive(archive_group,archive_id,start_date,end_date,delete_after_archive=False,check=False,overwrite=False,backup_table=None,rearchive=False,source_table="tracking_loggedpoint"):
    """
    Archive the resouce tracking history by start_date(inclusive), end_date(exclusive)
    archive_id: a unique identity of the archive file. that means different start_date and end_date should have a different archive_id
    overwrite: False: raise exception if archive_id already exists; True: overwrite the existing archive file
    rearchive: if true, rearchive the existing archived file;if false, throw exception if already archived 
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

    if rearchive:
        overwrite = True

    filename = None
    vrt_filename = None
    work_folder = tempfile.mkdtemp(prefix="archive_loggedpoint")
    resourcemetadata = None
    try:
        logger.info("Begin to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        resource_repository = get_resource_repository()
        sql = archive_sql.format(source_table,start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
        if db.count(sql) == 0:
            #no data to archive
            if resource_repository.is_exist(archive_group,resource_id):
                logger.info("The loggedpoint has already been archived. archive_id={0},start_archive_date={1},end_archive_date={2}".format(archive_id,start_date,end_date))
            else:
                logger.info("No loggedpoints to archive, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
            return

        if resource_repository.is_exist(archive_group,resource_id):
            #already archived, restore the data
            if not overwrite:
                #in normal mode
                raise ResourceAlreadyExist("The loggedpoint has already been archived. archive_id={0},start_archive_date={1},end_archive_date={2}".format(archive_id,start_date,end_date))
            elif rearchive:
                #in rearchive mode. restore the data to original table
                logger.info("In rearchive mode, The resource '{}' in blob storage will be restored and archived again".format(resource_id))
                logger.debug("Begin to restore the data({0}) from blob storage to table 'tracking_loggedpoint'".format(resource_id))
                restore_by_archive(archive_group,archive_id,restore_to_origin_table=True,preserve_id=True)
                logger.debug("End to restore the data({0}) from blob storage to table 'tracking_loggedpoint'".format(resource_id))
                if db.is_table_exist(backup_table):
                    logger.debug("Begin to delete the data from backup table '{}'".format(backup_table))
                    count = db.update(delete_backup_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern),backup_table))
                    logger.debug("End to delete {1} features from backup table {0}".format(backup_table,count))
            else:
                #in overwrite mode.
                logger.info("In overwrite mode, The resource '{}' in blob storage will be overwrided".format(resource_id))

        #export the archived data as geopackage
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
        resourcemetadata = resource_repository.push_file(filename,metadata,f_post_push=_set_end_datetime("end_archive"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether loggedpoint archive file was pushed to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_metadata,d_filename = resource_repository.download_resource(archive_group,resource_id,filename=os.path.join(work_folder,"loggedpoint_download.gpkg"))
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
            vrt_metadata = next(m for m in groupmetadata.values() if m.get("resource_id") == vrt_id)
        except StopIteration as ex:
            vrt_metadata = {"resource_id":vrt_id,"resource_file":vrt_id,"resource_group":archive_group}

        vrt_metadata["features"] = 0
        for m in groupmetadata.values():
            if not m.get("resource_id") or m.get("resource_id") == vrt_id:
                continue
            vrt_metadata["features"] += m["features"]

        layers =  [(m["layer"],m["resource_file"]) for m in groupmetadata.values() if m.get("resource_id") and m.get("resource_id") != vrt_id]
        layers.sort(key=lambda o:o[0])
        layers = os.linesep.join(individual_layer.format(m[0],m[1]) for m in layers )
        vrt_data = vrt.format(get_vrt_layername(archive_group),layers)
        vrt_filename = os.path.join(work_folder,"loggedpoint.vrt")
        with open(vrt_filename,"w") as f:
            f.write(vrt_data)

        vrt_metadata["file_md5"] = utils.file_md5(vrt_filename)

        resourcemetadata = resource_repository.push_file(vrt_filename,vrt_metadata,f_post_push=_set_end_datetime("updated"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether the group vrt file was pused to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_vrt_metadata,d_vrt_filename = resource_repository.download_resource(archive_group,vrt_id,filename=os.path.join(work_folder,"loggedpoint_download.vrt"))
            d_vrt_file_md5 = utils.file_md5(d_vrt_filename)
            if vrt_metadata["file_md5"] != d_vrt_file_md5:
                raise Exception("Upload vrt file failed.source file's md5={}, uploaded file's md5={}".format(vrt_metadata["file_md5"],d_vrt_file_md5))

        if backup_table:
            if not db.is_table_exist(backup_table):
                #table doesn't exist, create the table and indexes
                sql = create_backup_table_sql.format(backup_table)
                db.executeDDL(sql)

            sql = backup_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern),backup_table)
            count = db.update(sql)
            if count == layer_metadata["features"]:
                logger.debug("Backup {1} features to backup table {0},sql={2}".format(backup_table,count,sql))
            else:
                raise Exception("Only backup {1}/{2} features to backup table {0}".format(backup_table,count,layer_metadata["features"]))

        if delete_after_archive:
            logger.debug("Begin to delete archived data, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))

            delete_sql = del_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
            deleted_rows = db.update(delete_sql)
            logger.debug("Delete {} rows from table tracking_loggedpoint, archive_group={},archive_id={},start_date={},end_date={};sql={}".format(
                deleted_rows,archive_group,archive_id,start_date,end_date,delete_sql
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
    resource_repository = get_resource_repository()
    work_folder = tempfile.mkdtemp(prefix="restore_loggedpoint")
    try:
        metadata,filename = resource_repository.download_resources(resource_group=archive_group,folder=work_folder,overwrite=True)
        if metadata:
            imported_table,restored_rows = _restore_data(os.path.join(work_folder,archive_group,get_vrt_id(archive_group)),restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)

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
    return restore_by_archive(archive_group,archive_id,restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)

def restore_by_archive(archive_group,archive_id,restore_to_origin_table=False,preserve_id=True):
    """
    Restore the loggedpoint from archived files with archive group and archive id
    restore_to_origin_table: if true, restore the data to table tracking_loggedpoint; otherwise restore the data into a table with layer name
    preserve_id: meaningful if restore_to_origin_table is True.
    """
    resource_id = "{}.gpkg".format(archive_id)
    logger.info("Begin to import archived loggedpoint, archive_group={},archive_id={}".format(archive_group,archive_id))
    resource_repository = get_resource_repository()
    work_folder = tempfile.mkdtemp(prefix="restore_loggedpoint")
    try:
        metadata,filename = resource_repository.download_resource(archive_group,resource_id,filename=os.path.join(work_folder,resource_id))
        imported_table,restored_rows =_restore_data(filename,restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)
        if metadata["features"] != restored_rows:
            raise Exception("The archive(archive_group={0},archive_id={1}) has {2} rows, but only {4} rows are restored to table '{3}'.".format(archive_group,archive_id,metadata["features"],imported_table,restored_rows))
        logger.info("End to import {} archived loggedpoint, archive_group={},archive_id={},imported_table={}".format(restored_rows,archive_group,archive_id,imported_table))
        return (imported_table,restored_rows)
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
    restored_rows = db.count(imported_table)

    if restore_to_origin_table:
        #insert the missing device
        logger.debug("Create the missing devices from imported table({0})".format(imported_table))
        sql = missing_device_sql.format(imported_table)
        rows = db.update(sql,autocommit=True)
        if rows :
            logger.info("Created {1} missing devices from imported table({0})".format(imported_table,rows))
        else:
            logger.info("All devices referenced from imported table({0}) exist".format(imported_table))

        logger.info("Restore the logged points from table({0}) to table(tracking_loggedpoint)".format(imported_table))
        if preserve_id:
            sql = restore_with_id_sql
        else:
            sql = restore_sql

        sql = sql.format(imported_table)
        rows = db.update(sql,autocommit=True)
        if rows == restored_rows:
            logger.debug("Restored {1} rows from file '{0}' to table 'tracking_loggedpoint'".format(filename,restored_rows))
        else:
            raise Exception("Restored {1} rows from file '{0}', but only {2} rows are restored to table 'tracking_loggedpoint'".format(filename,restored_rows,rows))

        try:
            logger.debug("Try to drop the imported table({0})".format(imported_table))
            rows = db.executeDDL("DROP TABLE \"{}\"".format(imported_table))
            logger.debug("Dropped the imported table({0})".format(imported_table))
        except:
            logger.error("Failed to drop the temporary imported table to table({0}). {1}".format(imported_table,traceback.format_exc()))
            pass
        return ("tracking_loggedpoint",restored_rows)

    else:
        return (imported_table,restored_rows)

def download_by_month(year,month,folder=None,overwrite=False):
    """
    download the loggedpoint from archived files for the month
    """
    d = date(year,month,1)
    archive_group = get_archive_group(d)
    logger.info("Begin to download archived loggedpoint, archive_group={}".format(archive_group))
    resource_repository = get_resource_repository()
    folder = folder or tempfile.mkdtemp(prefix="loggedpoint{}".format(d.strftime("%Y-%m")))
    metadata,folder = resource_repository.download_resources(resource_group=archive_group,folder=folder,overwrite=overwrite)
    logger.info("End to download archived loggedpoint, archive_group={},downloaded_folder={}".format(archive_group,folder))

def download_by_date(d,folder=None,overwrite=False):
    """
    Download the loggedpoint from archived files for the day
    """
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    resource_id = "{}.gpkg".format(archive_id)
    logger.info("Begin to download archived loggedpoint, archive_group={},archive_id={}".format(archive_group,archive_id))
    resource_repository = get_resource_repository()
    folder = folder or tempfile.mkdtemp(prefix="loggedpoint{}".format(d.strftime("%Y-%m-%d")))
    metadata,filename = resource_repository.download_resource(archive_group,resource_id,filename=os.path.join(folder,resource_id),overwrite=overwrite)
    file_md5 = utils.file_md5(filename)
    if metadata["file_md5"] != file_md5:
        raise Exception("Download loggedpoint archive file failed.source file's md5={}, downloaded file's md5={}".format(metadata["file_md5"],file_md5))

    layer_metadata = gdal.get_layers(filename)[0]
    if metadata["features"] != layer_metadata["features"]:
        raise Exception("Download loggedpoint archive file failed.source file's features={}, downloaded file's features={}".format(metadata["features"],layer_metadata["features"]))
        
    logger.info("End to download archived loggedpoint, archive_group={},archive_id={},dowloaded_file={},features={}".format(archive_group,archive_id,filename,metadata["features"]))
    return (metadata,filename)

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

    resource_repository = get_resource_repository()
    resource_repository.delete_resources(throw_exception=False)

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
    resource_repository = get_resource_repository()
    resource_repository.delete_resources(resource_group=archive_group,throw_exception=False)

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
    resource_repository = get_resource_repository()
    try:
        del_metadata = resource_repository.delete_resource(archive_group,resource_id)
        groupmetadatas = [m for m in resource_repository.metadata_client.resource_metadatas(resource_group=archive_group,throw_exception=True)]

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
            resourcemetadata = resource_repository.push_file(vrt_filename,vrt_metadata,f_post_push=_set_end_datetime("updated"))
        else:
            #all archives in the group were deleted
            resource_repository.delete_resource(archive_group,vrt_id)
    finally:
        utils.remove_folder(work_folder)
        pass



def rearchive_from_archive_table(filename,check=False,backup_folder=None,limit=None,max_diff=100):
    filename = os.path.abspath(filename)
    base,ext = os.path.splitext(filename)
    finish_filename = "{}.finish{}".format(*os.path.splitext(filename))
    backup_filename = "{}.backup{}".format(*os.path.splitext(filename))
    finished = set()
    backuped = set()
    limit = 999999999 if limit <= 0 else limit
    if os.path.exists(finish_filename):
        with open(finish_filename,"r") as f:
            d = f.readline()
            while d:
                d = d.strip()
                if d:
                    try:
                        pos = d.index("#")
                        d = d[:pos].strip()
                        datetime.strptime(d,"%Y-%m-%d")
                    except:
                        pass
                    finished.add(d)

                d = f.readline()

    if backup_folder and os.path.exists(backup_filename):
        with open(backup_filename,"r") as f:
            d = f.readline()
            while d:
                d = d.strip()
                if d:
                    try:
                        pos = d.index("#")
                        d = d[:pos].strip()
                        datetime.strptime(d,"%Y-%m-%d")
                    except:
                        pass
                    backuped.add(d)
                d = f.readline()
     
    archived_files = 0
    with open(filename,"r") as rf:
        d = rf.readline()
        while d:
            try:
                d = d.strip()
                if not d:
                    continue

                dt = datetime.strptime(d,"%Y-%m-%d").date()
                if d not in backuped and backup_folder:
                    metadata,filename = download_by_date(dt,backup_folder,overwrite=True)
                    with open(backup_filename,"a") as bf:
                        bf.write("{} #resource_group={}, resource_id={}, file={} , features={}".format(d,metadata["resource_group"],metadata["resource_id"],filename,metadata["features"]))
                        bf.write("\n")
                else:
                    print("{} already backuped".format(d))

                if d not in finished:
                    try:
                        rearchive_metadata = rearchive_from_archive_table_by_date(dt,check=check,max_diff=max_diff)
                        with open(finish_filename,"a") as ff:
                            ff.write("{} #resource_group={}, resource_id={}, features={}".format(d,rearchive_metadata["resource_group"],rearchive_metadata["resource_id"],rearchive_metadata["features"]))
                            ff.write("\n")
                    except Exception as ex:
                        print(str(ex))
                        with open(finish_filename,"a") as ff:
                            ff.write("{} #Ignored. exception={}".format(d,str(ex)))
                            ff.write("\n")
                    archived_files += 1
                    if archived_files >= limit:
                        break
                else:
                    print("{} already rearchived".format(d))

            finally:
                d = rf.readline()

def rearchive_from_archive_table_by_date(d,check=False,backup_folder=None,max_diff=100):
    """
    Archive the resouce tracking history from archive table by start_date(inclusive), end_date(exclusive)
    check: check whether archiving is succeed or not
    """
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    start_date = timezone.datetime(d.year,d.month,d.day)
    end_date = start_date + timedelta(days=1)
    backup_table = get_backup_table(d) 

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
    resourcemetadata = None
    try:
        logger.info("Begin to rearchive loggedpoint from archive table '{}', archive_group={},archive_id={},start_date={},end_date={}".format(backup_table,archive_group,archive_id,start_date,end_date))
        resource_repository = get_resource_repository()
        try:
            res_metadata = resource_repository.get_resource_metadata(archive_group,resource_id)
            archived_count = res_metadata["features"]
        except:
            archived_count = 0
        
        sql = archive_from_archive_table_sql.format(backup_table,start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
        #export the archived data as geopackage
        export_result = db.export_spatial_data(sql,filename=os.path.join(work_folder,"loggedpoint.gpkg"),layer=archive_id)
        if not export_result:
            #no data to archive
            if archived_count:
                logger.info("The loggedpoint has already been archived. archive_id={0},start_archive_date={1},end_archive_date={2}".format(archive_id,start_date,end_date))
            else:
                logger.info("No loggedpoints to archive, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
            return

        if archived_count:
            if backup_folder:
                download_by_date(d,backup_folder)

        layer_metadata,filename = export_result
        if max_diff and abs(archived_count - layer_metadata["features"]) > max_diff:
            raise Exception("The difference({}) between the archived features({}) and the rearchived features({}) is greater than the max difference({})".format(
                abs(archived_count - layer_metadata["features"]),
                archived_count , 
                layer_metadata["features"],
                max_diff
            ))

        layer_metadata,filename = export_result
        metadata["file_md5"] = utils.file_md5(filename)
        metadata["layer"] = layer_metadata["layer"]
        metadata["features"] = layer_metadata["features"]
        #upload archive file
        logger.debug("Begin to push loggedpoint archive file to blob storage, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        resourcemetadata = resource_repository.push_file(filename,metadata,f_post_push=_set_end_datetime("end_archive"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether loggedpoint archive file was pushed to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_metadata,d_filename = resource_repository.download_resource(archive_group,resource_id,filename=os.path.join(work_folder,"loggedpoint_download.gpkg"))
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

        resourcemetadata = resource_repository.push_file(vrt_filename,vrt_metadata,f_post_push=_set_end_datetime("updated"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether the group vrt file was pused to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_vrt_metadata,d_vrt_filename = resource_repository.download_resource(archive_group,vrt_id,filename=os.path.join(work_folder,"loggedpoint_download.vrt"))
            d_vrt_file_md5 = utils.file_md5(d_vrt_filename)
            if vrt_metadata["file_md5"] != d_vrt_file_md5:
                raise Exception("Upload vrt file failed.source file's md5={}, uploaded file's md5={}".format(vrt_metadata["file_md5"],d_vrt_file_md5))

        logger.info("End to archive loggedpoint from archive table '{}', archive_group={},archive_id={},start_date={},end_date={},archived features={}".format(backup_table,archive_group,archive_id,start_date,end_date,layer_metadata["features"]))
        return metadata


    finally:
        utils.remove_folder(work_folder)
        pass
            
