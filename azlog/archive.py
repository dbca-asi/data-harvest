import os
import traceback
import json
import logging
import tempfile
import subprocess
from datetime import date,timedelta


from utils import timezone,remove_file,acquire_runlock

from data_storage import IndexedGroupHistoryDataRepository,LocalStorage
from data_storage.exceptions import ResourceAlreadyExist

from . import settings

logger = logging.getLogger(__name__)

index_metaname = "{}_index".format(settings.RESOURCE_NAME.lower())
get_metaname = lambda resource_group:"{}_{}".format(settings.RESOURCE_NAME.lower(),resource_group.split("-")[0])

#function to get the archive group name from archive date
get_resource_group = lambda d:d.strftime("%Y-%m-%d")
#function to get the archive id from date from archive date
resource_id_pattern = "{}_%Y-%m-%dT%H-%M-%S.json".format(settings.RESOURCE_NAME.lower())
get_resource_id= lambda d:d.strftime(resource_id_pattern)

_resource_repository = None
def get_resource_repository():
    """
    Return the blob resource client
    """
    global _resource_repository
    if _resource_repository is None:
        _resource_repository = IndexedGroupHistoryDataRepository(
            LocalStorage(settings.LOCAL_STORAGE_DIR),
            settings.RESOURCE_NAME,
            get_metaname,
            index_metaname=index_metaname
        )
    return _resource_repository

ARCHIVE_STARTTIME="archive_starttime"
ARCHIVE_ENDTIME="archive_endtime"

def get_query_interval():
    """
    Return the current query interval(start,end) based on the last archiving;
    return (start,None) if all logs are archived

    """
    last_resource = get_resource_repository().last_resource
    if last_resource:
        query_start = last_resource[1][ARCHIVE_ENDTIME]
    else:
        query_start = settings.QUERY_START
    
    query_end = query_start + settings.QUERY_DURATION
    
    if query_end > timezone.now():
        return (query_start,None)
    else:
        return (query_start,query_end)


def archive(max_archive_times=settings.MAX_ARCHIVE_TIMES_PER_RUN):
    """
    Continuous archiving the az log.
    max_archive_times: the maxmium times to arhive
    """
    acquire_runlock(settings.PROCESS_LOCKFILE)
    logger.info("Begin to continuous archive az logs, max_archive_times={}".format(max_archive_times))
    archived_times = 0
    while max_archive_times is None or archived_times < max_archive_times:
        if not _archive():
            break
        archived_times += 1

def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

def _archive():
    """
    Archive the az logs
    return True if some az logs are archived; False if nothing is archived
    """
    query_start,query_end = get_query_interval()
    if not query_end:
        logger.info("All az logs have been archived.the end time of the last archiving is {}".format(query_start))
        return False

    resource_group = get_resource_group(query_start)
    resource_id = get_resource_id(query_start)
    metadata = {
        "start_archive":timezone.now(),
        "resource_id":resource_id,
        "resource_group":resource_group,
        ARCHIVE_STARTTIME:query_start,
        ARCHIVE_ENDTIME:query_end
    }

    try:
        dump_file = None
        with tempfile.NamedTemporaryFile(suffix=".json",prefix=settings.RESOURCE_NAME,delete=False) as f:
            dump_file = f.name
        
        cmd = "az login -u {} -p {}&&az monitor log-analytics query -w {} --analytics-query '{}' -t {}/{} > {}".format(
            settings.USER,
            settings.PASSWORD,
            settings.WORKSPACE,
            settings.QUERY,
            timezone.utctime(query_start).strftime("%y-%m-%dT%H:%M:%SZ"),
            timezone.utctime(query_end).strftime("%y-%m-%dT%H:%M:%SZ"),
            dump_file
        )
        subprocess.check_output(cmd,shell=True)

        if settings.CHECK_DUMP_FILE:
            with open(dump_file,'r') as f:
                data = json.loads(f.read())
        resourcemetadata = get_resource_repository().push_file(dump_file,metadata,f_post_push=_set_end_datetime("end_archive"))

        return True
    finally:
        remove_file(dump_file)
    
