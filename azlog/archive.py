import os
import traceback
import json
import logging
import tempfile
import subprocess
from datetime import date,timedelta


from utils import timezone,remove_file,acquire_runlock

from data_storage.exceptions import ResourceAlreadyExist

from . import settings

logger = logging.getLogger(__name__)

def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

class Archive(object):
    ARCHIVE_STARTTIME="archive_starttime"
    ARCHIVE_ENDTIME="archive_endtime"

    index_metaname = "{}_index".format(settings.RESOURCE_NAME.lower())

    #function to get the archive id from date from archive date
    resource_id_pattern = "{}_%Y-%m-%dT%H-%M-%S.json".format(settings.RESOURCE_NAME.lower())
    get_resource_id= lambda d:d.strftime(resource_id_pattern)

    _resource_repository = None

    _instance = None

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            cls._instance = cls()

        return cls._instance


    @property
    def resource_repository(self):
        """
        Return the blob resource client
        """
        if self._resource_repository is None:
            _resource_repository = self.create_resource_repository()
        return self._resource_repository

    def create_resource_repository(self):
        raise NotImplementedError("The method 'create_resource_repositoru' Not Implemented")
    
    def get_query_interval(self):
        """
        Return the current query interval(start,end) based on the last archiving;
        return (start,None) if all logs are archived
    
        """
        last_resource = self.resource_repository.last_resource
        if last_resource:
            query_start = last_resource[1][self.ARCHIVE_ENDTIME]
        else:
            query_start = settings.QUERY_START
        
        query_end = query_start + settings.QUERY_DURATION
        
        if query_end > timezone.now():
            return (query_start,None)
        else:
            return (query_start,query_end)


    def archive(self,max_archive_times=settings.MAX_ARCHIVE_TIMES_PER_RUN):
        """
        Continuous archiving the az log.
        max_archive_times: the maxmium times to arhive
        Return the number of archived files
        """
        acquire_runlock(settings.PROCESS_LOCKFILE)
        logger.info("Begin to continuous archive az logs, max_archive_times={}".format(max_archive_times))
        archived_files = 0
        while max_archive_times is None or archived_times < max_archive_times:
            if not self._archive():
                break
            archived_times += 1

        return archived_times

    def set_metadata(self,metadata):
        """
        set more items in metadata
        """
        pass

    def _archive(self):
        """
        Archive the az logs
        return True if some az logs are archived; False if nothing is archived
        """
        query_start,query_end = self.get_query_interval()
        if not query_end:
            logger.info("All az logs have been archived.the end time of the last archiving is {}".format(query_start))
            return False
    
        logger.info("Archive az logs between {} and {}".format(query_start,query_end))
        resource_id = self.get_resource_id(query_start)
        metadata = {
            "start_archive":timezone.now(),
            "resource_id":resource_id,
            self.ARCHIVE_STARTTIME:query_start,
            self.ARCHIVE_ENDTIME:query_end
        }
        self.set_metadata(metadata)
    
        try:
            dump_file = None
            with tempfile.NamedTemporaryFile(suffix=".json",prefix=settings.RESOURCE_NAME,delete=False) as f:
                dump_file = f.name
            
            cmd = "az login -u {} -p {}&&az monitor log-analytics query -w {} --analytics-query '{}' -t {}/{} > {}".format(
                settings.USER,
                settings.PASSWORD,
                settings.WORKSPACE,
                settings.QUERY,
                timezone.utctime(query_start).strftime("%Y-%m-%dT%H:%M:%SZ"),
                timezone.utctime(query_end).strftime("%Y-%m-%dT%H:%M:%SZ"),
                dump_file
            )
            subprocess.check_output(cmd,shell=True)
    
            with open(dump_file,'r') as f:
                data = json.loads(f.read())
                metadata["log_records"] = len(data)
            resourcemetadata = self.resource_repository.push_file(dump_file,metadata,f_post_push=_set_end_datetime("end_archive"))
    
            return True
        finally:
            remove_file(dump_file)
    
