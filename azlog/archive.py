import os
import traceback
import json
import logging
import tempfile
import subprocess
from datetime import date,timedelta


from utils import timezone,remove_file
from data_storage.utils import acquire_runlock

from data_storage.exceptions import ResourceAlreadyExist

logger = logging.getLogger(__name__)

def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

class Archive(object):
    ARCHIVE_STARTTIME="archive_starttime"
    ARCHIVE_ENDTIME="archive_endtime"

    index_metaname = "metadata_index"

    _resource_repository = None

    _instances = {}

    def __init__(self,settings):
        self.settings = settings
        self.resource_id_pattern = "{}_%Y-%m-%dT%H-%M-%S.json".format(self.settings.RESOURCE_NAME.lower())

    @classmethod
    def get_instance(cls,settings):
        if settings not in cls._instances:
            cls._instances[settings] = cls(settings)

        return cls._instances[settings]

    def get_resource_id(self,d):
        return d.strftime(self.resource_id_pattern)

    @property
    def resource_repository(self):
        """
        Return the blob resource client
        """
        if self._resource_repository is None:
            self._resource_repository = self.create_resource_repository()
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
            query_start = self.settings.QUERY_START
        
        query_end = query_start + self.settings.QUERY_DURATION
        
        if query_end < timezone.now() - self.settings.LOG_DELAY_TIME:
            return (query_start,query_end)
        else:
            return (query_start,None)


    def archive(self,max_archive_times=False):
        """
        Continuous archiving the az log.
        max_archive_times: the maxmium times to arhive, can be None or positive numbers
        Return the number of archived files
        """
        if max_archive_times is False or max_archive_times <= 0:
            max_archive_times = self.settings.MAX_ARCHIVE_TIMES_PER_RUN

        acquire_runlock(self.settings.PROCESS_LOCKFILE)
        logger.info("Begin to continuous archive az logs, max_archive_times={}".format(max_archive_times))
        archived_times = 0
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
            with tempfile.NamedTemporaryFile(suffix=".json",prefix=self.settings.RESOURCE_NAME,delete=False) as f:
                dump_file = f.name

            if self.settings.TENANT:
                login_cmd = "az login --service-principal -u {} -p {} --tenant {}".format(
                    self.settings.USER,
                    self.settings.PASSWORD,
                    self.settings.TENANT
                )
            else:
                login_cmd = "az login -u {} -p {}".format(
                    self.settings.USER,
                    self.settings.PASSWORD
                )
            
            cmd = "{}&&az monitor log-analytics query -w {} --analytics-query '{}' -t {}/{} > {}".format(
                login_cmd,
                self.settings.WORKSPACE,
                self.settings.QUERY,
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
    
