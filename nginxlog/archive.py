from datetime import datetime,timedelta

from data_storage import IndexedGroupHistoryDataRepository,LocalStorage
from data_storage.utils import timezone

import azlog
from . import settings

get_metaname_code = """
def get_metaname(resource_group):
    from datetime import datetime,timedelta
    from data_storage.utils import timezone
    group_date = timezone.nativetime(datetime.strptime(resource_group,"%Y-%m-%d"))
    weekday = group_date.weekday()
    if weekday == 0:
        meta_date = group_date
    else:
        meta_date = group_date - timedelta(days=weekday)
    return "metadata_{}".format(meta_date.strftime("%Y-%m-%d"))
"""
exec(get_metaname_code)

def get_earliest_metaname(resource_id):
    d = timezone.nativetime(datetime.strptime(resource_id[0],"%Y-%m-%d"))
    weekday = d.weekday()
    if weekday == 0:
        meta_date = d
    else:
        meta_date = d - timedelta(days=weekday)
    earliest_meta_date = meta_date - timedelta(days=7*settings.ARCHIVE_LIFESPAN)

    return "metadata_{}".format(earliest_meta_date.strftime("%Y-%m-%d"))

class NginxLogArchive(azlog.Archive):
    #function to get the archive group name from archive date
 
    def get_resource_group(self,d):
        return d.strftime("%Y-%m-%d")


    def create_resource_repository(self):
        return IndexedGroupHistoryDataRepository(
            LocalStorage(settings.LOCAL_STORAGE_DIR),
            settings.RESOURCE_NAME,
            get_metaname_code,
            index_metaname=self.index_metaname,
            f_earliest_metaname=None if settings.ARCHIVE_LIFESPAN is None or settings.ARCHIVE_LIFESPAN <= 0 else get_earliest_metaname
        )

    def set_metadata(self,metadata):
        metadata["resource_group"] = self.get_resource_group(metadata[self.ARCHIVE_STARTTIME])
