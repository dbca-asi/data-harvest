from datetime import datetime

from data_storage import IndexedGroupHistoryDataRepository,LocalStorage

import azlog
from . import settings

def get_metaname_func():
    resource_name = settings.RESOURCE_NAME.lower()
    return lambda resource_group:"{}_{}".format(resource_name,resource_group.rsplit("-",1)[0])
get_metaname = get_metaname_func()

def get_earliest_metaname(resource_id):
    diff_months = settings.ARCHIVE_LIFESPAN % 12
    diff_years = int(settings.ARCHIVE_LIFESPAN / 12)

    d = datetime.strptime(resource_id[0],"%Y-%m-%d")
    d = d.replace(year=d.year - diff_years,day=1)
    earliest_month = d.month - diff_months
    if earliest > 0:
        d = d.replace(month=earliest_month)
    else:
        d = d.replace(year=d.year - 1,month=12 + earliest_month)

class NginxLogArchive(azlog.Archive):
    #function to get the archive group name from archive date
    get_resource_group = lambda d:d.strftime("%Y-%m-%d")
    

    def create_resource_repository(self):
        return IndexedGroupHistoryDataRepository(
            LocalStorage(settings.LOCAL_STORAGE_DIR),
            settings.RESOURCE_NAME,
            get_metaname,
            index_metaname=self.index_metaname,
            f_earliest_metaname=None if settings.ARCHIVE_LIFESPAN is None or settings.ARCHIVE_LIFESPAN <= 0 else get_earliest_metaname
        )

    def set_metadata(self,metadata):
        resource_group = get_resource_group(query_start)
        metadata["resource_group"] = get_resource_group(metadata[self.ARCHIVE_STARTTIME])
