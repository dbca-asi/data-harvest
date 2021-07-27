import argparse
from datetime import date,datetime
import sys
import logging
import traceback

from resource_tracking import archive

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog="continuous_archive",description='Continuous archiving the logged points and push it to blob storage')
parser.add_argument('--check',  action='store_true',help='Download the archived files to check whether it was archived successfully or not')
parser.add_argument('--delete', action='store_true',help='Delete the archived logged points from table after archiving')
parser.add_argument('--max-archive-days',dest="max_archive_days", type=int,action='store',help='Maximum days to archive')
parser.add_argument('--overwrite', action='store_true',help='Overwrite the existing archive file')
parser.add_argument('--rearchive', action='store_true',help='Rearchive the existing archive file')
parser.add_argument('--backup-to-archive-table',dest="backup_to_archive_table", action='store_true',help='Backup the archived data into a yearly based table, only useful if --delete is enabled')

def run():
    args = parser.parse_args(sys.argv[2:])
    #restore by date
    try:
        archive.continuous_archive(
                delete_after_archive=args.delete,
                check=args.check,
                max_archive_days=args.max_archive_days if args.max_archive_days and args.max_archive_days > 0 else None,
                overwrite=args.overwrite,
                rearchive=args.rearchive,
                backup_to_archive_table=args.backup_to_archive_table)
    except:
        logger.error(traceback.format_exc())



