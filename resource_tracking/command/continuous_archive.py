import argparse
from datetime import date,datetime
import sys

from resource_tracking import archive

parser = argparse.ArgumentParser(prog="restore",description='Archive the logged points')
parser.add_argument('--check',  action='store_true',help='Download the archived files to check whether it was archived successfully or not')
parser.add_argument('--delete', action='store_true',help='Delete the archived logged points from table after archiving')
parser.add_argument('--max-archive-days',dest="max_archive_days", type=int,action='store',help='Maximum days to archive')
parser.add_argument('--overwrite', action='store_true',help='Overwrite the existing archive file')

def run():
    args = parser.parse_args(sys.argv[2:])
    #restore by date
    archive.continuous_archive(delete_after_archive=args.delete,check=args.check,max_archive_days=args.max_archive_days if args.max_archive_days and args.max_archive_days > 0 else None,overwrite=args.overwrite)



