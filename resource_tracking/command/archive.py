import argparse
from datetime import date,datetime
import sys

from resource_tracking import archive

now = datetime.now()
today = now.date()
year = now.year

parser = argparse.ArgumentParser(prog="restore",description='Archive the logged points')
parser.add_argument('year', type=int, action='store',choices=[y for y in range(year - 30,year + 1,1)],help='The year of the logged points')
parser.add_argument('month', type=int, action='store',choices=[m for m in range(1,13)],help='The month of the logged points')
parser.add_argument('day', type=int, action='store',choices=[d for d in range(1,32)],nargs="?",help='The day of the logged points')
parser.add_argument('--check',  action='store_true',help='Download the archived files to check whether it was archived successfully or not')
parser.add_argument('--delete', action='store_true',help='Delete the archived logged points from table after archiving')
parser.add_argument('--overwrite', action='store_true',help='Overwrite the existing archive file')
parser.add_argument('--backup-to-archive-table',dest="backup_to_archive_table", action='store_true',help='Backup the archived data into a yearly based table')


def run():
    args = parser.parse_args(sys.argv[2:])
    d = date(args.year,args.month, args.day if args.day else 1)
    if d >= today:
        raise Exception("Can only archive logged points happened before today.")
    if args.day:
        #archive by date
        archive.archive_by_date(
            d,
            delete_after_archive=args.delete,
            check=args.check,
            overwrite=args.overwrite,
            backup_to_archive_table=args.backup_to_archive_table)
    else:
        #archive by month
        archive.archive_by_month(
            d.year,
            d.month,
            delete_after_archive=args.delete,
            check=args.check,
            overwrite=args.overwrite,
            backup_to_archive_table=args.backup_to_archive_table)



