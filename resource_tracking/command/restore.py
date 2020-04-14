import argparse
from datetime import date,datetime
import sys

from resource_tracking import archive

now = datetime.now()
today = now.date()
year = now.year

parser = argparse.ArgumentParser(prog="restore",description='Restore the logged points from archive')
parser.add_argument('year', type=int, action='store',choices=[y for y in range(year - 10,year + 1,1)],help='The year of the logged points')
parser.add_argument('month', type=int, action='store',choices=[m for m in range(1,13)],help='The month of the logged points')
parser.add_argument('day', type=int, action='store',choices=[d for d in range(1,32)],nargs="?",help='The day of the logged points')
parser.add_argument('--preserve-id',dest='preserve_id', action='store_true',help='Preserve loggedpoint\' id during restoring the data into table \'tracking_loggedpoint\'')
parser.add_argument('--restore-to-origin-table',dest='restore_to_origin_table', action='store_true',help='Restore the archived data to table \'tracking_loggedpoint\'')


def run():
    args = parser.parse_args(sys.argv[2:])
    d = date(args.year,args.month, args.day if args.day else 1)
    if d >= today:
        raise Exception("Can only restore logged points happened before today.")
    if args.day:
        #restore by date
        archive.restore_by_date(d,restore_to_origin_table=args.restore_to_origin_table,preserve_id=args.preserve_id)
    else:
        #restore by month
        archive.restore_by_month(d.year,d.month,restore_to_origin_table=args.restore_to_origin_table,preserve_id=args.preserve_id)



