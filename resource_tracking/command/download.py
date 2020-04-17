import argparse
from datetime import date,datetime
import sys
import os

from resource_tracking import archive

now = datetime.now()
today = now.date()
year = now.year

parser = argparse.ArgumentParser(prog="restore",description='Restore the logged points from archive')
parser.add_argument('year', type=int, action='store',choices=[y for y in range(year - 30,year + 1,1)],help='The year of the logged points')
parser.add_argument('month', type=int, action='store',choices=[m for m in range(1,13)],help='The month of the logged points')
parser.add_argument('day', type=int, action='store',choices=[d for d in range(1,32)],nargs="?",help='The day of the logged points')
parser.add_argument('--folder',dest='folder', action='store',help='The folder to place the downloaded file')


def run():
    args = parser.parse_args(sys.argv[2:])
    d = date(args.year,args.month, args.day if args.day else 1)
    if d >= today:
        raise Exception("Can only restore logged points happened before today.")
    if args.folder:
        if os.path.exists(args.folder):
            if not os.path.isdir(args.folder):
                raise Exception("{} is not a folder".format(args.folder))
        else:
            #folder doesn't exist, create it
            os.makedirs(args.folder)

    if args.day:
        #download by date
        archive.download_by_date(d,folder=args.folder)
    else:
        #download by month
        archive.download_by_month(d.year,d.month,folder=args.folder)



