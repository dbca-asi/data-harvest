import argparse
from datetime import date,datetime
import sys
import traceback
import logging

import azlog

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog="archive",description='Dump logs from azlog and push it to blob storage')
parser.add_argument('max_archive_times', type=int, action='store',nargs="?",help='The maximum archiving times')

def run():
    try:
        args = parser.parse_args(sys.argv[2:])
        if args.max_archive_times:
            azlog.archive(args.max_archive_times)
        else:
            azlog.archive()
    except:
        logger.error(traceback.format_exc())

