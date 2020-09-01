import argparse
from datetime import date,datetime
import sys
import traceback
import logging

from containerlog import ContainerLogArchive,settings

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog="archive",description='Dump container logs from azlog and push it to local storage')
parser.add_argument('max_archive_times', type=int, action='store',nargs="?",help='The maximum archiving times')

def run():
    try:
        args = parser.parse_args(sys.argv[2:])
        if args.max_archive_times:
            ContainerLogArchive.get_instance(settings).archive(args.max_archive_times)
        else:
            ContainerLogArchive.get_instance(settings).archive()
    except:
        logger.error(traceback.format_exc())

