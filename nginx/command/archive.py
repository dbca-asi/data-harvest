import argparse
from datetime import date,datetime
import sys
import traceback
import logging

import nginx

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog="archive",description='Archive nginx configuration files and push it to blob storage')

def run():
    try:
        args = parser.parse_args(sys.argv[2:])
        nginx.archive()
    except:
        logger.error(traceback.format_exc())
    



