import argparse
from datetime import date,datetime
import sys
import traceback
import logging

import rancher

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog="archive",description='Clean deleted resources from blob storage')

def run():
    try:
        args = parser.parse_args(sys.argv[2:])
        rancher.clean_resources()
    except:
        logger.error(traceback.format_exc())
    



