import argparse
from datetime import date,datetime
import sys

import nginx

parser = argparse.ArgumentParser(prog="archive",description='Archive nginx configuration files and push it to blob storage')

def run():
    args = parser.parse_args(sys.argv[2:])
    nginx.archive()
    



