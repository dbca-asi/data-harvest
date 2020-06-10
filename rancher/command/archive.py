import argparse
from datetime import date,datetime
import sys

import rancher

parser = argparse.ArgumentParser(prog="archive",description='Archive rancher configuration files and push it to blob storage')

def run():
    args = parser.parse_args(sys.argv[2:])
    rancher.archive()
    



