import argparse
from datetime import date,datetime
import sys

import docker


parser = argparse.ArgumentParser(prog="harvest",description='Harvest the information from docker image and push it to blob storage')
parser.add_argument('imageid',  action='store',help='docker image id')

def run():
    args = parser.parse_args(sys.argv[2:])
    docker.harvest(args.imageid)
    



