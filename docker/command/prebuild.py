import argparse
from datetime import date,datetime
import sys

import docker


parser = argparse.ArgumentParser(prog="prebuild",description='Inject some statements to collect some data from image.')
parser.add_argument('workdir',  action='store',help='Current folder')
parser.add_argument('buildpath',  action='store',help='Docker image build path')
parser.add_argument('dockerfile',  action='store',help='Docker file')

def run():
    args = parser.parse_args(sys.argv[2:])
    docker.prebuild(args.workdir,args.buildpath,args.dockerfile)
    



