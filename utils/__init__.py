from datetime import datetime,date
import hashlib
import pytz
import sys
import imp
import re
import os
import subprocess
import shutil

from .classproperty import classproperty,cachedclassproperty

from . import gdal
from . import timezone

db_connection_string_re = re.compile('^\s*(?P<database>(postgis)|(postgres))://(?P<user>[a-zA-Z0-9@\-_\.]+)(:(?P<password>[0-9a-zA-Z]+))?@(?P<host>[a-zA-Z0-9\-\_\.@]+)(:(?P<port>[1-9][0-9]*))?/(?P<dbname>[0-9a-zA-Z\-_]+)?\s*$')
def parse_db_connection_string(connection_string):
    """
    postgis://rockyc@localhost/bfrs
    """
    m = db_connection_string_re.match(connection_string)
    if not m:
        raise Exception("Invalid database configuration({})".format(connection_string))

    database_config = {
        "database":m.group("database"),
        "user":m.group("user"),
        "host":m.group("host"),
        "dbname":m.group("dbname"),
        "port" : int(m.group('port')) if m.group("port") else None,
        "password" : m.group('password') if m.group("password") else None
    }

    return database_config


def load_module(name,base_path="."):
    # Fast path: see if the module has already been imported.
    try:
        return sys.modules[name]
    except KeyError:
        pass
    
    path,filename = os.path.split(name.replace(".","/"))
    if not path.startswith("/"):
        base_path = os.path.realpath(base_path)
        path = os.path.join(base_path,path)

    # If any of the following calls raises an exception,
    # there's a problem we can't handle -- let the caller handle it.

    fp, pathname, description = imp.find_module(filename,[path])

    try:
        return imp.load_module(name, fp, pathname, description)
    finally:
        # Since we may exit via an exception, close fp explicitly.
        if fp:
            fp.close()


def file_md5(f):
    cmd = "md5sum {}".format(f)
    output = subprocess.check_output(cmd,shell=True)
    return output.split()[0].decode()

def remove_file(f):
    if not f: 
        return

    try:
        os.remove(f)
    finally:
        pass

def remove_folder(f):
    if not f: 
        return

    try:
        shutil.rmtree(f)
    finally:
        pass

def file_size(f):
    return os.stat(f).st_size
