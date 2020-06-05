import os
import json
import socket
import getpass
import tempfile
import re
import shutil
import subprocess

from utils import timezone
from . import settings
from data_storage.utils import JSONEncoder,JSONDecoder
from data_storage import AzureBlobGroupResource

pip_install_re = re.compile("^\s*RUN\s+(?P<pip>pip[0-9]?)\s+install\s+.+\-r\s+[\S]+",re.IGNORECASE)
python_run_re = re.compile("^\s*RUN\s+(?P<python>python[0-9]?)\s+[\S]+",re.IGNORECASE)

#harvest_statements_template="""RUN pip3 install --no-cache-dir pipdeptree
harvest_statements_template="""COPY {image_harvester} /image_harvester.py
COPY {image_metadata_file} /image_metadata.json
RUN {python} /image_harvester.py
RUN rm /image_harvester.py
"""

detached_re = re.compile("^\s*HEAD\s+detached\s+at\s+(?P<attach_point>[\S]+)",re.IGNORECASE)
commitid_re = re.compile("^\s*commit\s+(?P<commitid>[\S]+)",re.IGNORECASE)
def prebuild(workdir,buildpath,dockerfile):
    """
    called before building docker image
    Add some commands to Dockerfile to detect the python library dependency
    """
    if workdir[0] in ("'","\""):
        workdir = workdir[1:-1]
    workdir = os.path.abspath(workdir)

    if buildpath[0] in ("'","\""):
        buildpath = buildpath[1:-1]
    buildpath = os.path.abspath(os.path.join(workdir,buildpath))
    if not os.path.exists(buildpath):
        raise Exception("The build path({}) dones not exist".format(buildpath))
    elif not os.path.isdir(buildpath):
        raise Exception("The build path({}) is not a folder".format(buildpath))

    if dockerfile[0] in ("'","\""):
        dockerfile = dockerfile[1:-1]
    dockerfile = os.path.abspath(os.path.join(buildpath,dockerfile))
    if not os.path.exists(dockerfile):
        raise Exception("The dockerfile({}) dones not exist".format(dockerfile))
    elif not os.path.isfile(dockerfile):
        raise Exception("The dockerfile({}) is not a file".format(dockerfile))

    image_metadata = {
        "build_time":timezone.now(),
        "build_host": socket.getfqdn(),
        "build_folder":buildpath,
        "build_user": getpass.getuser(),
        "build_docketfile":dockerfile,
    }

    #get the git repository information
    #try to get the current branch, modified files and untracked files
    changed_files = []
    branch = None
    repository = None
    tag = None
    last_commit = None
    if os.path.exists(os.path.join(buildpath,".git")):
        #is a git repository
        git_status = subprocess.check_output("cd {}&&git status -b -s -u --porcelain".format(buildpath),shell=True).decode()
        git_status_lines = git_status.split(os.linesep)
        for line in git_status_lines:
            if line.startswith("## "):
                if "(no branch)" in line.lower():
                    branch = None
                    repository = None
                else:
                    branch_data = line[3:].strip()
                    if "..." in branch_data:
                        branch_local,branch_remote = branch_data.split("...",maxsplit=1)
                        repository_name,branch = branch_remote.split("/",1)
                        repository = subprocess.check_output("cd {}&&git remote get-url {}".format(buildpath,repository_name),shell=True).decode().strip()
                        branch = "{}/tree/{}".format(repository,branch)
                    else:
                        branch = branch_data
                        repository = None
                pass
            elif line:
                #untracked files
                changed_files.append(line)
        if not branch:
            #it attached to a tag or a remote branch
            git_status = subprocess.check_output("cd {}&&git status".format(buildpath),shell=True).decode()
            m = detached_re.search(git_status)
            if not m:
                raise Exception("Can't find the attach point with git")
            attach_point = m.group("attach_point")
            if "/" in attach_point:
                #attached to a remote repository
                repository_name,branch = attach_point.split("/",1)
                repository = subprocess.check_output("cd {}&&git remote get-url {}".format(buildpath,repository_name),shell=True).decode().strip()
                branch = "{}/tree/{}".format(repository,branch)
            else:
                #attached to a tag
                tag = attach_point
                remotes = subprocess.check_output("cd {}&&git remote".format(buildpath),shell=True).decode()
                remotes = remotes.split(os.linesep)
                for remote in remotes:
                    tag_data = subprocess.check_output("cd {}&&git ls-remote --tags {} | grep \"refs/tags/{}\"".format(buildpath,remotes[0],tag),shell=True).decode()
                    if "refs/tags/{}".format(tag) in tag_data:
                        repository = subprocess.check_output("cd {}&&git remote get-url {}".format(buildpath,remote),shell=True).decode().strip()
                        tag = "{}/tree/{}".format(repository,tag)
                        break
    
        if not repository:
            remotes = subprocess.check_output("cd {}&&git remote".format(buildpath),shell=True).decode()
            if "origin" in remotes:
                repository = subprocess.check_output("cd {}&&git remote get-url {}".format(buildpath,"origin"),shell=True).decode().strip()
            else:
                repository = subprocess.check_output("cd {}&&git remote get-url {}".format(buildpath,remotes[0]),shell=True).decode().strip()
    
        #get the latest commit
        commit_data = subprocess.check_output("cd {}&&git log -n 1".format(buildpath),shell=True).decode()
        m = commitid_re.search(commit_data)
        if  m:
            last_commit = m.group("commitid")

    image_metadata["git_repository"] = repository
    image_metadata["git_branch"] = branch
    image_metadata["git_tag"] = tag
    image_metadata["git_last_commit"] = last_commit
    image_metadata["git_changed_files"] = changed_files
    if changed_files:
        image_metadata["git_changes"] = subprocess.check_output("cd {}&&git diff".format(buildpath),shell=True).decode()
    else:
        image_metadata["git_changes"] = None



    tmpdir = tempfile.mkdtemp(prefix="dockerfile_",dir=buildpath)
    new_dockerfile = os.path.join(tmpdir,"Dockerfile")
    image_metadata_file = os.path.join(tmpdir,"image_metadata.json")
    image_harvester_file = os.path.join(tmpdir,"image_harvester.py")

    write_harvest_statements = True
    user_found = False
    cmd_found = False

    #copy the image_harvester.py to build path
    shutil.copyfile(os.path.join(settings.MODULE_DIR,"image_harvester.py"),image_harvester_file)

    harvest_statements = lambda image_metadata:harvest_statements_template.format(image_harvester=os.path.relpath(image_harvester_file,buildpath),image_metadata_file=os.path.relpath(image_metadata_file,buildpath),python=(image_python or "python"))
    first_line = True
    image_python = None
    with open(new_dockerfile,'w') as wf:
        new_dockerfile = wf.name
        with open(dockerfile,'r') as f:
            line = f.readline()
            while line:
                try:
                    s_line = line.strip().upper()
                    if not s_line:
                        #empty line
                        wf.write(line)
                    elif s_line[0] == '#':
                        #comment line
                        wf.write(line)
                    elif first_line:
                        first_line = False
                        if s_line.startswith("FROM"):
                            #declare the base image, parse the base image name,account and version
                            baseimage_account = None
                            baseimage_name = None
                            baseimage_version = None
                            datas = line.strip().split()
                            if "/" in datas[1]:
                                baseimage_account,baseimage = datas[1].split("/",1)
                            else:
                                baseimage_account = None
                                baseimage = datas[1]
                            if ":" in baseimage:
                                baseimage_name,baseimage_version = baseimage.split(":",1)
                            else:
                                baseimage_name = baseimage
                                baseimage_version = None
                            image_metadata["baseimage_account"] = baseimage_account
                            image_metadata["baseimage_name"] = baseimage_name
                            image_metadata["baseimage_version"] = baseimage_version
                            wf.write(line)
                        else:
                            raise Exception("Mssing the baseimage declaration.")
                    else:
                        if s_line.startswith("HEALTHCHECK"):
                            image_metadata["image_healthcheck"] = line.strip().split(maxsplit=1)[1]
                            wf.write(line)
                        elif s_line.startswith("WORKDIR"):
                            image_metadata["image_workdir"] = line.strip().split(maxsplit=1)[1]
                            wf.write(line)
                        elif s_line.startswith("EXPOSE"):
                            image_metadata["image_expose_port"] = int(line.strip().split(maxsplit=1)[1])
                            wf.write(line)
                        elif s_line.startswith("USER"):
                            if write_harvest_statements:
                                wf.write(harvest_statements(image_metadata))
                                write_harvest_statements = False
                            image_metadata["image_user"] = line.strip().split(maxsplit=1)[1]
                            wf.write(line)
                            user_found = True
                        elif s_line.startswith("CMD"):
                            if write_harvest_statements:
                                wf.write(harvest_statements(image_metadata))
                                write_harvest_statements = False
                            image_metadata["image_cmd"] = json.loads(line.strip().split(maxsplit=1)[1])
                            wf.write(line)
                            cmd_found = True
                        else:
                            wf.write(line)
                            m = pip_install_re.search(line)
                            if m:
                                image_metadata["image_language"] = "python"
                                image_metadata["image_pip"] = m.group("pip")
                                if user_found:
                                    raise Exception("Please move the statement 'USER' under statement '{}' ".format(line))
                                if cmd_found:
                                    raise Exception("Please move the statement 'CMD' under statement '{}' ".format(line))
                                continue
                            if not image_python:
                                m = python_run_re.search(line)
                                if m:
                                    image_metadata["image_language"] = "python"
                                    image_python = m.group("python")
                                    if user_found:
                                        raise Exception("Please move the statement 'USER' under statement '{}' ".format(line))
                                    if cmd_found:
                                        raise Exception("Please move the statement 'CMD' under statement '{}' ".format(line))
                                    continue
                finally:
                    line = f.readline()
        #if can't find statements 'USER' and 'CMD', append the harvest statements at the end
        if write_harvest_statements:
            wf.write(harvest_statements(image_metadata))

    if "image_expose_port" in image_metadata:
        #export a port,think it is a web project
        image_metadata["image_app_type"] = "webapp"
    else:
        image_metadata["image_app_type"] = "app"

    if "image_cmd" in image_metadata:
        if isinstance(image_metadata["image_cmd"],str):
            #convert the cmd from a string to list
            cmd = image_metadata["image_cmd"].strip()
            if "\"" in cmd or "'" in cmd:
                raise Exception("The command({}) includs \" or \', it is hard to parse, please convert it to list form(the prefered form)".format(cmd))
            cmd_list = [ s for s in cmd.split() if s]
            index = len(cmd_list) - 1
            while index >= 0:
                if cmd_list[index].startswith("--") and "=" in cmd_list[index]:
                    flag,value = cmd_list[index].split("=",1)
                    cmd_list[index] = flag
                    cmd_list.insert(index + 1,value)
                index -= 1
            image_metadata["image_cmd"] = cmd_list

        def get_option(args,option,flag_option=False):
            """
            get the option's value
            option: can be a string or list of string for the same option
            flag_option means this option is a flag optin, return True if exist otherwise return False
            """
            index = -1
            if isinstance(option,(tuple,list)):
                for o in option:
                    try:
                        index = args.index(o)
                        break
                    except ValueError as ex:
                        continue
            else:
                try:
                    index = args.index(option)
                except ValueError as ex:
                    pass
            if index == -1:
                return False if flag_option else None
            else:
                return True if flag_option else args[index + 1]

        cmd = image_metadata["image_cmd"]
        s_cmd = image_metadata["image_cmd"][0].lower()
        cmd_args = image_metadata["image_cmd"][1:]
        config_file_option = None
        if s_cmd == "gunicorn":
            image_metadata["image_type"] = "webapp"
            image_metadata["image_language"] = "python"
            image_metadata["image_server"] = "gunicorn"
            config_file_option = ("-c","--config")
        elif s_cmd == "uwsgi":
            image_metadata["image_type"] = "webapp"
            image_metadata["image_language"] = "python"
            image_metadata["image_server"] = "uwsgi"
            config_file_option = ("-i","--ini")
        elif s_cmd.startswith("python"):
            image_metadata["image_language"] = "python"
            if get_option(cmd_args,'runserver'):
                image_metadata["image_type"] = "webapp"
            else:
                image_metadata["image_type"] = "app"

        if config_file_option:
            config_file = get_option(cmd_args,config_file_option)
            if config_file:
                with open(os.path.join(buildpath,config_file),'r') as f:
                    config_txt = f.read()
            else:
                config_txt = None
            image_metadata["image_server_config"] = config_txt
    
    with open(image_metadata_file,"w") as f:
        f.write(json.dumps(image_metadata,cls=JSONEncoder,indent=4))

    print(new_dockerfile)


_blob_resource = None
def get_blob_resource():
    """
    Return the blob resource client
    """
    global _blob_resource
    if _blob_resource is None:
        _blob_resource = AzureBlobGroupResource(
            settings.DOCKER_RESOURCE_NAME,
            settings.AZURE_CONNECTION_STRING,
            settings.AZURE_CONTAINER,
            archive=False,
        )
    return _blob_resource

resource_file = lambda imageid:imageid.replace("/","_").replace(":","_").replace('.','-')
resource_group = lambda imageid:imageid.replace("/","_").replace(":","_").replace('.','-')
def harvest(imageid):
    """
    called before building docker image
    Add some commands to Dockerfile to detect the python library dependency
    """
    docker_metadata = subprocess.check_output("docker container run {} cat /image_metadata.json".format(imageid),shell=True).decode()
    docker_metadata = json.loads(docker_metadata,cls=JSONDecoder)

    docker_account,remains = imageid.split("/",1)
    docker_repository,docker_repository_tag = remains.split(":",1)

    docker_metadata["docker_account"] = docker_account
    docker_metadata["docker_repository"] = docker_repository
    docker_metadata["docker_repository_tag"] = docker_repository_tag


    metadata = {
        "resource_id":imageid,
        "resource_file":"{}_{}_{}.json".format(docker_account,docker_repository,docker_repository_tag),
        "resource_group":"{}_{}".format(docker_account,docker_repository),
    }
    resourcemetadata = get_blob_resource().push_json(docker_metadata,metadata=metadata)



