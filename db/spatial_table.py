import tempfile

def dump_data(view,view_sql=None,delete_sql=None,file_name=None,file_ext=None,check=True):
    """
    view : a table or view, where the data are coming from.
    view_sql: if not none, should be executed before dumping, and should deleted after dumping
    delete_sql: if not nond, should be executed after successfully dumping
    file_name: the dump file name
    file_ext: the dump file extension
    check:if True, will check the feature numbers between the view and the dumple file to check whether dumping is succeed or not.
    """
    if not file_name or not file_ext:
        raise Exception("Must specify file_name or file_ext, but not both")

    if file_ext[0] != ".":
        file_ext = "." + file_ext

    if not file_name:
        file_name = tempfile.NamedTemporaryFile(suffix=file_ext)


