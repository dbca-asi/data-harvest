from common_settings import *

AZURE_BLOG_CLIENT_KWARGS={} 
for key,ekey,vtype in [("max_single_put_size","AZURE_MAX_SINGLE_PUT_SIZE",int),("max_single_get_size","AZURE_MAX_SINGLE_GET_SIZE",int)]:
    val=env(ekey,vtype=vtype)
    if val is None:
        continue
    AZURE_BLOG_CLIENT_KWARGS[key] = val
