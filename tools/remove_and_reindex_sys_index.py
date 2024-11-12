import sys
import os
import requests
from requests.auth import HTTPBasicAuth
from importlib import import_module
import re
import json

args = sys.argv
if len(args) == 4:
    http_method = "https" if args[1] == "https" else "http"
    user = args[2]
    password = args[3]
    auth = HTTPBasicAuth(user,password)
elif len(args) == 2:
    http_method = "https" if args[1] == "https" else "http"
    auth = None
else:
    print("Usage: python reindex_all_index.py [http_method] [user] [password]")
    sys.exit(1)



delete_indexes = ["4200"]
reindex_indexes = [".tasks"]


host = os.environ.get('INVENIO_ELASTICSEARCH_HOST','localhost')
base_url = http_method + "://" + host +":9200/"
reindex_url = base_url + "_reindex?pretty&refresh=true&wait_for_completion=true"

verify=False
headers = {"Content-Type":"application/json"}
req_args = {"headers": headers, "verify": verify}
if auth:
    req_args["auth"] = auth 

success_deletes = []
success_reindexes = []
# delete index
for index in delete_indexes:
    url = base_url + index
    response = requests.delete(url, **req_args)
    if response.status_code != 200:
        print("Failed to delete index: " + index)
        print(response.text)
        continue
    print(f"success delete {index}")
    success_deletes.append(index)

for index in reindex_indexes:
    try:
        print(f"# start reindex: {index}")
        res = requests.get(base_url+index, **req_args)
        if res.status_code != 200:
            print(f"## can not find {index}")
            continue
        
        index_info = res.json()
        if index in index_info:
            index_info = index_info[index]
        else:
            print(f"## can not find {index} settings and mappings")
            continue
        
        tmp_index = index + "_tmp"
        json_data_to_tmp = {"source":{"index":index},"dest":{"index":tmp_index}}
        json_data_to_dest = {"source":{"index":tmp_index},"dest":{"index":index}}
        
        defalut_number_of_replicas = index_info.get("settings",{}).get("index",{}).get("number_of_replicas",1)
        default_refresh_interval = index_info.get("settings",{}).get("index",{}).get("refresh_interval","1s")
        performance_setting_body = {"index": {"number_of_replicas": 0, "refresh_interval": "-1"}}
        restore_setting_body = {"index": {"number_of_replicas": defalut_number_of_replicas, "refresh_interval": default_refresh_interval}}

        index_info["settings"]["index"].pop("creation_date", None)
        index_info["settings"]["index"].pop("uuid", None)
        index_info["settings"]["index"].pop("version", None)
        index_info["settings"]["index"].pop("provided_name", None)

        # create tmp index
        res = requests.put(base_url+tmp_index+"?pretty",json=index_info,**req_args)
        if res.status_code != 200:
            print("## raise error in creating tmp index")
            raise Exception(res.text)
        
        # Settings for speed
        res = requests.put(base_url+tmp_index+"/_settings?pretty",json=performance_setting_body,**req_args)
        if res.status_code != 200:
            print("## raise error in applying speed-up settings to tmp indexes")
            raise Exception(res.text)

        # Reindex the target index into a temporary index
        res = requests.post(reindex_url,json=json_data_to_tmp,**req_args)
        if res.status_code != 200:
            print("## raise error in reindexing into tmp index")
            raise Exception(res.text)

        # Delete target index
        res = requests.delete(base_url+index,**req_args)
        if res.status_code != 200:
            print("## raise error in deleting target index")
            raise Exception(res.text)

        # Create new target index
        res = requests.put(base_url+index+"?pretty",json=index_info,**req_args)
        if res.status_code != 200:
            print("## raise error in creating new target index")
            raise Exception(res.text)

        # Settings for speed
        res = requests.put(base_url+index+"/_settings?pretty",json=performance_setting_body,**req_args)
        if res.status_code != 200:
            print("## raise error in applying speed-up settings to target indexes")
            raise Exception(res.text)

        # Reindex the temporary index into a target index
        res = requests.post(reindex_url,json=json_data_to_dest,**req_args)
        if res.status_code != 200:
            print("## raise error in reindexing into target index")
            raise Exception(res.text)
        
        # Reverting settings for speed
        res = requests.put(base_url+index+"/_settings?pretty",json=restore_setting_body,**req_args)
        if res.status_code != 200:
            print("## raise error in reverting settings for speed")
            raise Exception(res.text)
        
        # Delete temporary index
        res = requests.delete(base_url+tmp_index,**req_args)
        if res.status_code != 200:
            print("## raise error in deleting tmp index")
            raise Exception(res.text)
        print(f"## success reindex: {index}")
        success_reindexes.append(index)
    except Exception as e:
        import traceback
        print(traceback.format_exc())

print(f"deletes: {success_deletes}")
print(f"reindexes: {success_reindexes}")