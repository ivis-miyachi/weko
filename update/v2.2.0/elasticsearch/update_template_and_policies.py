
import sys
import os
import json
import requests
from requests.auth import HTTPBasicAuth
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

args = sys.argv
if len(args) == 3:
    user = args[1]
    password = args[2]
    auth = HTTPBasicAuth(user,password)
elif len(args) == 1:
    auth = None
else:
    print("Usage: python update_template_and_policies.py [user] [password]")
    sys.exit(1)

host = "opensearch"
prefix = os.environ.get('SEARCH_INDEX_PREFIX')
module_dir = "/code/modules/"
base_url = f"https://{host}:9200/"
template_url = base_url + "_template/{}"
ism_url = base_url + "_plugins/_ism/policies/weko_stats_policy"
ism_add_url = base_url + "_plugins/_ism/add/{}"
settings_url = base_url + "{}/_settings"
close_url = base_url + "{}/_close"
open_url = base_url + "{}/_open"

verify = False
headers = {"Content-Type":"application/json"}

req_args = {"headers":headers,"verify":verify}
if auth:
    req_args["auth"] = auth

templates_files = {
    f"{prefix}-events-v1": "invenio-stats/invenio_stats/contrib/events/os-v2/events-v1.json",
    f"{prefix}-aggregation-v1": "invenio-stats/invenio_stats/contrib/aggregations/os-v2/aggregation-v1.json",
}

templates = {}
print("# get template from json files")
for index, path in templates_files.items():
    file_path = os.path.join(module_dir, path)
    if not os.path.isfile(file_path):
        print("## not exist file: {}".format(file_path))
        continue
    with open(file_path, "r") as json_file:
        templates[index] = json.loads(
            json_file.read().\
                replace("__SEARCH_INDEX_PREFIX__",prefix+"-")
        )

try:
    print("# put templates")
    print("## put events template")
    res = requests.put(template_url.format(f"{prefix}-events-v1"), json=templates[f"{prefix}-events-v1"],**req_args)
    if res.status_code!=200:
        raise Exception(res.text)
    print("## put aggregations template")
    res = requests.put(template_url.format(f"{prefix}-aggregation-v1"), json=templates[f"{prefix}-aggregation-v1"],**req_args)
    if res.status_code!=200:
        raise Exception(res.text)

    ism_body = {
        "policy": {
            "description": "Rollover policy based on max size",
            "default_state": "rollover",
            "states": [
            {
                "name": "rollover",
                "actions": [
                {
                    "rollover": {
                    "min_size": "50gb"
                    }
                }
                ]
            }
            ],
            "ism_template": [
                {
                    "index_patterns": [
                        "*-events-stats-index-*",
                        "*-stats-index-*",
                    ],
                }
            ]
        }
    }
    policy_name="weko_stats_policy"
    print("# put ism policy")
    res = requests.put(ism_url, json=ism_body,**req_args)
    if res.status_code!=201:
        print(3)
        raise Exception(res.text)

    print("# get indexes")
    target_filter = f"{prefix}-*"
    indexes = requests.get(f"{base_url}{target_filter}",**req_args).json()
    alias_list = [f"{prefix}-events-stats-index", f"{prefix}-stats-index"]
    target_index = []
    for index_name, index_info in indexes.items():
        aliases = index_info.get("aliases", {})
        for alias_name, alias_info in aliases.items():
            if alias_name in alias_list:
                is_write_index = alias_info.get("is_write_index", None)
                print(f"index: {index_name}, alias: {alias_name}, is_write_index: {is_write_index}")
                if is_write_index:
                    target_index.append([index_name,alias_name])

    for index_name,alias_name in target_index:
        print(f"## {index_name} setting")
        print("### close index")
        index_close_url = close_url.format(index_name)
        res = requests.post(index_close_url, **req_args)
        if res.status_code != 200:
            raise Exception(res.text)

        print("### update index settings")
        update_settings_url = settings_url.format(index_name)
        settings_body = {
            "archived.index.lifecycle.name": None,
            "archived.index.lifecycle.rollover_alias": None,
            "index.plugins.index_state_management.policy_id": "weko_stats_policy",
            "index.plugins.index_state_management.rollover_alias": alias_name
        }
        res = requests.put(update_settings_url, json=settings_body, **req_args)
        if res.status_code != 200:
            raise Exception(res.text)

        print("### open index")
        index_open_url = open_url.format(index_name)
        res = requests.post(index_open_url, **req_args)
        if res.status_code != 200:
            raise Exception(res.text)

        print("### add ism policy to index")
        add_policy_url = ism_add_url.format(index_name)
        add_policy_body = {
            "policy_id": "weko_stats_policy"
        }
        res = requests.post(add_policy_url, json=add_policy_body, **req_args)
        if res.status_code != 200:
            raise Exception(res.text)

except Exception as e:
    import traceback
    print("## raise error")
    print(traceback.format_exc())
