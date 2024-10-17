import os
import requests
from requests.auth import HTTPBasicAuth
from importlib import import_module
import re
import json

host = os.environ.get('INVENIO_ELASTICSEARCH_HOST','localhost')
port = 9200
auth = ("admin","admin")

#from opensearchpy import OpenSearch
#client = OpenSearch(
#    hosts=[{"host":host,"port":port}],
#    http_auth=auth,
#    use_ssl=True,
#    verify_certs=False
#)
from elasticsearch import Elasticsearch
client = Elasticsearch(
    host=host,
    # http_auth=auth,
    # use_ssl=True,
    verify_certs=False
)

#version = "os-v2"
version="v7"
mapping_files = {
    # "deposits-deposit-v1.0.0": ("invenio_deposit", f"mappings/{version}/deposits/deposit-v1.0.0.json"),
    "authors-author-v1.0.0": f"weko-authors/weko_authors/mappings/{version}/authors/author-v1.0.0.json",
    "weko-item-v1.0.0": f"weko-schema-ui/weko_schema_ui/mappings/{version}/weko/item-v1.0.0.json",
    # "marc21-holdings-hd-v1.0.0": ("invenio_marc21", f"mappings/{version}/marc21/holdings/hd-v1.0.0.json"),
    # "marc21-authority-ad-v1.0.0": ("invenio_marc21",f"mappings/{version}/marc21/authority/ad-v1.0.0.json"),
    # "marc21-bibliographic-bd-v1.0.0": ("invenio_marc21", f"mappings/{version}/marc21/bibliographic/bd-v1.0.0.json"),
}

# template_files = {
#     "events-stats-celery-task": ("invenio_stats", f"contrib/celery_task/{version}/celery-task-v1.json"),
#     "events-stats-file-download": ("invenio_stats", f"contrib/file_download/{version}/file-download-v1.json"),
#     "events-stats-file-preview": ("invenio_stats", f"contrib/file_preview/{version}/file-preview-v1.json"),
#     "events-stats-item-create": ("invenio_stats", f"contrib/item_create/{version}/item-create-v1.json"),
#     "events-stats-record-view": ("invenio_stats", f"contrib/record_view/{version}/record-view-v1.json"),
#     "events-stats-search": ("invenio_stats", f"contrib/search/{version}/search-v1.json"),
#     "events-stats-top-view": ("invenio_stats", f"contrib/top_view/{version}/top-view-v1.json"),
#     "stats-celery-task": ("invenio_stats", f"contrib/aggregations/aggr_celery_task/{version}/aggr-celery-task-v1.json"),
#     "stats-file-download": ("invenio_stats", f"contrib/aggregations/aggr_file_download/{version}/aggr-file-download-v1.json"),
#     "stats-file-preview": ("invenio_stats", f"contrib/aggregations/aggr_file_preview/{version}/aggr-file-preview-v1.json"),
#     "stats-item-create": ("invenio_stats", f"contrib/aggregations/aggr_item_create/{version}/aggr-item-create-v1.json"),
#     "stats-record-view": ("invenio_stats", f"contrib/aggregations/aggr_record_view/{version}/aggr-record-view-v1.json"),
#     "stats-search": ("invenio_stats", f"contrib/aggregations/aggr_search/{version}/aggr-search-v1.json"),
#     "stats-top-view": ("invenio_stats", f"contrib/aggregations/aggr_top_view/{version}/aggr-top-view-v1.json"),
# }
stats_indexes = ["events-stats-celery-task", "events-stats-file-download", "events-stats-file-preview", "events-stats-item-create", "events-stats-record-view", "events-stats-search", "events-stats-top-view", "stats-celery-task", "stats-file-download", "stats-file-preview", "stats-item-create", "stats-record-view", "stats-search", "stats-top-view"]

template_files = {
    "events-stats-index": f"invenio-stats/invenio_stats/contrib/events/{version}/events-v1.json",
    "stats-index": f"invenio-stats/invenio_stats/contrib/aggregations/{version}/aggregation-v1.json",
}

delete_indexes = [
    "stats-bookmarks",
    "marc21-holdings-hd-v1.0.0",
    "marc21-authority-ad-v1.0.0",
    "marc21-bibliographic-bd-v1.0.0",
]

# indexとalias一覧取得
organization_aliases = os.environ.get('SEARCH_INDEX_PREFIX')+"-*"
indexes_alias = client.indices.get_alias(name=organization_aliases)
print(indexes_alias)
indexes = {}
write_indexes = {"events-stats-index":[],"stats-index":[]}
for index in indexes_alias:
    aliases = indexes_alias[index].get("aliases",{})
    indexes[index] = aliases
    
    index_tmp = index.replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-", "")
    index_tmp = re.sub("-\d{6}$","",index_tmp)
    if index_tmp not in stats_indexes:
        continue
    index_type = "events-stats-index" if "events" in index_tmp else "stats-index"
    for alias, alias_info in aliases.items():
        if alias_info.get("is_write_index",False) is True:
            write_indexes[index_type].append(
                {"index":index,"alias":alias}
            )
print(write_indexes)
    
print(indexes)
modules_dir = "/code/modules/"
mappings = {}
templates = {}
# ファイルからマッピングデータを取得
print("# get mapping from json file")
for index in indexes:
    index_tmp = index.replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-", "")
    index_tmp = re.sub("-\d{6}$","",index_tmp)
    if index_tmp not in list(mapping_files.keys()):
        print("## not exists: {}, {}".format(index, index_tmp))
        continue
    if index_tmp in stats_indexes:
        continue
    mapping_type=""
    mapping_file_datas = mapping_files
    mapping_type="mapping"

    path_data = mapping_file_datas[index_tmp]
    # module_name = path_data[0]
    # try:
    #     res = import_module(module_name)
    # except ImportError:
    #     print("##not find: {}".format(module_name))
    #     import traceback
    #     print(traceback.format_exc())
    #     continue
    # current_path = os.path.dirname(os.path.abspath(res.__file__))
    file_path = os.path.join(modules_dir, path_data)
    
    if not os.path.isfile(file_path):
        print("## not exist file: {}".format(file_path))
        continue

    with open(file_path, "r") as json_file:
        mappings[index] = json.loads(json_file.read())
print("#get template files")
for index, path in template_files.items():
    file_path = os.path.join(modules_dir, path)
    if not os.path.isfile(file_path):
        print("## not exist file: {}".format(file_path))
        continue
    with open(file_path, "r") as json_file:
        templates[index] = json.loads(
            json_file.read().\
                replace("__SEARCH_INDEX_PREFIX__",os.environ.get('SEARCH_INDEX_PREFIX')+"-")
        )
print(templates)
#base_url = "https://"+host +":9200/"
base_url = "http://"+host +":9200/"
reindex_url = base_url + "_reindex?pretty&refresh=true&wait_for_completion=true"
template_url = base_url + "_template/{}"
auth = HTTPBasicAuth("admin","admin")
verify=False
headers = {"Content-Type":"application/json"}
percolator_body = {"properties": {"query": {"type": "percolator"}}}
req_args = {"headers":headers,"auth":auth,"verify":verify}

# 削除対象のインデックスの削除
delete_target_index = [index for index in indexes if index.replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-","") in delete_indexes]
for target in delete_target_index:
    res = requests.delete(base_url+target,**req_args)

# # for index in indexes:
# for index, mapping in mappings.items():
#     print("# start reindex: {}".format(index))
#     tmpindex = index+"-tmp"
    
#     # target index is weko-item-v1.0.0
#     is_weko_item = index.replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-","") == "weko-item-v1.0.0"
    
#     # target index mapping
#     base_index_definition = mappings[index]

#     # create speed up setting body
#     defalut_number_of_replicas = base_index_definition.get("settings",{}).get("index",{}).get("number_of_replicas",1)
#     default_refresh_interval = base_index_definition.get("settings",{}).get("index",{}).get("refresh_interval","1s")
#     performance_setting_body = {"index": {"number_of_replicas": 0, "refresh_interval": "-1"}}
#     restore_setting_body = {"index": {"number_of_replicas": defalut_number_of_replicas, "refresh_interval": default_refresh_interval}}
    
#     # body for reindex
#     json_data_to_tmp = {"source":{"index":index},"dest":{"index":tmpindex}}
#     json_data_to_dest = {"source":{"index":tmpindex},"dest":{"index":index}}
    
#     # body for setting alias
#     json_data_set_aliases = {
#         "actions":[]
#     }
#     for alias in indexes[index]:
#         alias_info = {"index": index, "alias": alias}
#         if "is_write_index" in indexes[index][alias]:
#             alias_info["is_write_index"] = indexes[index][alias]["is_write_index"]
#         json_data_set_aliases["actions"].append({"add":alias_info})

#     try:
#         # 一時保存用インデックス作成
#         #res = requests.put(base_url+tmpindex+"?pretty",headers=headers,json=base_index_definition,auth=auth,verify=verify)
#         res = requests.put(base_url+tmpindex+"?pretty",json=base_index_definition,**req_args)
#         # elif index in templates:
#         #     template_url.format(index+"/"+version)
#         #     res = requests.put(template_url,headers=headers,json=base_index_definition)
#         #     res = requests.put(base_url+tmpindex+"?pretty",headers=headers)
#         if res.status_code!=200:
#             raise Exception(res.text)
        
#         if is_weko_item:
#             #res = requests.put(base_url+tmpindex+"/_mapping",headers=headers,json=percolator_body,auth=auth,verify=verify)
#             res = requests.put(base_url+tmpindex+"/_mapping/",json=percolator_body,**req_args)
            
#             if res.status_code!=200:
#                 raise Exception(res.text)
#         print("## create tmp index")
        
#         # 高速化のための設定
#         #res = requests.put(base_url+tmpindex+"/_settings?pretty",headers=headers,json=performance_setting_body,auth=auth,verify=verify)
#         res = requests.put(base_url+tmpindex+"/_settings?pretty",json=performance_setting_body,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)
#         print("## speed-up setting for tmp_index")
        
#         # 一時保存用インデックスに元のインデックスの再インデックス
#         #res = requests.post(url=reindex_url,headers=headers,json=json_data_to_tmp,auth=auth,verify=verify)
#         res = requests.post(url=reindex_url,json=json_data_to_tmp,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)
#         print("## reindex to tmp_index")

#         # 再インデックス前のインデックスを削除
#         #res = requests.delete(base_url+index,headers=headers,auth=auth,verify=verify)
#         res = requests.delete(base_url+index,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)
#         print("## delete old index")
        
#         # 新しくインデックス作成
#         #res = requests.put(base_url+tmpindex+"?pretty",headers=headers,json=base_index_definition,auth=auth,verify=verify)
#         res = requests.put(base_url+index+"?pretty",json=base_index_definition,**req_args)
#         # elif index in templates:
#         #     template_url.format(index+"/"+version)
#         #     res = requests.put(template_url,headers=headers,json=base_index_definition)
#         #     res = requests.put(base_url+index+"?pretty",headers=headers)

#         if is_weko_item:
#             #res = requests.put(base_url+tmpindex+"/_mapping",headers=headers,json=percolator_body,auth=auth,verify=verify)
#             res = requests.put(base_url+tmpindex+"/_mapping/",json=percolator_body,**req_args)
#             if res.status_code!=200:
#                 raise Exception(res.text)
#         print("## create new index")
        
#         # 高速化のための設定
#         #res = requests.put(base_url+index+"/_settings?pretty",headers=headers,json=performance_setting_body,auth=auth,verify=verify)
#         res = requests.put(base_url+index+"/_settings?pretty",json=performance_setting_body,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)
#         print("## speed-up setting for new index")
        
#         # aliasの設定
#         if json_data_set_aliases["actions"]:
#             #res = requests.post(base_url+"_aliases",headers=headers,json=json_data_set_aliases,auth=auth,verify=verify)
#             res = requests.post(base_url+"_aliases",json=json_data_set_aliases,**req_args)
#             if res.status_code!=200:
#                 raise Exception(res.text)
#         print("## setting alias for new index")
        
#         # アイテムの再挿入
#         #res = requests.post(url=reindex_url, headers=headers,json=json_data_to_dest,auth=auth,verify=verify)
#         res = requests.post(url=reindex_url,json=json_data_to_dest,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)
#         print("## put into new index")
        
#         # 高速化のための設定を元に戻す
#         #res = requests.put(base_url+index+"/_settings?pretty",headers=headers,json=restore_setting_body,auth=auth,verify=verify)
#         res = requests.put(base_url+index+"/_settings?pretty",json=restore_setting_body,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)

#         # 一時保存用のインデックスを削除
#         #res = requests.delete(base_url+tmpindex,auth=auth,verify=verify)
#         res = requests.delete(base_url+tmpindex,**req_args)
#         if res.status_code!=200:
#             raise Exception(res.text)
#         print("## delete tmp_index")
        
#         print("# end reindex: {}\n".format(index))
#     except Exception as e:
#         import traceback
#         print("raise error: {}".format(index))
#         print(traceback.format_exc())

event_stats_types = [
    "events-stats-celery-task",
    "events-stats-file-download",
    "events-stats-file-preview",
    "events-stats-item-create",
    "events-stats-record-view",
    "events-stats-search",
    "events-stats-top-view",
]

# 新しいevent-statsインデックスの作成
try:
    event_write_indexes = write_indexes["events-stats-index"]
    json_data_toggle_aliases={"actions":[]}
    for index in event_write_indexes:
        json_data_toggle_aliases["actions"].append({
            "remove":{
                "index": index["index"],
                "alias": index["alias"],
            }
        })
        json_data_toggle_aliases["actions"].append({
            "add":{
                "index": index["index"],
                "alias": index["alias"],
                "is_write_index": False
            }
        })
    print(json_data_toggle_aliases)
    res = requests.post(base_url+"_aliases",json=json_data_toggle_aliases,**req_args)
    print(res.text)
    print("# start aggregation events-stats")
    index_name = "events-stats-index"
    index_with_prefix = f"{os.environ.get('SEARCH_INDEX_PREFIX')}-{index_name}"
    new_event_stats_index_name = f"{index_with_prefix}-000001"
    template_url_event_stats = template_url.format(index_with_prefix)
    print(template_url_event_stats)
    print("## set templates")
    res = requests.put(template_url_event_stats,json=templates[new_event_stats_index_name.replace("-000001","").replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-","")],**req_args)
    print(res.text)
    print("$ create index")
    res = requests.put(base_url+new_event_stats_index_name+"?pretty",**req_args)
    print(res.text)
    # エイリアスの設定
    print("## set aliases")
    json_data_set_aliases = {
        "actions":[
            {
                "add": {
                    "index":new_event_stats_index_name,
                    "alias":os.environ.get('SEARCH_INDEX_PREFIX')+"-events-stats-index",
                    "is_write_index":True
                }
            }
        ]
    }
    for type in event_stats_types:
        event_type = type.replace("events-stats-","")
        alias_info = {
            "index": new_event_stats_index_name,
            "alias": os.environ.get('SEARCH_INDEX_PREFIX')+"-events-stats-"+event_type,
            "is_write_index": True,
            "filter":{
                "term":{"event_type":event_type}
            }
        }
        json_data_set_aliases["actions"].append({"add":alias_info})
    res = requests.post(base_url+"_aliases",json=json_data_set_aliases,**req_args)
    print(res.text)
    # event-statsインデックスの集約
    print("## index Aggregation")
    event_stats_indexes = [index for index in indexes if re.sub("-\d{6}$","",index.replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-", "")) in event_stats_types]
    for index in event_stats_indexes:
        from_reindex = index
        to_reindex = os.environ.get('SEARCH_INDEX_PREFIX')+"-events-stats-index"
        event_type = index_tmp.replace("events-stats-","")
        body = {
            "source": {"index": from_reindex},
            "dest": {"index": to_reindex},
            "script": {
                "source": "ctx._source['event_type'] = {}".format(event_type),
                "lang": "painless"
            }
        }
except Exception as e:
    import traceback
    print("raise error: {}".format(index))
    print(traceback.format_exc())


stats_types = [
    "stats-celery-task",
    "stats-file-download",
    "stats-file-preview",
    "stats-item-create",
    "stats-record-view",
    "stats-search",
    "stats-top-view",
]
# statsインデックスの集約
print("# start aggregation stats")
stats_write_indexes = write_indexes["stats-index"]
json_data_toggle_aliases={"actions":[]}
for index in stats_write_indexes:
    json_data_toggle_aliases["actions"].append({
        "remove":{
            "index": index["index"],
            "alias": index["alias"],
        }
    })
    json_data_toggle_aliases["actions"].append({
        "add":{
            "index": index["index"],
            "alias": index["alias"],
            "is_write_index": False
        }
    })
json_data_toggle_aliases
res = requests.post(base_url+"_aliases",json=json_data_toggle_aliases,**req_args)
print(res.text)
index_name ="stats-index"
index_with_prefix = f"{os.environ.get('SEARCH_INDEX_PREFIX')}-{index_name}"
new_stats_index_name = f"{index_with_prefix}-000001"
template_url_stats = template_url.format(index_with_prefix)
print(template_url_stats)
print("## set templates")
res = requests.put(template_url_stats,json=templates[new_stats_index_name.replace("-000001","").replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-","")],**req_args)
print(res.text)
print("## create index")
res = requests.put(base_url+new_stats_index_name+"?pretty",**req_args)
print(res.text)
# エイリアスの設定
print("## set aliases")
json_data_set_aliases = {
    "actions":[
        {
            "add":
                {
                    "index":new_stats_index_name,
                    "alias":os.environ.get('SEARCH_INDEX_PREFIX')+"-stats-index",
                    "is_write_index":True
                }
        }
    ]
}
for type in stats_types:
    event_type = type.replace("stats-","")
    alias_info = {
        "index": new_stats_index_name,
        "alias": os.environ.get('SEARCH_INDEX_PREFIX')+"-stats-index-"+event_type,
        "is_write_index": True,
        "filter":{
            "term":{"event_type":event_type}
        }
    }
    json_data_set_aliases["actions"].append({"add":alias_info})
res = requests.post(base_url+"_aliases",json=json_data_set_aliases,**req_args)
print(res.text)
stats_indexes = [index for index in indexes if re.sub("-\d{6}$","",index.replace(os.environ.get('SEARCH_INDEX_PREFIX')+"-", "")) in stats_types]
for index in stats_indexes:
    
    from_reindex = index
    to_reindex = os.environ.get('SEARCH_INDEX_PREFIX')+"-stats-index"
    event_type = index_tmp.replace("stats-","")
    body = {
        "source": {"index": from_reindex},
        "dest": {"index": to_reindex},
        "script": {
            "source": "ctx._source['event_type'] = {}".format(event_type),
            "lang": "painless"
        }
    }