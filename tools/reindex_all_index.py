
import sys
import argparse
import os
import requests
from requests.auth import HTTPBasicAuth
from importlib import import_module
import re
import json
from datetime import datetime, timedelta, timezone
import traceback, copy

now = datetime.now(timezone.utc)
today_str = now.strftime("%Y-%m-%dT00:00:00")

def validate_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%dT00:00:00")
    except ValueError:
        raise argparse.ArgumentTypeError(f"無効な日付形式: {date_str}（YYYY-MM-DD の形式で入力してください）")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Elasticsearch v6 から v7 に reindex するツール",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "http_method",
        choices=["http", "https"],
        help="Elasticsearch に接続するプロトコル（http または https）"
    )
    
    parser.add_argument(
        "--es6_port",
        type=int,
        default=9200,
        help="Elasticsearch 6 のポート番号（デフォルト: 9200）"
    )

    parser.add_argument(
        "--es7_host",
        type=str,
        default="elasticsearch7",
        help="Elasticsearch 7 のコンテナ名"
    )
    
    parser.add_argument(
        "--es7_port",
        type=int,
        default=9200,
        help="Elasticsearch 7 のポート番号（デフォルト: 9200）"
    )

    parser.add_argument(
        "--user",
        type=str,
        help="Elasticsearch のユーザー名"
    )

    parser.add_argument(
        "--password",
        type=str,
        help="Elasticsearch のパスワード"
    )

    parser.add_argument(
        "--date",
        type=validate_date,
        default=None,
        help="操作する日付（省略可能）\n"
             "  ・日付なしで実行すると、前日23:59:59までのデータがreindexされる\n"
             "  ・日付付きで実行すると、指定した日の23:59:59までのデータがreindexされる\n"
             "  ・形式: YYYY-MM-DD（例: 2024-02-14）"
    )

    args = parser.parse_args()
    return args

args = parse_arguments()
http_method = args.http_method
user = args.user
password = args.password
es7_host = args.es7_host
es7_port = args.es7_port
gte_date = args.date
auth = HTTPBasicAuth(user, password) if user and password else None

es6_host = os.environ.get('INVENIO_ELASTICSEARCH_HOST','localhost')
es6_port = args.es6_port
version="v7"

es6_url = http_method + "://" + es6_host +":"+es6_port+"/"
es7_url = http_method + "://"+ es7_host +":"+es7_port+"/"
reindex_url = es7_url + "_reindex?pretty&refresh=true&wait_for_completion=true"
template_url = es7_url + "_template/{}"
verify=False
headers = {"Content-Type":"application/json"}
percolator_prefix = "oaiset-"

req_args = {"headers":headers,"verify":verify}
if auth:
    req_args["auth"] = auth

mapping_files = {
    "authors-author-v1.0.0": f"weko-authors/weko_authors/mappings/{version}/authors/author-v1.0.0.json",
    "weko-item-v1.0.0": f"weko-schema-ui/weko_schema_ui/mappings/{version}/weko/item-v1.0.0.json",
}
template_files = {
    "events-stats-index": f"invenio-stats/invenio_stats/contrib/events/{version}/events-v1.json",
    "stats-index": f"invenio-stats/invenio_stats/contrib/aggregations/{version}/aggregation-v1.json",
}
stats_indexes = ["events-stats-celery-task", "events-stats-file-download", "events-stats-file-preview", "events-stats-item-create", "events-stats-record-view", "events-stats-search", "events-stats-top-view", "stats-celery-task", "stats-file-download", "stats-file-preview", "stats-item-create", "stats-record-view", "stats-search", "stats-top-view"]


prefix = os.environ.get('SEARCH_INDEX_PREFIX')

def replace_prefix_index(index_name):
    index_tmp = re.sub(f"^{prefix}-", "", index_name)
    index_tmp = re.sub("-\d{6}$","",index_tmp)
    return index_tmp

# indexとalias一覧取得
print("# get indexes and aliases")
organization_aliases = prefix+"-*"
indexes = requests.get(f"{es6_url}{organization_aliases}",**req_args).json()
indexes_alias = {} # indexとaliasのリスト
for index in indexes:
    aliases = indexes[index].get("aliases",{})
    indexes_alias[index] = aliases

modules_dir = "/code/modules/"
mappings = {}
templates = {}

# ファイルからマッピングデータを取得
print("# get mapping from json file")
for index in indexes_alias:
    index_tmp = replace_prefix_index(index)
    if index_tmp in stats_indexes:
        continue
    if index_tmp not in list(mapping_files.keys()):
        print("## not exists: {}, {}".format(index, index_tmp))
        continue
    path_data = mapping_files[index_tmp]
    file_path = os.path.join(modules_dir, path_data)

    if not os.path.isfile(file_path):
        print("## not exist file: {}".format(file_path))
        continue

    with open(file_path, "r") as json_file:
        mappings[index] = json.loads(json_file.read())

print("# get template from json files")
for index, path in template_files.items():
    file_path = os.path.join(modules_dir, path)
    if not os.path.isfile(file_path):
        print("## not exist file: {}".format(file_path))
        continue
    with open(file_path, "r") as json_file:
        templates[index] = json.loads(
            json_file.read().\
                replace("__SEARCH_INDEX_PREFIX__",prefix+"-")
        )


percolator_body = {"properties": {"query": {"type": "percolator"}}}
# for index in indexes_alias:
for index, mapping in mappings.items():
    print("# start reindex: {}".format(index))

    # target index mapping
    base_index_definition = mappings[index]

    # create speed up setting body
    defalut_number_of_replicas = base_index_definition.get("settings",{}).get("index",{}).get("number_of_replicas",1)
    default_refresh_interval = base_index_definition.get("settings",{}).get("index",{}).get("refresh_interval","1s")
    performance_setting_body = {"index": {"number_of_replicas": 0, "refresh_interval": "-1"}}
    restore_setting_body = {"index": {"number_of_replicas": defalut_number_of_replicas, "refresh_interval": default_refresh_interval}}
    remote = {
        "host": es6_url
    }
    if user and password:
        remote["username"] = user
        remote["password"] = password
        
    json_data_to_es7 = {
        "source": {
            "remote": remote,
            "index": index,
            "query": {
                "bool": {
                    "must": [],
                    "filter": [],
                }
            }
        },
        "dest": {
            "index": index
        }
    }

    query_before_today = {
        "range": {
            "_updated": {
                "lt": today_str
            }
        }
    }

    query_after_specific_date  = {
        "range": {
            "_updated": {
                "gte": gte_date
            }
        }
    }

    filter_id_starts_with  = {
        "script": {
            "script": {
                "source": "doc['_id'].value.startsWith(params.prefix)",
                "params": {
                    "prefix": percolator_prefix
                }
            }
        }
    }

    filter_id_not_starts_with = {
        "script": {
            "script": {
                "source": "!doc['_id'].value.startsWith(params.prefix)",
                "params": {
                    "prefix": percolator_prefix
                }
            }
        }
    }

    # body for setting alias
    json_data_set_aliases = {
        "actions":[]
    }
    for alias in indexes_alias[index]:
        alias_info = {"index": index, "alias": alias}
        if "is_write_index" in indexes_alias[index][alias]:
            alias_info["is_write_index"] = indexes_alias[index][alias]["is_write_index"]
        json_data_set_aliases["actions"].append({"add":alias_info})

    try:
        res = requests.get(es7_url + index, **req_args)
        if res.status_code == 200:
            print(f"## Index {index} already exists, skipping creation.")
        else:
            print(f"## Creating index: {index}")
            res = requests.put(es7_url + index + "?pretty", json=base_index_definition, **req_args)
            if res.status_code != 200:
                print(f"raise error create index: {index}")
                raise Exception(res.text)
            print(f"Created index: {index}")


            if json_data_set_aliases["actions"]:
                res = requests.post(es7_url + "_aliases", json=json_data_set_aliases, **req_args)
                if res.status_code != 200:
                    print(f"## raise error: set aliases: {index}")
                    raise Exception(res.text)
            print("## setting alias for new index")


        index_percolator = index + "-percolator"

        res = requests.get(es7_url + index_percolator, **req_args)
        if res.status_code == 200:
            print(f"## Index {index_percolator} already exists, skipping creation.")
        else:
            print(f"## Creating index: {index_percolator}")
            percolator_definition = copy.deepcopy(base_index_definition)
            percolator_definition["mappings"]["properties"].update(percolator_body["properties"])
            res = requests.put(es7_url + index_percolator + "?pretty", json=percolator_definition, **req_args)
            if res.status_code != 200:
                raise Exception(res.text)
            print(f"Created index: {index}")

        res = requests.put(es7_url + index + "/_settings?pretty", json=performance_setting_body, **req_args)
        if res.status_code != 200:
            raise Exception(res.text)
        print("## speed-up setting for reindex")


        if "author" not in index:
            if gte_date:
                json_data_to_es7["source"]["query"]["bool"]["must"] = [query_after_specific_date]
                json_data_to_es7["source"]["query"]["bool"]["filter"] = [filter_id_not_starts_with]
                res = requests.post(url=reindex_url, json=json_data_to_es7, **req_args)
                if res.status_code != 200:
                    raise Exception(res.text)
                json_data_to_es7["source"]["query"]["bool"]["must"] = []
                json_data_to_es7["source"]["query"]["bool"]["filter"] = [filter_id_starts_with]
                json_data_to_es7["dest"]["index"] = index_percolator
                res = requests.post(url=reindex_url, json=json_data_to_es7, **req_args)
                if res.status_code != 200:
                    raise Exception(res.text)
                print("## Second reindex from ES6 to ES7 (>= today 00:00:00)")
            else:
                json_data_to_es7["source"]["query"]["bool"]["must"] = [query_before_today]
                json_data_to_es7["source"]["query"]["bool"]["filter"] = [filter_id_not_starts_with]
                res = requests.post(url=reindex_url, json=json_data_to_es7, **req_args)
                if res.status_code != 200:
                    raise Exception(res.text)
                print("## First reindex from ES6 to ES7 (< today 00:00:00)")
        else:
            if gte_date:
                json_data_to_es7["source"]["query"]["bool"]["filter"] = [filter_id_not_starts_with]
                res = requests.post(url=reindex_url, json=json_data_to_es7, **req_args)
                json_data_to_es7["source"]["query"]["bool"]["filter"] = [filter_id_starts_with]
                json_data_to_es7["dest"]["index"] = index_percolator
                res = requests.post(url=reindex_url, json=json_data_to_es7, **req_args)
                if res.status_code != 200:
                    raise Exception(res.text)
                print("## [Author] Reindex from ES6 to ES7 ALL")
            else:
                print("## [Author] Reindex next time")

        res = requests.put(es7_url + index + "/_settings?pretty", json=restore_setting_body, **req_args)
        if res.status_code != 200:
            raise Exception(res.text)

        print("# end reindex: {}\n".format(index))
    except Exception as e:
        print("##raise error: {}".format(index))
        print(traceback.format_exc())


def create_stats_index(index_name):
    alias_actions = []
    print("## start create stats index: {}".format(index_name))
    index_with_prefix = f"{prefix}-{index_name}"
    new_index_name = f"{index_with_prefix}-000001"
    filename_without_ext = template_files[index_name].split("/")[-1].replace(".json","")
    template_name = f"{prefix}-{filename_without_ext}"
    template_url_event_stats = template_url.format(template_name)
    # template登録
    print("### put template")
    res = requests.put(template_url_event_stats,json=templates[index_name],**req_args)
    if res.status_code!=200:
        print("### raise error: put template")
        raise Exception(res.text)
    # index作成
    res = requests.get(es7_url + new_index_name, **req_args)
    if res.status_code == 200:
        print(f"## Index {index} already exists, skipping creation.")
    else:
        print("### craete index")
        res = requests.put(es7_url+new_index_name+"?pretty",**req_args)
        if res.status_code!=200:
            print("## raise error: create index")
            raise Exception(res.text)

        # エイリアス登録用データ作成
        alias_actions.append(
            {
                "add": {
                    "index":new_index_name,
                    "alias":index_with_prefix,
                    "is_write_index":True
                }
            }
        )
    return alias_actions

def stats_reindex(stats_types, stats_prefix):
    print("## start reindex stats index: {}".format(stats_prefix))
    stats_indexes = [index for index in indexes_alias if replace_prefix_index(index) in stats_types]
    from_sizes = {}

    # 既存indexのsizeを調べる
    def get_index_size(alias, es_url):
        size_url = f"{es_url}{alias}/_stats/store"
        res = requests.get(url=size_url, **req_args)
        if res.status_code == 200:
            stats = res.json()
            for real_index, data in stats['indices'].items():
                return data['total']['store']['size_in_bytes']
        else:
            print("### raise error: failed to get size for alias: {}".format(alias))
            raise Exception(res.text)

    for index in stats_indexes:
        from_sizes[index] = get_index_size(index, es6_url)

    to_reindex = f"{prefix}-{stats_prefix}-index"
    to_size = get_index_size(to_reindex, es7_url)

    max_size = 50  # GB
    size_limit = max_size * 1024 * 1024 * 1024  # byteに変換する

    for index in stats_indexes:
        print("### reindex: {}".format(index))
        from_reindex = index
        to_reindex = f"{prefix}-{stats_prefix}-index"
        event_type = replace_prefix_index(index).replace(f"{stats_prefix}-","")

        from_size = from_sizes[from_reindex]

        if from_size + to_size > size_limit:
            print(f"### Performing rollover for index: {to_reindex}")
            rollover_url = es7_url + "{}/_rollover".format(to_reindex)
            res = requests.post(url=rollover_url, **req_args)
            if res.status_code != 200:
                print(f"### raise error: rollover failed for index: {to_reindex}")
                raise Exception(res.text)
            to_size = 0
            print(f"### New to_index size after rollover: {to_size} bytes")

        # Reindex process
        remote = {
            "host": es6_url,
        }

        if user and password:
            remote["username"] = user
            remote["password"] = password

        if gte_date:
            source_index = {
                "remote": remote,
                "index": from_reindex,
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": gte_date
                        }
                    }
                }
            }
        else:
            source_index = {
                "remote": remote,
                "index": from_reindex,
                "query": {
                    "range": {
                        "timestamp": {
                            "lt": today_str
                        }
                    }
                }
            }

        body = {
            "source": source_index,
            "dest": {"index": to_reindex},
            "script": {
                "source": """
                    // Update the event_type field
                    ctx._source['event_type'] = params.event_type;
                    // Check if the event_type is file-download or file-preview
                    if (params.event_type == 'file-download' || params.event_type == 'file-preview') {
                        // Append event_type to id
                        String originalId = ctx._id;
                        ctx._id = originalId + '-' + params.event_type;
                        // Append event_type to unique_id if exists
                        if (ctx._source.containsKey('unique_id')) {
                            String originalUniqueId = ctx._source['unique_id'];
                            ctx._source['unique_id'] = originalUniqueId + '-' + params.event_type;
                        }
                    }
                """,
                "lang": "painless",
                "params": {"event_type": event_type}
            }
        }
        res = requests.post(url=reindex_url, json=body, **req_args)
        if res.status_code != 200:
            print(f"### raise error: reindex: {from_reindex}")
            raise Exception(res.text)

        to_size += from_size

event_stats_types = [
    "events-stats-celery-task",
    "events-stats-file-download",
    "events-stats-file-preview",
    "events-stats-item-create",
    "events-stats-record-view",
    "events-stats-search",
    "events-stats-top-view",
]

stats_types = [
    "stats-celery-task",
    "stats-file-download",
    "stats-file-preview",
    "stats-item-create",
    "stats-record-view",
    "stats-search",
    "stats-top-view",
]

alias_actions = []
alias_actions += create_stats_index("events-stats-index")
alias_actions += create_stats_index("stats-index")

if alias_actions:
    res = requests.post(es7_url+"_aliases",json={"actions":alias_actions},**req_args)
    if res.status_code!=200:
        print("## raise error: put aliases")
        raise Exception(res.text)

stats_reindex(event_stats_types, "events-stats")
stats_reindex(stats_types, "stats")
