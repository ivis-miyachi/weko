import argparse
import copy
import json
import os
import re
import sys
import traceback
import uuid
from builtins import print as builtin_print

from datetime import datetime, timedelta, timezone

import requests
import psycopg2
from requests.auth import HTTPBasicAuth


def print(*args, **kwargs):
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    builtin_print(f"[{timestamp}]", *args, **kwargs)


def validate_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%dT00:00:00")
    except ValueError:
        raise argparse.ArgumentTypeError(
            "無効な日付形式: {}（YYYY-MM-DD の形式で入力してください）".format(date_str)
        )


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Elasticsearch v7 から OpenSearch に reindex するツール\n"
            "安全版: 日付指定がない場合のみ date window 分割を行う\n"
            "※ --date を指定した場合は元の指定範囲をそのまま維持する"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "http_method",
        choices=["http", "https"],
        help="Elasticsearch に接続するプロトコル（http または https）",
    )
    parser.add_argument(
        "prefix",
        type=str,
        help="Elasticsearch のインデックス名の接頭辞（例: tenant1）",
    )
    parser.add_argument(
        "--es7_host",
        type=str,
        default="elasticsearch7",
        help="Elasticsearch 7 のコンテナ名",
    )
    parser.add_argument(
        "--es7_port",
        type=str,
        default="9200",
        help="Elasticsearch 7 のポート番号（デフォルト: 9200）",
    )
    parser.add_argument(
        "--os_host",
        type=str,
        default="opensearch",
        help="Opensearch のコンテナ名",
    )
    parser.add_argument(
        "--os_port",
        type=str,
        default="9200",
        help="Opensearch のポート番号（デフォルト: 9200）",
    )
    parser.add_argument("--user", type=str, help="Elasticsearch のユーザー名")
    parser.add_argument("--password", type=str, help="Elasticsearch のパスワード")

    parser.add_argument("--db-host", default=os.getenv("PGHOST", "pgpool"), help="PostgreSQL のホスト")
    parser.add_argument("--db-port", default=os.getenv("PGPORT", "5432"), help="PostgreSQL のポート")
    parser.add_argument("--db-name", default=os.getenv("PGDATABASE", "invenio"), help="PostgreSQL のDB名")
    parser.add_argument("--db-user", default=os.getenv("PGUSER", "invenio"), help="PostgreSQL のユーザー")
    parser.add_argument("--db-password", default=os.getenv("PGPASSWORD", "dbpass123"), help="PostgreSQL のパスワード")

    parser.add_argument(
        "--db-fetch-size",
        type=int,
        default=5000,
        help="DBカーソルの fetchmany サイズ",
    )
    parser.add_argument(
        "--terms-chunk-size",
        type=int,
        default=5000,
        help="1回の terms に渡す pk_id 件数",
    )

    parser.add_argument(
        "--user_os",
        type=str,
        default="admin",
        help="Opensearch のユーザー名",
    )
    parser.add_argument(
        "--password_os",
        type=str,
        default="WekoOpensearch123!",
        help="Opensearch のパスワード",
    )
    parser.add_argument(
        "--date",
        type=validate_date,
        default=None,
        help=(
            "操作する日付（省略可能）\n"
            "  ・日付なしで実行すると、前日23:59:59までのデータがreindexされる\n"
            "  ・日付付きで実行すると、指定した日の23:59:59までのデータがreindexされる\n"
            "  ・形式: YYYY-MM-DD（例: 2024-02-14）"
        ),
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=0,
        help="reindex API のタイムアウト秒。0以下なら timeout=None として無制限扱い",
    )
    parser.add_argument(
        "--date-window-days",
        type=int,
        default=30,
        help=(
            "日付指定がない場合、何日単位で分割して再インデックスするか。0 で分割しない\n"
            "※ --date を指定した場合は元の仕様を維持し、分割ロジックは適用しない"
        ),
    )

    parser.add_argument(
        "--subsplit-window-days",
        type=int,
        default=14,
        help=(
            "重い date window を再分割するときの最小単位(日数)。\n"
            "過剰な分割を避けるため、既定値は 14 日単位"
        ),
    )
    parser.add_argument(
        "--heavy-window-doc-threshold",
        type=int,
        default=50000,
        help=(
            "重い window と判定するドキュメント数の閾値。\n"
            "過剰な分割を避けるため、既定値は 5 万件"
        ),
    )


    parser.add_argument(
        "--auto-split-threshold-mb",
        type=int,
        default=1024,
        help=(
            "大きい index のみ date window 分割を行う閾値(MB)。\n"
            "この値未満なら従来どおり 1 回で処理する"
        ),
    )
    parser.add_argument(
        "--safe-limit-mb",
        type=int,
        default=5,
        help="weko-item の安全側の source.size 上限(MB)。小さくするほど分割が安全になる",
    )
    return parser.parse_args()


class ReindexEs7ToOsRunner(object):
    def __init__(self, args):
        self.args = args
        self.run_id = str(uuid.uuid4())[:8]
        self.http_method = args.http_method
        self.user = args.user
        self.password = args.password
        self.user_os = args.user_os
        self.password_os = args.password_os
        self.gte_date = args.date
        self.db_host = args.db_host
        self.db_port = args.db_port
        self.db_name = args.db_name
        self.db_user = args.db_user
        self.db_password = args.db_password
        self.db_json_column = "json"
        self.db_table = "authors"
        self.pkid_field = "pk_id"
        self.db_fetch_size = args.db_fetch_size
        self.terms_chunk_size = args.terms_chunk_size
        self.request_timeout = int(getattr(args, "request_timeout", 1800))
        if self.request_timeout <= 0:
            self.request_timeout = None

        self.subsplit_window_days = max(1, int(getattr(args, "subsplit_window_days", 14)))
        self.heavy_window_doc_threshold = max(1, int(getattr(args, "heavy_window_doc_threshold", 50000)))

        self.date_window_days = max(0, int(getattr(args, "date_window_days", 30)))
        self.auto_split_threshold_mb = max(0, int(getattr(args, "auto_split_threshold_mb", 1024)))
        self.auto_split_threshold_bytes = self.auto_split_threshold_mb * 1024 * 1024
        self.safe_limit_mb = max(1, int(getattr(args, "safe_limit_mb", 5)))
        self.safe_limit_bytes = self.safe_limit_mb * 1024 * 1024

        self.version = "os-v2"
        self.modules_dir = "/code/modules/"
        self.percolator_prefix = "oaiset-"
        self.prefix = args.prefix
        self.today_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00")

        self.es7_url = "{}://{}:{}/".format(self.http_method, args.es7_host, args.es7_port)
        self.os_url = "https://{}:{}/".format(args.os_host, args.os_port)
        self.reindex_url = self.os_url + "_reindex?pretty&refresh=true&wait_for_completion=true"
        self.template_url = self.os_url + "_template/{}"

        self.es_auth = HTTPBasicAuth(self.user, self.password) if self.user and self.password else None
        self.os_auth = (
            HTTPBasicAuth(self.user_os, self.password_os) if self.user_os and self.password_os else None
        )

        self.req_args = {"headers": {"Content-Type": "application/json"}, "verify": False}
        self.os_req_args = {"headers": {"Content-Type": "application/json"}, "verify": False}
        if self.request_timeout is not None:
            self.req_args["timeout"] = self.request_timeout
            self.os_req_args["timeout"] = self.request_timeout
        if self.es_auth:
            self.req_args["auth"] = self.es_auth
        if self.os_auth:
            self.os_req_args["auth"] = self.os_auth

        self.mapping_files = {
            "authors-author-v1.0.0": "weko-authors/weko_authors/mappings/{}/authors/author-v1.0.0.json".format(
                self.version
            ),
            "weko-item-v1.0.0": "weko-schema-ui/weko_schema_ui/mappings/{}/weko/item-v1.0.0.json".format(
                self.version
            ),
        }
        self.template_files = {
            "events-stats-index": "invenio-stats/invenio_stats/contrib/events/{}/events-v1.json".format(
                self.version
            ),
            "stats-index": "invenio-stats/invenio_stats/contrib/aggregations/{}/aggregation-v1.json".format(
                self.version
            ),
        }
        self.reindex_target_names = [
            "events-stats-celery-task",
            "events-stats-file-download",
            "events-stats-file-preview",
            "events-stats-item-create",
            "events-stats-record-view",
            "events-stats-search",
            "events-stats-top-view",
            "stats-celery-task",
            "stats-file-download",
            "stats-file-preview",
            "stats-item-create",
            "stats-record-view",
            "stats-search",
            "stats-top-view",
        ]
        self.stats_index_names = ["events-stats-index", "stats-index"]

        self.indexes_alias = {}
        self.es7_mappings_cache = {}
        self.es7_settings_cache = {}
        self.source_index_size_cache = {}
        self.mappings = {}
        self.templates = {}
        self.had_errors = False

    def _log(self, level, message):
        print("[{}] [run_id={}] {}".format(level, self.run_id, message))

    def _log_info(self, message):
        self._log("INFO", message)

    def _log_warn(self, message):
        self._log("WARN", message)

    def _log_error(self, message):
        self._log("ERROR", message)

    def _log_step_start(self, step_name):
        self._log("STEP", "START {}".format(step_name))

    def _log_step_end(self, step_name, status="OK"):
        self._log("STEP", "END {} status={}".format(step_name, status))

    def _request_or_raise(self, method, url, request_args, **kwargs):
        merged_args = dict(request_args)
        merged_args.update(kwargs)
        response = requests.request(method, url, **merged_args)
        if response.status_code != 200:
            raise Exception(response.text)
        return response

    def _request_es7_or_raise(self, method, url, **kwargs):
        return self._request_or_raise(method, url, self.req_args, **kwargs)

    def _request_os_or_raise(self, method, url, **kwargs):
        return self._request_or_raise(method, url, self.os_req_args, **kwargs)

    def _run_os_reindex_or_raise(self, body):
        return self._request_os_or_raise("POST", self.reindex_url, json=body)

    def _replace_prefix_index(self, index_name):
        index_tmp = re.sub(r"^{}-".format(self.prefix), "", index_name)
        if "stats" in index_tmp:
            index_tmp = re.sub(r"-(?:\d{6}|\d{4})$", "", index_tmp)
        return index_tmp

    def _wait_for_shards_started(self, host, index_name, request_args, timeout=120):
        response = self._request_or_raise(
            "GET",
            host
            + "_cluster/health/{}?wait_for_status=yellow&wait_for_active_shards=1&timeout={}s".format(
                index_name, timeout
            ),
            request_args
        )
        if response.status_code != 200 or response.json().get("timed_out"):
            raise Exception("Timeout waiting for index to be ready: {}".format(index_name))

    def _deep_merge_props(self, dst, src):
        for field, field_def in src.items():
            if field not in dst:
                dst[field] = field_def
            elif isinstance(field_def, dict) and "properties" in field_def:
                dst[field].setdefault("properties", {})
                self._deep_merge_props(dst[field]["properties"], field_def["properties"])

    def _merge_es7_mapping(self, index_name, base_definition):
        es7_mappings = self.es7_mappings_cache.get(index_name, {})
        if not es7_mappings:
            self._log_warn("no ES7 mapping cached for {}, skipping merge".format(index_name))
            return base_definition

        if "properties" in es7_mappings:
            es7_props = es7_mappings["properties"]
        else:
            first_type = next(iter(es7_mappings), None)
            es7_props = es7_mappings[first_type].get("properties", {}) if first_type else {}

        base_props = base_definition.setdefault("mappings", {}).setdefault("properties", {})
        before_keys = set(base_props.keys())
        self._deep_merge_props(base_props, es7_props)
        added = [key for key in base_props if key not in before_keys]
        self._log_info("merged ES7 mapping (top-level added: {}): {}".format(len(added), added))
        return base_definition

    def _load_indexes_and_aliases(self):
        self._log_step_start("load_indexes_and_aliases")
        self._log_info("get indexes and aliases")
        organization_aliases = self.prefix + "-*"
        indexes = self._request_es7_or_raise("GET", self.es7_url + organization_aliases).json()

        for index_name in indexes:
            self.indexes_alias[index_name] = indexes[index_name].get("aliases", {})
            self.es7_mappings_cache[index_name] = indexes[index_name].get("mappings", {})
            self.es7_settings_cache[index_name] = indexes[index_name].get("settings", {})

        self._log_step_end("load_indexes_and_aliases")

    def _load_mapping_files(self):
        self._log_step_start("load_mapping_files")
        self._log_info("get mapping from json file")

        for index_name in self.indexes_alias:
            index_tmp = self._replace_prefix_index(index_name)
            if index_tmp in self.reindex_target_names:
                continue
            if index_tmp not in self.mapping_files:
                self._log_warn("mapping target not found: {}, {}".format(index_name, index_tmp))
                continue

            file_path = os.path.join(self.modules_dir, self.mapping_files[index_tmp])
            if not os.path.isfile(file_path):
                self._log_warn("mapping file not found: {}".format(file_path))
                continue

            with open(file_path, "r") as json_file:
                self.mappings[index_name] = json.loads(json_file.read())

        self._log_step_end("load_mapping_files")

    def _load_template_files(self):
        self._log_step_start("load_template_files")
        self._log_info("get template from json files")

        for index_name, path in self.template_files.items():
            file_path = os.path.join(self.modules_dir, path)
            if not os.path.isfile(file_path):
                self._log_warn("template file not found: {}".format(file_path))
                continue

            with open(file_path, "r") as json_file:
                self.templates[index_name] = json.loads(
                    json_file.read().replace("__SEARCH_INDEX_PREFIX__", self.prefix + "-")
                )

        self._log_step_end("load_template_files")

    def _make_alias_actions(self, index_name):
        actions = []
        for alias in self.indexes_alias[index_name]:
            alias_info = {"index": index_name, "alias": alias}
            if "is_write_index" in self.indexes_alias[index_name][alias]:
                alias_info["is_write_index"] = self.indexes_alias[index_name][alias]["is_write_index"]
            actions.append({"add": alias_info})
        return {"actions": actions}

    def _get_source_number_of_shards(self, index_name):
        settings = self.es7_settings_cache.get(index_name, {})
        index_settings = settings.get("index", {})
        shard_count = index_settings.get("number_of_shards")
        return int(shard_count) if shard_count is not None else None

    def _apply_source_shard_count(self, index_definition, index_name):
        shard_count = self._get_source_number_of_shards(index_name)
        if shard_count is None:
            self._log_warn("source shard count not found for {}, keeping template/default setting".format(index_name))
            return index_definition

        settings = index_definition.setdefault("settings", {})
        current_shards = settings.get("number_of_shards")
        if current_shards is None and isinstance(settings.get("index"), dict):
            current_shards = settings["index"].get("number_of_shards")

        settings["number_of_shards"] = shard_count

        if isinstance(settings.get("index"), dict):
            settings["index"]["number_of_shards"] = shard_count

        self._log_info(
            "use source shard count for {}: current={} source={}".format(
                index_name,
                current_shards,
                shard_count,
            )
        )
        return index_definition

    def _get_index_size_in_bytes(self, index_name):
        stats = self._request_es7_or_raise("GET", self.es7_url + index_name + "/_stats?metric=store&human=false").json()
        index_stats = stats.get("_all", {}).get("total", {}).get("store", {})
        size_in_bytes = index_stats.get("size_in_bytes", 0)
        if size_in_bytes <= 0:
            self._log_warn("index size unknown for {}".format(index_name))
        return int(size_in_bytes)

    def _get_source_index_size(self, index_name):
        if index_name not in self.source_index_size_cache:
            self.source_index_size_cache[index_name] = self._get_index_size_in_bytes(index_name)
        return self.source_index_size_cache[index_name]

    def _calc_percentile(self, values, percentile):
        """Return the requested percentile value from the supplied numeric values."""
        if not values:
            return 0
        sorted_values = sorted(values)
        idx = int((len(sorted_values) - 1) * (percentile / 100.0))
        return sorted_values[idx]

    def _should_split_index_by_date_window(self, index_name):
        if self.gte_date:
            return False
        if self.date_window_days <= 0:
            return False
        if self.auto_split_threshold_mb <= 0:
            return True

        index_size = self._get_source_index_size(index_name)
        if index_size <= 0:
            return False

        should_split = index_size >= self.auto_split_threshold_bytes
        self._log_info(
            "index {} size={} bytes threshold={} bytes split_by_date_window={}".format(
                index_name,
                index_size,
                self.auto_split_threshold_bytes,
                should_split,
            )
        )
        return should_split

    def _parse_window_start_end(self, start_str, end_str):
        """Parse a window start and end timestamp into UTC datetime objects."""
        start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return start_dt, end_dt

    def _count_docs_in_window(self, index_name, start_str, end_str):
        """Count documents whose _updated field falls within the provided window."""
        if start_str is None and end_str is None:
            return 0
        if start_str is None:
            query = {"range": {"_updated": {"lt": end_str}}}
        elif end_str is None:
            query = {"range": {"_updated": {"gte": start_str}}}
        else:
            query = {"range": {"_updated": {"gte": start_str, "lt": end_str}}}

        body = {"query": {"bool": {"must": [query]}}}
        try:
            response = requests.post(self.es7_url + index_name + "/_count", json=body, **self.req_args)
            if response.status_code != 200:
                self._log_warn("window count skipped for {} {} -> {}: {}".format(index_name, start_str, end_str, response.text))
                return 0
            return int(response.json().get("count", 0))
        except Exception:
            self._log_warn("window count failed for {} {} -> {}".format(index_name, start_str, end_str))
            self._log_error(traceback.format_exc())
            return 0

    def _split_heavy_window(self, index_name, start_str, end_str, depth=0):
        """Split a large date window into smaller windows when document volume is high."""
        if start_str is None and end_str is None:
            return [(start_str, end_str)]

        if start_str is None:
            start_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        elif end_str is None:
            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            end_dt = datetime.now(timezone.utc)
        else:
            start_dt, end_dt = self._parse_window_start_end(start_str, end_str)

        total_days = max(1, int((end_dt - start_dt).total_seconds() // 86400) + 1)
        window_count = self._count_docs_in_window(index_name, start_str, end_str)
        self._log_info(
            "window check: {} -> {} docs={} days={} threshold={} depth={}".format(
                start_str,
                end_str,
                window_count,
                total_days,
                self.heavy_window_doc_threshold,
                depth,
            )
        )

        if window_count <= self.heavy_window_doc_threshold or total_days <= self.subsplit_window_days:
            return [(start_str, end_str)]

        split_days = max(1, min(self.subsplit_window_days, max(1, total_days // 2)))
        if split_days >= total_days:
            self._log_info(
                "stop recursive split: {} -> {} because split_days={} total_days={} would not shrink the window".format(
                    start_str,
                    end_str,
                    split_days,
                    total_days,
                )
            )
            return [(start_str, end_str)]

        sub_windows = []
        current_dt = start_dt
        while current_dt < end_dt:
            next_dt = min(current_dt + timedelta(days=split_days), end_dt)
            if next_dt <= current_dt:
                break
            sub_windows.append(
                (
                    current_dt.strftime("%Y-%m-%dT00:00:00"),
                    next_dt.strftime("%Y-%m-%dT00:00:00"),
                )
            )
            current_dt = next_dt

        if not sub_windows or (len(sub_windows) == 1 and sub_windows[0] == (start_str, end_str)):
            self._log_info(
                "stop recursive split: {} -> {} because the child window would be identical to the parent".format(
                    start_str,
                    end_str,
                )
            )
            return [(start_str, end_str)]

        expanded = []
        for sub_start, sub_end in sub_windows:
            expanded.extend(self._split_heavy_window(index_name, sub_start, sub_end, depth + 1))
        return expanded

    def _get_earliest_updated_datetime(self, index_name):
        """Fetch the earliest _updated timestamp found in the source index."""
        try:
            body = {
                "size": 1,
                "sort": [{"_updated": {"order": "asc"}}],
                "_source": False,
                "query": {"exists": {"field": "_updated"}},
            }
            response = requests.post(self.es7_url + index_name + "/_search", json=body, **self.req_args)
            if response.status_code != 200:
                self._log_warn("earliest updated date lookup failed for {}: {}".format(index_name, response.text))
                return None
            hits = response.json().get("hits", {}).get("hits", [])
            if not hits:
                self._log_warn("no _updated values found for {}".format(index_name))
                return None

            value = hits[0].get("sort", [None])[0]
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
            if isinstance(value, str):
                if value.endswith("Z"):
                    value = value[:-1] + "+00:00"
                return datetime.fromisoformat(value).astimezone(timezone.utc)
            return None
        except Exception:
            self._log_warn("failed to detect earliest updated date for {}".format(index_name))
            self._log_error(traceback.format_exc())
            return None

    def _iter_reindex_windows(self, index_name=None):
        """Yield the date windows used to reindex the source index in batches."""
        if self.gte_date:
            return [(self.gte_date, None)]
        if self.date_window_days <= 0:
            return [(None, self.today_str)]

        end_dt = datetime.now(timezone.utc)
        earliest_dt = self._get_earliest_updated_datetime(index_name) if index_name else None
        if earliest_dt is not None:
            earliest_dt = earliest_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            earliest_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)

        windows = []
        current_end = end_dt
        while True:
            current_start = current_end - timedelta(days=self.date_window_days)
            if current_start < earliest_dt:
                current_start = earliest_dt

            start_str = current_start.strftime("%Y-%m-%dT00:00:00")
            end_str = current_end.strftime("%Y-%m-%dT00:00:00")
            windows.insert(0, (start_str, end_str))

            if current_start <= earliest_dt:
                break
            current_end = current_start
        return windows

    def _create_remote(self):
        remote = {"host": self.es7_url}
        if self.user and self.password:
            remote["username"] = self.user
            remote["password"] = self.password
        return remote

    def _create_updated_query(self):
        return {"range": {"_updated": {"gte": self.gte_date}}}

    def _expand_heavy_windows(self, index_name):
        """Expand the base date windows into smaller sub-windows for large indexes."""
        windows = self._iter_reindex_windows(index_name=index_name)
        expanded = []
        for start_str, end_str in windows:
            expanded.extend(self._split_heavy_window(index_name, start_str, end_str, depth=0))
        self._log_info("expanded reindex windows count={} (before={} after={})".format(len(expanded), len(windows), len(expanded)))
        return expanded


    def _decide_reindex_size_for_weko_item(self, index_name):
        """Estimate a safe source.size value for weko-item to avoid oversized reindex requests."""
        sample_count = 300
        default_batch_size = 1000
        minimum_batch_size = 50

        try:
            self._log_info("precheck start: {} (sample={})".format(index_name, sample_count))
            body = {
                "size": sample_count,
                "sort": ["_doc"],
                "_source": True,
                "query": {"match_all": {}},
            }
            response = requests.post(self.es7_url + index_name + "/_search", json=body, **self.req_args)
            if response.status_code != 200:
                self._log_warn("precheck skipped (search failed): {}".format(index_name))
                return None

            hits = response.json().get("hits", {}).get("hits", [])
            if not hits:
                self._log_warn("precheck skipped (no docs): {}".format(index_name))
                return None

            doc_sizes = []
            for hit in hits:
                source_doc = hit.get("_source", {})
                doc_sizes.append(len(json.dumps(source_doc, ensure_ascii=False).encode("utf-8")))

            avg_size = int(sum(doc_sizes) / len(doc_sizes))
            p95_size = self._calc_percentile(doc_sizes, 95)
            max_size = max(doc_sizes)
            est_avg_total = avg_size * default_batch_size
            est_p95_total = p95_size * default_batch_size

            self._log_info(
                "precheck stats: avg={} bytes, p95={} bytes, max={} bytes".format(
                    avg_size, p95_size, max_size
                )
            )
            self._log_info(
                "precheck estimate@size1000: avg_total={} bytes, p95_total={} bytes, limit={} bytes".format(
                    est_avg_total, est_p95_total, self.safe_limit_bytes
                )
            )

            if est_avg_total <= self.safe_limit_bytes and est_p95_total <= self.safe_limit_bytes:
                self._log_info("precheck result: use lower safe limit (omit source.size)")
                return None

            by_avg = self.safe_limit_bytes // max(avg_size, 1)
            by_p95 = self.safe_limit_bytes // max(p95_size, 1)
            decided_size = min(default_batch_size, by_avg, by_p95)
            decided_size = max(minimum_batch_size, int(decided_size))
            self._log_info("precheck result: set source.size={}".format(decided_size))
            return decided_size
        except Exception:
            self._log_warn("precheck failed to decide size, use default")
            self._log_error(traceback.format_exc())
            return None

    def _make_updated_range_query(self):
        if self.gte_date:
            return {"range": {"_updated": {"gte": self.gte_date}}}
        return {"range": {"_updated": {"lt": self.today_str}}}

    def _ensure_target_indexes(self, index_name, prepared_index_definition, percolator_body):
        index_percolator = index_name + "-percolators"

        response = requests.get(self.os_url + index_name, **self.os_req_args)

        if response.status_code == 200:
            self._log_info("Index {} already exists, skipping creation.".format(index_name))
        else:
            self._log_step_start("create_index:{}".format(index_name))
            self._log_info("Creating index: {}".format(index_name))
            self._request_os_or_raise("PUT", self.os_url + index_name + "?pretty", json=prepared_index_definition)
            alias_actions = self._make_alias_actions(index_name)
            if alias_actions["actions"]:
                self._request_os_or_raise("POST", self.os_url + "_aliases", json=alias_actions)
            self._log_info("Created index: {}".format(index_name))
            self._log_step_end("create_index:{}".format(index_name))

        response = requests.get(self.os_url + index_percolator, **self.os_req_args)
        if response.status_code == 200:
            self._log_info("Index {} already exists, skipping creation.".format(index_percolator))
        else:
            self._log_step_start("create_percolator:{}".format(index_percolator))
            self._log_info("Creating index: {}".format(index_percolator))
            percolator_definition = copy.deepcopy(prepared_index_definition)
            percolator_definition.setdefault("mappings", {}).setdefault("properties", {}).update(
                percolator_body["properties"]
            )
            percolator_definition = self._apply_source_shard_count(percolator_definition, index_percolator)
            self._request_os_or_raise(
                "PUT", self.os_url + index_percolator + "?pretty", json=percolator_definition
            )
            self._log_info("Created index: {}".format(index_percolator))
            self._log_step_end("create_percolator:{}".format(index_percolator))

        return index_percolator

    def _reindex_non_author(self, index_name, index_percolator):
        reindex_body = {
            "source": {
                "remote": self._create_remote(),
                "index": index_name,
                "query": {"bool": {"must": [], "filter": []}},
            },
            "dest": {"index": index_name},
        }

        if self._replace_prefix_index(index_name) == "weko-item-v1.0.0":
            prechecked_size = self._decide_reindex_size_for_weko_item(index_name)
            if prechecked_size:
                reindex_body["source"]["size"] = prechecked_size


        if self.gte_date:
            main_body = copy.deepcopy(reindex_body)
            main_body["source"]["query"]["bool"]["must"].append(self._create_updated_query())
            self._log_info("start reindex {} (>= date filter)".format(index_name))
            self._run_os_reindex_or_raise(main_body)
            self._log_info("end reindex {} (>= date filter)".format(index_name))

            percolator_body = {
                "source": {
                    "remote": self._create_remote(),
                    "index": index_percolator,
                    "query": {"bool": {"must": [], "filter": []}},
                },
                "dest": {"index": index_percolator},
            }
            self._log_info("start reindex {} (full percolator copy)".format(index_percolator))
            self._run_os_reindex_or_raise(percolator_body)
            self._log_info("end reindex {} (full percolator copy)".format(index_percolator))
            return

        if self.date_window_days > 0 and self._should_split_index_by_date_window(index_name):
            # windows = self._iter_reindex_windows()
            windows = self._expand_heavy_windows(index_name)
            self._log_info("split reindex windows count={} for index {}".format(len(windows), index_name))
            for idx, (start_ts, end_ts) in enumerate(windows, start=1):
                body_main = copy.deepcopy(reindex_body)
                if start_ts and end_ts:
                    body_main["source"]["query"]["bool"]["must"] = [
                        {"range": {"_updated": {"gte": start_ts, "lt": end_ts}}}
                    ]
                elif start_ts:
                    body_main["source"]["query"]["bool"]["must"] = [
                        {"range": {"_updated": {"gte": start_ts}}}
                    ]
                elif end_ts:
                    body_main["source"]["query"]["bool"]["must"] = [
                        {"range": {"_updated": {"lt": end_ts}}}
                    ]
                else:
                    body_main["source"]["query"]["bool"]["must"] = []

                self._log_info(
                    "window {}/{}: range {} -> {} for index {}".format(
                        idx,
                        len(windows),
                        start_ts if start_ts is not None else "*",
                        end_ts if end_ts is not None else "*",
                        index_name,
                    )
                )
                self._run_os_reindex_or_raise(body_main)
                self._log_info("reindex window ok: {} -> {}".format(start_ts, end_ts))
        else:
            main_body = copy.deepcopy(reindex_body)
            self._log_info("start reindex {} (full index)".format(index_name))
            self._run_os_reindex_or_raise(main_body)
            self._log_info("end reindex {} (full index)".format(index_name))

        percolator_body = {
            "source": {
                "remote": self._create_remote(),
                "index": index_percolator,
                "query": {"bool": {"must": [], "filter": []}},
            },
            "dest": {"index": index_percolator},
        }
        self._log_info("start reindex {} (full percolator copy)".format(index_percolator))
        self._run_os_reindex_or_raise(percolator_body)
        self._log_info("end reindex {} (full percolator copy)".format(index_percolator))

    def _validate_identifier(self, identifier_name, label):
        """Validate that a database identifier is safe to use in SQL queries."""
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier_name):
            raise ValueError("Invalid {}: {}".format(label, identifier_name))
        return identifier_name

    def _iter_author_pkids_from_db(self):
        """Yield author pk_id values from the database in update order."""
        json_col = self._validate_identifier(self.db_json_column, "db-json-column")
        table = self._validate_identifier(self.db_table, "db-table")

        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )

        where_parts = ["{}->>'pk_id' IS NOT NULL".format(json_col)]
        sql_params = []

        if self.gte_date:
            where_parts.append("updated >= %s")
            sql_params.append(self.gte_date)

        sql = (
            "SELECT {}->>'pk_id' AS pk_id ".format(json_col)
            + "FROM {} ".format(table)
            + "WHERE {} ".format(" AND ".join(where_parts))
            + "ORDER BY updated ASC"
        )

        cursor = conn.cursor(name="authors_pkid_cursor")
        cursor.itersize = self.db_fetch_size
        cursor.execute(sql, sql_params)

        try:
            while True:
                rows = cursor.fetchmany(self.db_fetch_size)
                if not rows:
                    break
                for row in rows:
                    yield row[0]
        finally:
            cursor.close()
            conn.close()

    def _chunked(self, iterable, chunk_size):
        """Yield contiguous chunks of the provided iterable."""
        if chunk_size <= 0:
            raise ValueError("terms-chunk-size must be > 0")

        chunk = []
        for item in iterable:
            chunk.append(item)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def _build_author_reindex_body(self, source_index, dest_index, pkids=None, percolator_prefix=None):
        """Build the reindex request body used for author documents."""
        filters = []
        if pkids is not None:
            filters.append({"terms": {self.pkid_field: pkids}})
        if percolator_prefix is not None:
            filters.append(
                {
                    "script": {
                        "script": {
                            "source": "doc['_id'].value.startsWith(params.prefix)",
                            "params": {"prefix": percolator_prefix},
                        }
                    }
                }
            )
        else:
            filters.append(
                {
                    "script": {
                        "script": {
                            "source": "!doc['_id'].value.startsWith(params.prefix)",
                            "params": {"prefix": self.percolator_prefix},
                        }
                    }
                }
            )

        return {
            "source": {
                "remote": self._create_remote(),
                "index": source_index,
                "query": {"bool": {"filter": filters}},
            },
            "dest": {"index": dest_index},
        }

    def _reindex_author(self, index_name, index_percolator):
        self._log_info("[Author] start DB-based reindex")
        self._log_info(
            "[Author] db={} host={} port={} table={} json_col={} pkid_field={} date={}".format(
                self.db_name,
                self.db_host,
                self.db_port,
                self.db_table,
                self.db_json_column,
                self.pkid_field,
                self.gte_date if self.gte_date else "ALL",
            )
        )
        total_pkids = 0
        total_created_main = 0
        chunk_no = 0


        for pkid_chunk in self._chunked(self._iter_author_pkids_from_db(), self.terms_chunk_size):
            chunk_no += 1
            total_pkids += len(pkid_chunk)

            body_main = self._build_author_reindex_body(
                source_index=index_name,
                dest_index=index_name,
                pkids=pkid_chunk,
                percolator_prefix=None,
            )

            result_main = self._run_os_reindex_or_raise(body_main).json()

            created_main = result_main.get("created", 0)
            total_created_main += created_main

            self._log_info(
                "[Author] chunk={} pkids={} created_main={}".format(
                    chunk_no,
                    len(pkid_chunk),
                    created_main,
                )
            )

        total_created_percolator = 0
        if self.gte_date:
            self._log_info("[Author] percolator reindex with oaiset-* IDs (full scan)")
            body_percolator = self._build_author_reindex_body(
                source_index=index_name,
                dest_index=index_percolator,
                pkids=None,
                percolator_prefix=self.percolator_prefix,
            )
            result_percolator = self._run_os_reindex_or_raise(body_percolator).json()
            total_created_percolator = result_percolator.get("created", 0)
            self._log_info("[Author] percolator created={}".format(total_created_percolator))

        self._log_info(
            "[Author] done chunks={} total_pkids={} total_created_main={} total_created_percolator={}".format(
                chunk_no,
                total_pkids,
                total_created_main,
                total_created_percolator,
            )
        )




        if not self.gte_date:
            self._log_info("{} Reindex next time".format(index_name))
            return

        main_body = {
            "source": {"remote": self._create_remote(), "index": index_name, "query": {"bool": {"must": [], "filter": []}}},
            "dest": {"index": index_name},
        }
        self._log_info("start reindex {} all".format(index_name))
        self._run_os_reindex_or_raise(main_body)

        percolator_body = {
            "source": {"remote": self._create_remote(), "index": index_percolator, "query": {"bool": {"must": [], "filter": []}}},
            "dest": {"index": index_percolator},
        }
        self._run_os_reindex_or_raise(percolator_body)
        self._log_info("end reindex {} all".format(index_name))

    def _run_main_index_reindex(self):
        self._log_step_start("main_index_reindex")
        percolator_body = {"properties": {"query": {"type": "percolator"}}}

        for index_name in self.mappings:
            index_step = "index_reindex:{}".format(index_name)
            self._log_step_start(index_step)
            speed_settings_applied = False
            restore_setting_body = None
            try:
                prepared_index_definition = self._merge_es7_mapping(
                    index_name, copy.deepcopy(self.mappings[index_name])
                )
                prepared_index_definition = self._apply_source_shard_count(prepared_index_definition, index_name)
                default_number_of_replicas = (
                    prepared_index_definition.get("settings", {}).get("index", {}).get("number_of_replicas", 1)
                )
                default_refresh_interval = (
                    prepared_index_definition.get("settings", {}).get("index", {}).get("refresh_interval", "1s")
                )
                performance_setting_body = {"index": {"number_of_replicas": 0, "refresh_interval": "-1"}}
                restore_setting_body = {
                    "index": {
                        "number_of_replicas": default_number_of_replicas,
                        "refresh_interval": default_refresh_interval,
                    }
                }

                index_percolator = self._ensure_target_indexes(
                    index_name, prepared_index_definition, percolator_body
                )

                self._log_step_start("speed_settings:{}".format(index_name))
                self._request_os_or_raise(
                    "PUT", self.os_url + index_name + "/_settings?pretty", json=performance_setting_body
                )
                speed_settings_applied = True
                self._log_step_end("speed_settings:{}".format(index_name))
                self._log_step_start("wait_shard_ready(source):{}".format(index_name))
                self._wait_for_shards_started(self.es7_url, index_name, self.req_args)
                self._log_step_end("wait_shard_ready(source):{}".format(index_name))
                self._log_step_start("wait_shard_ready(dest):{}".format(index_name))
                self._wait_for_shards_started(self.os_url, index_name, self.os_req_args)
                self._log_step_end("wait_shard_ready(dest):{}".format(index_name))

                if "author" not in index_name:
                    self._log_step_start("reindex_non_author:{}".format(index_name))
                    self._reindex_non_author(index_name, index_percolator)
                    self._log_step_end("reindex_non_author:{}".format(index_name))
                else:
                    self._log_step_start("reindex_author:{}".format(index_name))
                    self._reindex_author(index_name, index_percolator)
                    self._log_step_end("reindex_author:{}".format(index_name))

                self._log_step_end(index_step)
            except Exception:
                self.had_errors = True
                self._log_error("raise error: {}".format(index_name))
                self._log_error(traceback.format_exc())
                self._log_step_end(index_step, "ERROR")
            finally:
                if speed_settings_applied and restore_setting_body is not None:
                    try:
                        self._log_step_start("restore_settings:{}".format(index_name))
                        self._request_os_or_raise(
                            "PUT",
                            self.os_url + index_name + "/_settings?pretty",
                            json=restore_setting_body,
                        )
                        self._log_info("reset speed-up setting")
                        self._log_step_end("restore_settings:{}".format(index_name))
                    except Exception:
                        self.had_errors = True
                        self._log_error("restore settings failed: {}".format(index_name))
                        self._log_error(traceback.format_exc())
                        self._log_step_end("restore_settings:{}".format(index_name), "ERROR")

        self._log_step_end("main_index_reindex")

    def _build_stats_alias_actions(self, index_name, alias_name, source_indexes):
        alias_actions = []
        for source_index_name in source_indexes:
            alias_actions.append(
                {
                    "add": {
                        "index": source_index_name,
                        "alias": alias_name,
                        "is_write_index": self.indexes_alias.get(source_index_name, {})
                        .get(alias_name, {})
                        .get("is_write_index", False),
                    }
                }
            )
        return alias_actions

    def _reindex_stats_index(self, index_name):
        self._log_step_start("stats_index:{}".format(index_name))
        filename_without_ext = self.template_files[index_name].split("/")[-1].replace(".json", "")
        template_name = "{}-{}".format(self.prefix, filename_without_ext)
        template_url = self.template_url.format(template_name)

        self._log_info("put template: {}".format(template_name))
        self._request_os_or_raise("PUT", template_url, json=self.templates[index_name])

        index_pattern = "{}-{}-*".format(self.prefix, index_name)
        indexes = self._request_es7_or_raise("GET", self.es7_url + index_pattern).json()
        alias_name = "{}-{}".format(self.prefix, index_name)

        for source_index_name in indexes:
            self._log_info("stats source index: {}".format(source_index_name))

            response = requests.get(self.os_url + source_index_name, **self.os_req_args)
            if response.status_code == 200:
                self._log_info("Index {} already exists, skipping creation.".format(source_index_name))
            else:
                self._log_info("create index: {}".format(source_index_name))
                source_size_in_bytes = self._get_source_index_size(source_index_name)
                create_body = self._apply_source_shard_count({}, source_index_name)
                self._request_os_or_raise("PUT", self.os_url + source_index_name + "?pretty", json=create_body)
                alias_actions = self._build_stats_alias_actions(index_name, alias_name, [source_index_name])
                self._request_os_or_raise("POST", self.os_url + "_aliases", json={"actions": alias_actions})

            source_index = {"remote": self._create_remote(), "index": source_index_name}
            if self.gte_date:
                source_index["query"] = {"range": {"timestamp": {"gte": self.gte_date}}}

            body = {"source": source_index, "dest": {"index": source_index_name}}
            self._log_info("start reindex {}".format(source_index_name))
            self._run_os_reindex_or_raise(body)
            self._log_info("end reindex {}".format(source_index_name))

        self._log_step_end("stats_index:{}".format(index_name))

    def _run_stats_reindex(self):
        self._log_step_start("stats_reindex")
        for stats_index_name in self.stats_index_names:
            try:
                self._reindex_stats_index(stats_index_name)
            except Exception:
                self.had_errors = True
                self._log_error("raise error: {}".format(stats_index_name))
                self._log_error(traceback.format_exc())
        self._log_step_end("stats_reindex")

    def run(self):
        self._log_step_start("run")
        self._log_info("run_id={}".format(self.run_id))
        self._log_info("prefix={}".format(self.prefix))
        self._log_info(
            "safe mode: split by {} day window(s) for large indexes only (threshold={} MB)".format(
                self.date_window_days,
                self.auto_split_threshold_mb,
            )
        )
        self._load_indexes_and_aliases()
        self._load_mapping_files()
        self._load_template_files()
        self._run_main_index_reindex()
        self._run_stats_reindex()

        if self.had_errors:
            self._log_step_end("run", "ERROR")
            self._log_error("Completed with errors")
            sys.exit(1)

        self._log_step_end("run")


def main():
    args = parse_arguments()
    runner = ReindexEs7ToOsRunner(args)
    runner.run()


if __name__ == "__main__":
    main()
