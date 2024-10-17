
# ?-> id
# _id-> source_id
# _index -> index
# _type-> type
# _source-> source
# _source.timestamp or _source.date or date-> date
import os
import json
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from elasticsearch_dsl import Index, Search
from elasticsearch import Elasticsearch

from uuid import uuid4
import hashlib
def _generate_id():
    """Generate identifier.

    :return:
    """
    current_time = datetime.utcnow().isoformat()
    return str(str(uuid4()) + \
        hashlib.sha1(current_time.encode("utf-8")).hexdigest())

ES_HOST = os.environ.get('INVENIO_ELASTICSEARCH_HOST','localhost')
ES_USER = "admin"
ES_PASSWORD = "admin"
ES_PORT = 9200

client = Elasticsearch(
    host=ES_HOST,
    # http_auth=(ES_USER, ES_PASSWORD),
    # use_ssl=True,
    verify_certs=False
)

stats_bookmark_index = os.environ.get('SEARCH_INDEX_PREFIX')+"-stats-bookmarks"
result = client.search(
    index=stats_bookmark_index,
    body={"query": {"match_all": {}}}
)

stats_bookmark_documents = result["hits"]["hits"]
# stats_bookmark_documents = Search(
#     using=current_search_client, 
#     index=stats_bookmark_index
# ).query("match_all").scan()


import psycopg2
import psycopg2.extras
DB_HOST = os.environ['INVENIO_POSTGRESQL_HOST']
DB_PORT = 5432
DB_DATABASE = os.environ['INVENIO_POSTGRESQL_DBNAME']
DB_USER = os.environ['INVENIO_POSTGRESQL_DBUSER']
DB_PASSWORD = os.environ['INVENIO_POSTGRESQL_DBPASS']
current_time = datetime.utcnow().isoformat()
with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_DATABASE, user=DB_USER, password=DB_PASSWORD) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            current_time = datetime.utcnow().isoformat()
            for doc in stats_bookmark_documents:
                # source_id = doc.meta.id
                # index = doc.meta.index
                # source = doc.to_dict()
                source_id =doc["_id"]
                index = doc["_index"]
                type = doc["_type"]
                source = doc["_source"]
                if "timestamp" in source:
                    date = source["timestamp"]
                elif "date" in source:
                    date = source["date"]

                stats_data = {
                    "id": _generate_id(),
                    "source_id": source_id,
                    "index": index,
                    "source": json.dumps(source),
                }
                query = u"INSERT INTO stats_bookmark (id, source_id, index, type, source, date, created, updated) "\
                    "VALUES ('{id}', '{source_id}', '{index}', '{type}', '{source}', '{date}', '{created}', '{updated}') "\
                    "ON CONFLICT ON CONSTRAINT uq_stats_key_stats_bookmark "\
                    "DO UPDATE SET"\
                    " source = EXCLUDED.source;".format(
                        id=_generate_id(),
                        source_id=source_id,
                        index=index,
                        type=type,
                        source=json.dumps(source),
                        date=date,
                        created=date,
                        updated=date
                    )
                print(query)
                cur.execute(
                    query
                )
            conn.commit()
        except Exception as e:
            print("raise error")
            import traceback
            print(traceback.format_exc())
            conn.rollback()
# try:
#     current_time = datetime.utcnow().isoformat()
#     for doc in stats_bookmark_documents:
#         source_id = doc.meta.id
#         index = doc.meta.index
#         source = doc.to_dict()
#         if "timestamp" in source:
#             date = source["timestamp"]
#         elif "date" in source:
#             date = source["date"]

#         stats_data = {
#             "id":_generate_id(),
#             "source_id": source_id,
#             "index": index,
#             "source": json.dumps(source),
#         }

#         up_stats_key = StatsBookmark.get_up_key()
#         stmt = insert(StatsBookmark)
#         db.session.execute(
#             stmt.on_conflict_do_update(
#                 set_={"source":stmt.excluded.source},
#                 constraint=up_stats_key),
#             params=stats_data
#         )
# except Exception as e:
#     print(e)
#     db.session.rollback()
