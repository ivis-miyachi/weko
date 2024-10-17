
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

from invenio_db import db
from invenio_search import current_search_client
from invenio_stats.models import StatsBookmark, _generate_id

stats_bookmark_index = os.environ.get('SEARCH_INDEX_PREFIX')+"-stats-bookmarks"

stats_bookmark_documents = Search(
    using=current_search_client, 
    index=stats_bookmark_index
).query("match_all").scan()

try:
    current_time = datetime.utcnow().isoformat()
    for doc in stats_bookmark_documents:
        source_id = doc.meta.id
        index = doc.meta.index
        source = doc.to_dict()
        if "timestamp" in source:
            date = source["timestamp"]
        elif "date" in source:
            date = source["date"]

        stats_data = {
            "id":_generate_id(),
            "source_id": source_id,
            "index": index,
            "source": json.dumps(source),
        }

        up_stats_key = StatsBookmark.get_up_key()
        stmt = insert(StatsBookmark)
        db.session.execute(
            stmt.on_conflict_do_update(
                set_={"source":stmt.excluded.source},
                constraint=up_stats_key),
            params=stats_data
        )
except Exception as e:
    print(e)
    db.session.rollback()
