#!/usr/bin/env bash
${INVENIO_WEB_INSTANCE} index destroy --yes-i-know
${INVENIO_WEB_INSTANCE} index init
sleep 20
${INVENIO_WEB_INSTANCE} index queue init
# sphinxdoc-index-initialisation-end

# elasticsearch-ilm-setting-begin
curl -XPUT 'http://'${INVENIO_ELASTICSEARCH_HOST}':9200/_ilm/policy/weko_stats_policy' -H 'Content-Type: application/json' -d '
{
  "policy":{
    "phases":{
      "hot":{
        "actions":{
          "rollover":{
            "max_size":"50gb"
          }
        }
      }
    }
  }
}'
event_list=('celery-task' 'item-create' 'top-view' 'record-view' 'file-download' 'file-preview' 'search')
for event_name in ${event_list[@]}
do
  curl -XPUT 'http://'${INVENIO_ELASTICSEARCH_HOST}':9200/'${SEARCH_INDEX_PREFIX}'-events-stats-'${event_name}'-000001' -H 'Content-Type: application/json' -d '
  {
    "aliases": {
      "'${SEARCH_INDEX_PREFIX}'-events-stats-'${event_name}'": {
        "is_write_index": true
      }
    }
  }'
  curl -XPUT 'http://'${INVENIO_ELASTICSEARCH_HOST}':9200/'${SEARCH_INDEX_PREFIX}'-stats-'${event_name}'-000001' -H 'Content-Type: application/json' -d '
  {
    "aliases": {
      "'${SEARCH_INDEX_PREFIX}'-stats-'${event_name}'": {
        "is_write_index": true
      }
    }
  }'
done