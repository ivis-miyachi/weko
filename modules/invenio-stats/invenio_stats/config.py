# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
# Copyright (C)      2022 TU Wien.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Proxy to the current stats module."""

import os

from flask_babel import get_timezone
from kombu.entity import Exchange

from .utils import default_permission_factory, weko_permission_factory
from invenio_stats.queries import ESDateHistogramQuery, ESTermsQuery, \
    ESWekoFileStatsQuery, ESWekoTermsQuery, ESWekoRankingQuery, ESWekoFileRankingQuery
from invenio_stats.processors import EventsIndexer, anonymize_user, \
    flag_restricted, flag_robots
from invenio_stats.contrib.event_builders import build_celery_task_unique_id, \
    build_file_unique_id, build_item_create_unique_id, \
    build_record_unique_id, build_search_detail_condition, \
    build_search_unique_id, build_top_unique_id, copy_record_index_list, \
    copy_search_keyword, copy_search_type, copy_user_group_list
from invenio_stats.aggregations import StatAggregator, filter_restricted
from invenio_search import current_search_client
from weko_schema_ui.models import PublishStatus

STATS_REGISTER_RECEIVERS = True
"""Enable the registration of signal receivers.

Default is ``True``.
The signal receivers are functions which will listen to the signals listed in
by the ``STATS_EVENTS`` config variable. An event will be generated for each
signal sent.
"""

PROVIDE_PERIOD_YEAR = 5

REPORTS_PER_PAGE = 10

SEARCH_INDEX_PREFIX = os.environ.get('SEARCH_INDEX_PREFIX', '')
"""Search index prefix which is set in weko config."""


STATS_EVENTS = {
    'celery-task': {
        "templates": 'invenio_stats.contrib.celery_task',
        'signal': ['invenio_oaiharvester.signals.oaiharvest_finished',
                   'weko_sitemap.signals.sitemap_finished'],
        'event_builders': [
            'invenio_stats.contrib.event_builders.celery_task_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_celery_task_unique_id
                ],
                suffix="%Y",
            ),
    },
    'file-download': {
        "templates":'invenio_stats.contrib.file_download',
        'signal': 'invenio_files_rest.signals.file_downloaded',
        'event_builders': [
            'invenio_stats.contrib.event_builders.file_download_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_file_unique_id
                ],
                double_click_window=30,
                suffix="%Y",
            ),
    },
    'file-preview': {
        'templates':'invenio_stats.contrib.file_preview',
        'signal': 'invenio_files_rest.signals.file_previewed',
        'event_builders': [
            'invenio_stats.contrib.event_builders.file_preview_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_file_unique_id
                ],
                double_click_window=30,
                suffix="%Y",
            )
    },
    'item-create': {
        'templates':'invenio_stats.contrib.item_create',
        'signal': 'weko_deposit.signals.item_created',
        'event_builders': [
            'invenio_stats.contrib.event_builders.item_create_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_item_create_unique_id
                ],
                suffix="%Y",
            )
    },
    'record-view': {
        'templates':'invenio_stats.contrib.record_view',
        'signal': 'invenio_records_ui.signals.record_viewed',
        'event_builders': [
            'invenio_stats.contrib.event_builders.record_view_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_record_unique_id
                ],
                double_click_window=30,
                suffix="%Y",
            )
    },
    'top-view': {
        'templates':'invenio_stats.contrib.top_view',
        'signal': 'weko_theme.views.top_viewed',
        'event_builders': [
            'invenio_stats.contrib.event_builders.top_view_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_top_unique_id
                ],
                double_click_window=30,
                suffix="%Y",
            )
    },
    'search': {
        'templates':'invenio_stats.contrib.search',
        'signal': 'weko_search_ui.views.searched',
        'event_builders': [
            'invenio_stats.contrib.event_builders.search_event_builder'
        ],
        'cls':EventsIndexer,
        'params':dict(
                preprocessors=[
                    flag_restricted,
                    flag_robots,
                    anonymize_user,
                    build_search_detail_condition,
                    build_search_unique_id
                ],
                double_click_window=30,
                suffix="%Y",
            )
    }
}
"""Enabled Events.

Each key is the name of an event. A queue will be created for each event.

If the dict of an event contains the ``signal`` key, and the config variable
``STATS_REGISTER_RECEIVERS`` is ``True``, a signal receiver will be registered.
Receiver function which will be connected on a signal and emit events. The key
is the name of the emitted event.

``signal``: Signal to which the receiver will be connected to.

``event_builders``: list of functions which will create and enhance the event.
    Each function will receive the event created by the previous function and
    can update it. Keep in mind that these functions will run synchronously
    during the creation of the event, meaning that if the signal is sent during
    a request they will increase the response time.

You can find a sampe of STATS_EVENT configuration in the `registrations.py`
"""

STATS_EXCLUDED_ADDRS = []
"""Fill IP Addresses which will be excluded from stats in `[]`"""
STATS_AGGREGATIONS = {
    'celery-task-agg': {
        "templates": 'invenio_stats.contrib.aggregations.aggr_celery_task',
        "cls":StatAggregator,
        "params":dict(
            client=current_search_client,
            event='celery-task',
            aggregation_field='unique_id',
            aggregation_interval='day',
            index_interval='year',
            query_modifiers=[filter_restricted],
            copy_fields=dict(
                task_id='task_id',
                task_name='task_name',
                task_state='task_state',
                start_time='start_time',
                end_time='end_time',
                total_records='total_records',
                repository_name='repository_name',
                execution_time='execution_time',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )
    },
    'search-agg': {
        "templates": 'invenio_stats.contrib.aggregations.aggr_search',
        "cls":StatAggregator,
        "params":dict(
                client=current_search_client,
                event='search',
                aggregation_field='unique_id',
                aggregation_interval='day',
                index_interval='year',
                query_modifiers=[filter_restricted],
                copy_fields=dict(
                    country='country',
                    referrer='referrer',
                    search_key=copy_search_keyword,
                    search_type=copy_search_type,
                    site_license_name='site_license_name',
                    site_license_flag='site_license_flag'
                    # count='count',
                ),
                metric_aggregation_fields={
                    'unique_count': ('cardinality', 'unique_session_id',
                                     {'precision_threshold': 1000}),
                },
            )
    },
    'file-download-agg': {
        "templates":'invenio_stats.contrib.aggregations.aggr_file_download',
        "cls":StatAggregator,
        "params":dict(
            client=current_search_client,
            event='file-download',
            aggregation_field='unique_id',
            aggregation_interval='day',
            index_interval='year',
            query_modifiers=[filter_restricted],
            copy_fields=dict(
                country='country',
                item_id='item_id',
                item_title='item_title',
                file_key='file_key',
                bucket_id='bucket_id',
                file_id='file_id',
                root_file_id='root_file_id',
                accessrole='accessrole',
                userrole='userrole',
                index_list='index_list',
                is_billing_item='is_billing_item',
                billing_file_price='billing_file_price',
                user_group_names=copy_user_group_list,
                site_license_name='site_license_name',
                site_license_flag='site_license_flag',
                cur_user_id='cur_user_id',
                hostname='hostname',
                remote_addr='remote_addr',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )
    },
    'file-preview-agg': {
        "templates": 'invenio_stats.contrib.aggregations.aggr_file_preview',
        "cls":StatAggregator,
        "params":dict(
            client=current_search_client,
            event='file-preview',
            aggregation_field='unique_id',
            query_modifiers=[filter_restricted],
            aggregation_interval='day',
            index_interval='year',
            copy_fields=dict(
                country='country',
                item_id='item_id',
                item_title='item_title',
                file_key='file_key',
                bucket_id='bucket_id',
                file_id='file_id',
                root_file_id='root_file_id',
                accessrole='accessrole',
                userrole='userrole',
                index_list='index_list',
                is_billing_item='is_billing_item',
                billing_file_price='billing_file_price',
                user_group_names=copy_user_group_list,
                site_license_name='site_license_name',
                site_license_flag='site_license_flag',
                cur_user_id='cur_user_id',
                hostname='hostname',
                remote_addr='remote_addr',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )
    },
    'item-create-agg': {
        "templates": 'invenio_stats.contrib.aggregations.aggr_item_create',
        "cls":StatAggregator,
        "params":dict(
            client=current_search_client,
            event='item-create',
            aggregation_field='unique_id',
            aggregation_interval='day',
            index_interval='year',
            query_modifiers=[filter_restricted],
            copy_fields=dict(
                country='country',
                hostname='hostname',
                cur_user_id='cur_user_id',
                remote_addr='remote_addr',
                pid_type='pid_type',
                pid_value='pid_value',
                record_name='record_name',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
            },
        )
    },
    "record-view-agg": {
        "templates": 'invenio_stats.contrib.aggregations.aggr_record_view',
        "cls":StatAggregator,
        "params": dict(
            client=current_search_client,
            event='record-view',
            aggregation_field='unique_id',
            aggregation_interval='day',
            index_interval='year',
            query_modifiers=[filter_restricted],
            copy_fields=dict(
                country='country',
                hostname='hostname',
                remote_addr='remote_addr',
                record_id='record_id',
                record_name='record_name',
                record_index_names=copy_record_index_list,
                pid_type='pid_type',
                pid_value='pid_value',
                cur_user_id='cur_user_id',
                site_license_name='site_license_name',
                site_license_flag='site_license_flag'
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
            },
        )
    },
    "top-view-agg": {
        "templates": 'invenio_stats.contrib.aggregations.aggr_top_view',
        "cls":StatAggregator,
        "params": dict(
            client=current_search_client,
            event='top-view',
            aggregation_field='unique_id',
            aggregation_interval='day',
            index_interval='year',
            query_modifiers=[filter_restricted],
            copy_fields=dict(
                country='country',
                hostname='hostname',
                remote_addr='remote_addr',
                site_license_name='site_license_name',
                site_license_flag='site_license_flag'
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
            },
        )
    }
}

search_index_prefix = SEARCH_INDEX_PREFIX.strip('-')
STATS_QUERIES = {
    'get-celery-task-report': {
        "cls": ESTermsQuery,
        "params": dict(
                index='{}-stats-celery-task'.format(search_index_prefix),
                aggregated_fields=['task_id', 'task_name', 'start_time',
                                   'end_time', 'total_records', 'task_state'],
                required_filters=dict(
                    task_name='task_name',
                ),
            )
    },
    'get-search-report': {
        "cls": ESWekoTermsQuery,
        "params": dict(
            index='{}-stats-search'.format(search_index_prefix),
            group_fields=['search_key', 'count'],
        )
    },

    'get-file-download-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            group_fields=['file_key', 'index_list', 'userrole',
                        'site_license_flag', 'count'],
        )
    },

    'get-file-download-open-access-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            group_fields=['file_key', 'index_list', 'userrole',
                        'site_license_flag', 'count'],
            required_filters=dict(
                accessrole='accessrole',
            ),
        )
    },

    'get-file-preview-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            group_fields=['file_key', 'index_list', 'userrole',
                        'site_license_flag', 'count'],
        )
    },

    'get-file-preview-open-access-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            group_fields=['file_key', 'index_list', 'userrole',
                        'site_license_flag', 'count'],
            required_filters=dict(
                accessrole='accessrole',
            ),
        )
    },

    'get-billing-file-download-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            group_fields=['file_key', 'index_list',
                        'userrole', 'site_license_flag',
                        'user_group_names', 'count'],
            required_filters=dict(
                is_billing_item='is_billing_item',
            ),
        )
    },

    'get-billing-file-preview-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            group_fields=['file_key', 'index_list',
                        'userrole', 'site_license_flag',
                        'user_group_names', 'count'],
            required_filters=dict(
                is_billing_item='is_billing_item',
            ),
        )
    },

    # 'bucket-celery-task-histogram': {},
    # 'bucket-celery-task-total': {},

    'bucket-file-download-histogram': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            copy_fields=dict(
                bucket_id='bucket_id',
                file_key='file_key',
            ),
            required_filters=dict(
                bucket_id='bucket_id',
                file_key='file_key',
            ),
        )
    },

    'bucket-file-download-total': {
        "cls": ESWekoFileStatsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            copy_fields=dict(),
            main_fields=['bucket_id', 'file_key', 'root_file_id'],
            main_query={
                "query": {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "bucket_id": {
                                                    "value": "@bucket_id",
                                                    "boost": 1
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "file_key": {
                                                    "value": "@file_key",
                                                    "boost": 1
                                                }
                                            }
                                        },
                                        {
                                            "bool": {
                                                "must_not": {
                                                    "exists": {
                                                        "field": "root_file_id"
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "root_file_id": {
                                                    "value": "@root_file_id",
                                                    "boost": 1
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                        "adjust_pure_negative": True,
                        "boost": 1
                    }
                }
            },
            aggregated_fields=['country'],
        )
    },

    'bucket-file-preview-histogram': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            copy_fields=dict(
                bucket_id='bucket_id',
                file_key='file_key',
            ),
            required_filters=dict(
                bucket_id='bucket_id',
                file_key='file_key',
            ),
        )
    },

    'bucket-file-preview-total': {
        "cls": ESWekoFileStatsQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            copy_fields=dict(),
            main_fields=['bucket_id', 'file_key', 'root_file_id'],
            main_query={
                "query": {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "bucket_id": {
                                                    "value": "@bucket_id",
                                                    "boost": 1
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "file_key": {
                                                    "value": "@file_key",
                                                    "boost": 1
                                                }
                                            }
                                        },
                                        {
                                            "bool": {
                                                "must_not": {
                                                    "exists": {
                                                        "field": "root_file_id"
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "root_file_id": {
                                                    "value": "@root_file_id",
                                                    "boost": 1
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                        "adjust_pure_negative": True,
                        "boost": 1
                    }
                }
            },
            aggregated_fields=['country'],
        )
    },

    'get-file-download-per-user-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            group_fields=['cur_user_id', 'count'],
        )
    },

    'get-file-preview-per-user-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            group_fields=['cur_user_id', 'count'],
        )
    },

    'get-record-view-report': {
        "cls": ESWekoTermsQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            group_fields=['record_id', 'record_index_names',
                        'cur_user_id', 'pid_value', 'record_name', 'count'],
        )
    },

    'bucket-record-view-histogram': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            copy_fields=dict(
                record_id='record_id',
            ),
            required_filters=dict(
                record_id='record_id',
            ),
        )
    },

    'bucket-record-view-total': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            copy_fields=dict(
                record_id='record_id',
            ),
            required_filters=dict(
                record_id='record_id',
            ),
            metric_fields=dict(
                count=('sum', 'count', {}),
                unique_count=('sum', 'unique_count', {}),
            ),
            aggregated_fields=['country'],
        )
    },

    'item-create-total': {
        "cls": ESWekoTermsQuery,
        "params": dict(
            index='{}-stats-item-create'.format(search_index_prefix),
            metric_fields=dict(
                count=('sum', 'count', {}),
            ),
            aggregated_fields=['remote_addr', 'hostname', 'cur_user_id'],
        )
    },

    'item-create-per-date': {
        "cls": ESWekoTermsQuery,
        "params": dict(
            index='{}-stats-item-create'.format(search_index_prefix),
            metric_fields=dict(
                count=('sum', 'count', {}),
            ),
            aggregated_fields=['timestamp', 'pid_value', 'record_name'],
        )
    },

    'item-create-histogram': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-item-create'.format(search_index_prefix),
            aggregated_fields=['timestamp'],
        )
    },

    'item-detail-total': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            metric_fields=dict(
                count=('sum', 'count', {}),
            ),
            aggregated_fields=['remote_addr', 'hostname'],
        )
    },

    'item-detail-item-total': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            metric_fields=dict(
                count=('sum', 'count', {}),
            ),
            aggregated_fields=['pid_value', 'record_name'],
        )
    },

    'bucket-item-detail-view-histogram': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            aggregated_fields=['timestamp'],
        )
    },

    'get-file-download-per-host-report': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            metric_fields=dict(
                count=('sum', 'count', {}),
            ),
            aggregated_fields=['remote_addr', 'hostname'],
        )
    },

    'get-file-download-per-item-report': {
        "cls": ESWekoTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            metric_fields=dict(
                count=('sum', 'count', {}),
            ),
            aggregated_fields=['item_id', 'item_title'],
        )
    },

    'get-file-download-per-time-report': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            aggregated_fields=['timestamp'],
        )
    },

    'top-view-total': {
        "cls": ESDateHistogramQuery,
        "params": dict(
            index='{}-stats-top-view'.format(search_index_prefix),
            aggregated_fields=['remote_addr', 'hostname']
        )
    },

    'top-view-total-per-host': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-top-view'.format(search_index_prefix),
            group_fields=['remote_addr', 'hostname', 'count']
        )
    },

    'get-top-view-per-site-license': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-top-view'.format(search_index_prefix),
            group_fields=['site_license_name', 'count'],
        )
    },

    'get-record-view-per-site-license': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-record-view'.format(search_index_prefix),
            group_fields=['site_license_name', 'count'],
        )
    },

    'get-search-per-site-license': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-search'.format(search_index_prefix),
            group_fields=['site_license_name', 'count'],
        )
    },

    'get-file-download-per-site-license': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-download'.format(search_index_prefix),
            group_fields=['site_license_name', 'count'],
        )
    },

    'get-file-preview-per-site-license': {
        "cls": ESTermsQuery,
        "params": dict(
            index='{}-stats-file-preview'.format(search_index_prefix),
            group_fields=['site_license_name', 'count'],
        )
    },

    'get-ranking-data': {
        "cls": ESWekoRankingQuery,
        "params": dict(
            index='{}-stats-{}',
            main_fields=['start_date', 'end_date', 'group_field', 'agg_size', 'count_field'],
            metric_fields=dict(),
            main_query={
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": "@start_date",
                                        "lte": "@end_date",
                                        "time_zone": "@time_zone"
                                    }
                                }
                            }
                        ],
                        "must_not": "@must_not"
                    }
                },
                "aggs": {
                    "my_buckets": {
                        "terms": {
                            "field": "@group_field",
                            "size": "@agg_size",
                            "order": {
                                "my_sum": "desc" 
                            }
                        },
                        "aggs": {
                            "my_sum": {
                                "sum": {
                                    "field": "@count_field" 
                                }
                            }
                        }
                    }
                }
            }
        )
    },

    'get-new-items-data': {
        "cls": ESWekoRankingQuery,
        "params": dict(
            index='{}-weko',
            main_fields=['start_date', 'end_date', 'agg_size'],
            metric_fields=dict(),
            main_query={
                "size": "@agg_size",
                "sort": [
                    {
                        "publish_date":{
                            "order": "desc"
                        }
                    }
                ],
                "query":{
                    "bool":{
                        "must":[
                            {
                                "range": {
                                    "publish_date": {
                                        "gte": "@start_date",
                                        "lte": "@end_date"
                                    }
                                }
                            },
                            {
                                "term": {
                                    "relation_version_is_last": True
                                }
                            },
                            {
                                "terms": {
                                    "publish_status": [
                                        PublishStatus.PUBLIC.value,
                                        PublishStatus.PRIVATE.value
                                    ]
                                }
                            }
                        ],
                        "must_not": "@must_not"
                    }
                }
            }
        )
    },

    'item-file-download-aggs': {
        "cls": ESWekoFileRankingQuery,
        "params": dict(
            index='{}-events-stats-file-download'.format(search_index_prefix),
            copy_fields=dict(),
            metric_fields=dict(
                download_ranking=('terms', 'file_key', {})
            ),
            main_fields=['item_id'],
            main_query={
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "item_id": {
                                        "value": "@item_id",
                                        "boost": 1
                                    }
                                }
                            },
                            {
                                "terms": {
                                    "root_file_id": "@root_file_id_list"
                                }
                            }
                        ]
                    }
                },
                "size": 0
            },
        )
    },
}


STATS_PERMISSION_FACTORY = weko_permission_factory
"""Permission factory used by the statistics REST API.

This is a function which returns a permission granting or forbidding access
to a request. It is of the form ``permission_factory(query_name, params)``
where ``query_name`` is the name of the statistic requested by the user and
``params`` is a dict of parameters for this statistic. The result of the
function is a Permission.

See Invenio-access and Flask-principal for a better understanding of the
access control mechanisms.
"""


STATS_MQ_EXCHANGE = Exchange(
    "events",
    type="direct",
    delivery_mode="transient",  # in-memory queue
)
"""Default exchange used for the message queues."""

TARGET_REPORTS = {
    'Item Registration': '1',
    'Item Detail': '2',
    'Contents Download': '3',
}

STATS_ES_INTEGER_MAX_VALUE = 6000
"""Since ES2 using size=0 has been prohibited, so in order to accomplish
the same thing, Integer.MAX_VALUE is used to retrieve agg buckets.
In ES2, size=0 was internally replaced by this value, so we have effectively
mimicked the same functonality.

Changed from 2147483647 to 6000. (refs. weko#23741)
"""


WEKO_STATS_UNKNOWN_LABEL = 'UNKNOWN'
"""Label using for missing of view or file-download stats."""

STATS_EVENT_STRING = 'events'
"""Stats event string."""

STATS_AGGREGATION_INDEXES = []
"""Stats aggregation indexes."""


STATS_WEKO_DEFAULT_TIMEZONE = get_timezone
"""Bucketing should use a different time zone."""

STATS_WEKO_DB_BACKUP_EVENTS = True
"""Enable DB backup of events."""

STATS_WEKO_DB_BACKUP_AGGREGATION = False
"""Enable DB backup of aggregation."""

STATS_WEKO_DB_BACKUP_BOOKMARK = False
"""Enable DB backup of bookmark."""

