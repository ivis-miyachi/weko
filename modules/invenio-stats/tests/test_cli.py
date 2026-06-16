# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CLI tests."""

import datetime

import pytest
from unittest.mock import patch
from click.testing import CliRunner
from tests.conftest import _create_file_download_event, _create_record_view_event

from .helpers import mock_date
from invenio_search import current_search
from invenio_search.engine import dsl

from invenio_stats import current_stats
from invenio_stats.cli import stats

# def _events_process(event_types=None, eager=False):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_events_process -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp


def test_events_process(app, es, script_info, event_queues):
    """Test "events process" CLI command."""
    search_obj = dsl.Search(using=es)
    runner = CliRunner()

    # Invalid argument
    result = runner.invoke(
        stats, ["events", "process", "invalid-event-type", "--eager"], obj=script_info
    )
    assert result.exit_code == 2
    assert "Invalid event type(s):" in result.output

    current_stats.publish(
        "file-download",
        [
            _create_file_download_event(date)
            for date in [(2018, 1, 1, 10), (2018, 1, 1, 12), (2018, 1, 1, 14)]
        ],
    )
    current_stats.publish(
        "record-view",
        [
            _create_record_view_event(date)
            for date in [(2018, 1, 1, 10), (2018, 1, 1, 12), (2018, 1, 1, 14)]
        ],
    )

    result = runner.invoke(
        stats, ["events", "process", "file-download", "--eager"], obj=script_info
    )
    assert result.exit_code == 0

    current_search.flush_and_refresh(index="test-*")

    assert not es.indices.exists("events-stats-record-view-2018-01")
    assert not es.indices.exists_alias(name="events-stats-record-view")

    result = runner.invoke(
        stats, ["events", "process", "record-view", "--eager"], obj=script_info
    )
    assert result.exit_code == 0

    current_search.flush_and_refresh(index="test-*")

    # Create some more events
    current_stats.publish(
        "file-download", [_create_file_download_event((2018, 2, 1, 12))]
    )
    current_stats.publish(
        "record-view", [_create_record_view_event((2018, 2, 1, 10))])

    current_search.flush_and_refresh(index="test-*")
    # Process all event types via a celery task
    result = runner.invoke(stats, ["events", "process"], obj=script_info)
    assert result.exit_code == 0

    current_search.flush_and_refresh(index="test-*")

# def _events_delete(event_types, start_date, end_date, force, verbose):
# def _events_restore(event_types, start_date, end_date, force, verbose):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_events_delete_restore -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp


def test_events_delete_restore(app, script_info, es, event_queues):
    search = dsl.Search(using=es)
    runner = CliRunner()

    current_stats.publish(
        "file-download",
        [_create_file_download_event(date) for date in
         [(2018, 1, 1, 10), (2018, 1, 2, 12), (2018, 1, 3, 14)]])
    current_stats.publish(
        "record-view",
        [_create_record_view_event(date) for date in
         [(2018, 1, 1, 10), (2018, 1, 2, 12), (2018, 1, 3, 14)]])
    result = runner.invoke(
        stats, ["events", "delete", "file-download", "--start-date=2018-01-01",
                "--end-date=2018-01-02", "--force", "--verbose", "--yes-i-know"],
        obj=script_info)
    assert result
    result = runner.invoke(
        stats, ["events", "restore", "file-download", "--start-date=2018-01-01",
                "--end-date=2018-01-02", "--force", "--verbose"],
        obj=script_info)
    assert result

# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_events_restore -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_events_restore(app, script_info, es, db, event_queues):
    from invenio_stats.models import StatsEvents
    from datetime import datetime
    import json
    import time

    with app.app_context():
        runner = CliRunner()
        
        # 以下のSQLを先に実行する必要がある
        # CREATE TABLE stats_events_2024_12_31 PARTITION OF stats_events
        # FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2024-12-31 00:00:00');

        sql = """
            CREATE TABLE stats_events_2024_12_31 PARTITION OF stats_events
            FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2024-12-31 00:00:00');
        """
        with db.engine.connect() as connection:
            connection.execute(str(sql))
            print("Partation table created successfully.")

        new_version_data = [
            {
                "created": "2024-11-28 00:33:09.480608",
                "updated": "2024-11-28 00:33:09.480612",
                "_id": "2024-11-28T00:32:30-1277b9fdc34f218e8e1802f30523832c6d0ec86f",
                "_index": "test-events-stats-index",
                "_source":{"event_type":"top-view","timestamp":"2024-11-28T00:32:41","unique_id":"d24421df-bc17-3b86-a180-897a9882e667","remote_addr":"192.168.56.1","visitor_id":"48dab7ebc3f8363a70e420ceaec7bc1a6c56c5d5a2fe9c9c1f55da39"},
                "date": datetime.strptime("2024-11-28 00:32:41", "%Y-%m-%d %H:%M:%S")
            },
            {
                "created": "2024-11-28 00:35:09.389215",
                "updated": "2024-11-28 00:35:09.389219",
                "_id": "2024-11-28T00:34:00-898cf618a0bcadc7eab07ac2a453d705ba92cd78",
                "_index": "test-events-stats-index",
                "_source":{"event_type":"file-download","timestamp":"2024-11-28T00:34:12","unique_id":"f8aaa533-bdbd-360c-aaef-39455dabb40e","remote_addr":"192.168.56.1","visitor_id":"48dab7ebc3f8363a70e420ceaec7bc1a6c56c5d5a2fe9c9c1f55da39","file_id":"3f642f8b-433a-447e-9bdd-cf5d867788c4","item_id":"5","file_key":"Test_video.mp4"},
                "date": datetime.strptime("2024-11-28 00:34:12", "%Y-%m-%d %H:%M:%S")
            }
        ]

        old_version_data = [
            {
                "created": "2024-09-13 09:36:45.524256",
                "updated": "2024-09-13 09:36:45.524261",
                "_id": "2024-09-13T09:36:30-b26d305f1c7d2bc280e50df53fe9bd548b9b4251",
                "_index": "test-events-stats-top-view",
                "type": "stats-top-view",
                "timestamp": "2024-09-13T09:36:31",
                "_source": "{\"timestamp\": \"2024-09-13T09:36:31\", \"unique_id\": \"a3a4c4e0-032e-3d08-beee-b4f79681e21c\", \"remote_addr\": \"192.168.56.1\", \"visitor_id\": \"a2c4a00903fce259a64795b27087a6afa7921e8927facdd3011602d9\"}",
                "date": datetime.strptime("2024-09-13 09:36:31", "%Y-%m-%d %H:%M:%S")
            },
            {
                "created": "2024-11-15 07:01:06.441355",
                "updated": "2024-11-15 07:01:06.441358",
                "_id": "2024-11-15T07:00:00-840dab21ad2035be553bce285cca8108154dfb62",
                "_index": "test-events-stats-file-download",
                "type": "stats-file-download",
                "timestamp": "2024-11-15T07:00:17",
                "_source": "{\"timestamp\": \"2024-11-15T07:00:17\", \"unique_id\": \"0327ab05-39cc-38da-8dcf-ed4e26f6eaef\", \"remote_addr\": \"192.168.56.1\", \"visitor_id\": \"66aad2366fc18433f2c279affa6771e8cee2c63aaa0604e163e51901\", \"file_id\": \"55641917-719f-4068-96e8-3f9178fce963\", \"item_id\": \"2000005\", \"file_key\": \"data.zip\"}",
                "date": datetime.strptime("2024-11-15 07:00:17", "%Y-%m-%d %H:%M:%S")
            }
        ]

        test_data = new_version_data + old_version_data
        for data in test_data:
            if isinstance(data["_source"], str):
                data["_source"]=json.loads(data["_source"])
            StatsEvents.save(data, True)

        time.sleep(3)

        test_cases = [
            # date
            {
                "params": ["--start-date=2024-11-01", "--end-date=2024-11-30"],
                "expected_ids": [
                    "2024-11-28T00:32:30-1277b9fdc34f218e8e1802f30523832c6d0ec86f",
                    "2024-11-28T00:34:00-898cf618a0bcadc7eab07ac2a453d705ba92cd78",
                    "2024-11-15T07:00:00-840dab21ad2035be553bce285cca8108154dfb62-file-download"
                ]
            },
            # single event-type
            {
                "params": ["top-view"],
                "expected_ids": [
                    "2024-11-28T00:32:30-1277b9fdc34f218e8e1802f30523832c6d0ec86f",
                    "2024-09-13T09:36:30-b26d305f1c7d2bc280e50df53fe9bd548b9b4251"
                ]
            },
            # double event-type
            {
                "params": ["top-view", "file-download"],
                "expected_ids": [
                    "2024-11-28T00:32:30-1277b9fdc34f218e8e1802f30523832c6d0ec86f",
                    "2024-11-28T00:34:00-898cf618a0bcadc7eab07ac2a453d705ba92cd78",
                    "2024-11-15T07:00:00-840dab21ad2035be553bce285cca8108154dfb62-file-download",
                    "2024-09-13T09:36:30-b26d305f1c7d2bc280e50df53fe9bd548b9b4251"
                ]
            },
            # date + event-type
            {
                "params": ["file-download", "top-view", "--start-date=2024-11-01", "--end-date=2024-11-30"],
                "expected_ids": [
                    "2024-11-28T00:32:30-1277b9fdc34f218e8e1802f30523832c6d0ec86f",
                    "2024-11-28T00:34:00-898cf618a0bcadc7eab07ac2a453d705ba92cd78",
                    "2024-11-15T07:00:00-840dab21ad2035be553bce285cca8108154dfb62-file-download"
                ]
            }
        ]

        for i, case in enumerate(test_cases):
            cmd = ["events", "restore"] + case["params"] + ["--force", "--verbose"]
            runner.invoke(stats, cmd, obj=script_info)

            time.sleep(15)

            query = dsl.Search(using=es, index="test-events-stats-index")
            res = query.execute()

            restored_ids = [hit.meta.id for hit in res]
            assert set(restored_ids) == set(case["expected_ids"])

            for hit in res:
                assert "event_type" in hit.to_dict()
                if hit.meta.id in [id for id in case['expected_ids'] if id.endswith("-file-download")]:
                    assert hit.to_dict()["unique_id"].endswith("-file-download")

            delete_cmd = ["events", "delete", "--force", "--verbose", "--yes-i-know"]
            runner.invoke(stats, delete_cmd, obj=script_info)
            
            time.sleep(5)
            
            # delete結果検証
            query_1 = dsl.Search(using=es, index="test-events-stats-index")
            res_1 = query_1.execute()
            assert res_1.hits.total.value == 0
            
            print(f"Test progress: {i+1}/4")

# def _aggregations_process(aggregation_types=None, start_date=None, end_date=None, update_bookmark=False, eager=False):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_aggregations_process -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
@pytest.mark.parametrize(
    "indexed_file_download_events",
    [
        {
            "file_number": 1,
            "event_number": 1,
            "robot_event_number": 0,
            "start_date": datetime.date(2018, 1, 1),
            "end_date": datetime.date(2018, 2, 15),
        }
    ],
    indirect=["indexed_file_download_events"]
)
def test_aggregations_process(script_info, event_queues, es, indexed_file_download_events):
    """Test "aggregations process" CLI command."""
    search_obj = dsl.Search(using=es)
    runner = CliRunner()

    # Invalid argument
    result = runner.invoke(
        stats,
        ["aggregations", "process", "invalid-aggr-type", "--eager"],
        obj=script_info,
    )
    assert result.exit_code == 2
    assert "Invalid aggregation type(s):" in result.output

    result = runner.invoke(
        stats,
        [
            "aggregations",
            "process",
            "file-download-agg",
            "--start-date=2018-01-01",
            "--end-date=2018-01-10",
            "--eager",
        ],
        obj=script_info,
    )
    assert result.exit_code == 1

    agg_alias = search_obj.index("stats-file-download")

    current_search.flush_and_refresh(index="test-*")

    # Run again over same period, but update the bookmark
    result = runner.invoke(
        stats,
        [
            "aggregations",
            "process",
            "file-download-agg",
            "--start-date=2018-01-01",
            "--end-date=2018-01-10",
            "--eager",
            "--update-bookmark",
        ],
        obj=script_info,
    )
    assert result.exit_code == 1

    current_search.flush_and_refresh(index="test-*")

    # Run over all the events via celery task
    with patch("invenio_stats.aggregations.datetime", mock_date(2018, 2, 15)):
        result = runner.invoke(
            stats,
            ["aggregations", "process", "file-download-agg", "--update-bookmark"],
            obj=script_info,
        )
        assert result

    current_search.flush_and_refresh(index="test-*")
    assert agg_alias.count() == 46
    assert (
        search_obj.index("stats-bookmarks").count() == 2
    )  # This time there are two, since we had two different dates
    assert search_obj.index("stats-file-download-2018-01").count() == 31
    assert search_obj.index("stats-file-download-2018-02").count() == 15

# def _aggregations_delete(aggregation_types=None, start_date=None, end_date=None):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_aggregations_delete -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp


@pytest.mark.parametrize(
    "aggregated_file_download_events",
    [
        {
            "file_number": 1,
            "event_number": 1,
            "robot_event_number": 0,
            "start_date": datetime.date(2018, 1, 1),
            "end_date": datetime.date(2018, 1, 31),
        }
    ],
    indirect=["aggregated_file_download_events"],
)
def test_aggregations_delete(
    db, script_info, event_queues, es, aggregated_file_download_events
):
    """Test "aggregations process" CLI command."""
    search_obj = dsl.Search(using=es)
    runner = CliRunner()

    current_search.flush_and_refresh(index="test-*")
    agg_alias = search_obj.index("stats-file-download")

    result = runner.invoke(
        stats,
        [
            "aggregations",
            "delete",
            "file-download-agg",
            "--start-date=2018-01-01",
            "--end-date=2018-01-10",
            "--yes",
        ],
        obj=script_info,
    )
    assert result

    current_search.flush_and_refresh(index="test-*")
    agg_alias = search_obj.index("stats-file-download")

    # Delete all aggregations
    result = runner.invoke(
        stats, ["aggregations", "delete", "file-download-agg", "--yes"], obj=script_info
    )
    assert result

    current_search.flush_and_refresh(index="test-*")
    agg_alias = search_obj.index("stats-file-download")

# def _aggregations_list_bookmarks(aggregation_types=None, start_date=None, end_date=None, limit=None):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_aggregations_list_bookmarks -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp


@pytest.mark.parametrize(
    "aggregated_file_download_events",
    [
        {
            "file_number": 1,
            "event_number": 1,
            "robot_event_number": 0,
            "start_date": datetime.date(2018, 1, 1),
            "end_date": datetime.date(2018, 1, 31),
        }
    ],
    indirect=["aggregated_file_download_events"],
)
def test_aggregations_list_bookmarks(
    db, script_info, event_queues, es, aggregated_file_download_events
):
    with app.app_context():
        """Test "aggregations list-bookmarks" CLI command."""
        search_obj = dsl.Search(using=es)
        runner = CliRunner()

        current_search.flush_and_refresh(index="test-*")
        agg_alias = search_obj.index("stats-file-download")

        bookmarks = [b.date for b in search_obj.index(
            "stats-bookmarks").scan()]

        result = runner.invoke(
            stats,
            ["aggregations", "list-bookmarks", "file-download-agg", "--limit", "31"],
            obj=script_info,
        )
        assert result
        assert all(b in result.output for b in bookmarks)

        result = runner.invoke(
            stats, ["aggregations", "list-bookmarks", "file-download-agg"], obj=script_info
        )
        assert result

# def _aggregations_delete_index(aggregation_types=None, bookindexed_eventssmark=False, start_date=None, end_date=None, force=False, verbose=False
# def _aggregations_restore(aggregation_types=None, bookmark=False, start_date=None, end_date=None, force=False, verbose=False):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_aggregations_deleteindex_restore -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_aggregations_deleteindex_restore(app, db, script_info, event_queues, es):
    search = dsl.Search(using=es)
    runner = CliRunner()

    result = runner.invoke(
        stats, ["aggregations", "delete-index", "file-download-agg", "--start-date=2018-01-01",
                "--end-date=2018-01-10", "--force", "--verbose", "--yes-i-know"],
        obj=script_info)
    assert result

    result = runner.invoke(
        stats, ["aggregations", "restore", "file-download-agg",
                "--start-date=2018-01-01", "--end-date=2018-01-10", "--force", "--verbose"],
        obj=script_info)
    assert result

# def _partition_create(year, month):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_cli.py::test_partition_create -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp


def test_partition_create(db, script_info, event_queues, es):
    search = dsl.Search(using=es)
    runner = CliRunner()

    with patch("invenio_stats.cli.get_stats_events_partition_tables", return_value=["stats_events_202201", "stats_events_202202"]):
        with patch("invenio_stats.cli.make_stats_events_partition_table", return_value="stats_events_202201"):
            result = runner.invoke(
                stats, ["partition", "create", "2022", "1"],
                obj=script_info)
            assert result

            result = runner.invoke(
                stats, ["partition", "create", "2022"],
                obj=script_info)
            assert result

            result = runner.invoke(
                stats, ["partition", "create", "20220", "1"],
                obj=script_info)
            assert result

        with patch("invenio_stats.cli.make_stats_events_partition_table", return_value="stats_events_202203"):
            result = runner.invoke(
                stats, ["partition", "create", "2022", "3"],
                obj=script_info)
            assert result
