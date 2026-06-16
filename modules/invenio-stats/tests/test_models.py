import datetime
from mock import patch
from sqlalchemy.exc import SQLAlchemyError

from invenio_stats.models import (
    get_stats_events_partition_tables,
    make_stats_events_partition_table,
    StatsEvents,
    StatsAggregation,
    StatsBookmark
)


# class StatsEvents(db.Model, _StataModelBase):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_models.py::test_StatsEvents -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_StatsEvents(app, db, stats_events_for_db):
    _save_data1 = {
        "_source": {
            "timestamp": "2023-01-01T01:01:00",
            "_id": "2",
            "_index": "test-events-stats-record-view",
            "_type": "record-view"
        }
    }
    _save_data2 = {
        "_source": {
            "date": "2023-01-01T01:01:01",
            "_id": "3",
            "_index": "test-events-stats-record-view",
            "_type": "record-view"
        }
    }
    assert StatsEvents.get_uq_key() == 'uq_stats_key_stats_events'
    assert StatsEvents.delete_by_source_id("1", "test-events-stats-top-view") == True
    with patch('invenio_db.db.session.query', side_effect=SQLAlchemyError("test_sql_error")):
        assert StatsEvents.delete_by_source_id("1", "test-events-stats-top-view") == False
    assert StatsEvents.save({"_source": None}) == False
    with patch('invenio_db.db.session.execute', return_value=True):
        assert StatsEvents.save(_save_data1) == True
        assert StatsEvents.save(_save_data2) == True
    with patch('invenio_db.db.session.execute', side_effect=SQLAlchemyError("test_sql_error")):
        assert StatsEvents.save(_save_data1) == False
      
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_models.py::test_get_by_event_type -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp        
def test_get_by_event_type(app, db):
    from invenio_stats.models import StatsEvents
    from datetime import datetime
    import json
    import time
    from click.testing import CliRunner

    with app.app_context():
        runner = CliRunner()
        
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
            
        # type & date 1
        index = "test-events-stats-index"
        type = "file-download"
        start_date = "2024-11-01"
        end_date = "2024-11-30"
        data = StatsEvents.get_by_event_type(index, type, start_date, end_date)
        assert len(data) == 2
        
        # type & date 2
        index = "test-events-stats-index"
        type = "top-view"
        start_date = "2024-11-01"
        end_date = "2024-11-30"
        data = StatsEvents.get_by_event_type(index, type, start_date, end_date)
        assert len(data) == 1
        
        # type 1
        index = "test-events-stats-index"
        type = "top-view"
        data = StatsEvents.get_by_event_type(index, type)
        assert len(data) == 2
        
        # type 2
        index = "test-events-stats-index"
        type = "file-preview"
        data = StatsEvents.get_by_event_type(index, type)
        assert len(data) == 0
        
# class StatsAggregation(db.Model, _StataModelBase):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_models.py::test_StatsAggregation -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_StatsAggregation(app, db):
    assert StatsAggregation.get_uq_key() == 'uq_stats_key_stats_aggregation'


# class StatsBookmark(db.Model, _StataModelBase):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_models.py::test_StatsBookmark -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_StatsBookmark(app, db):
    assert StatsBookmark.get_uq_key() == 'uq_stats_key_stats_bookmark'


class DbExec():
    def __init__(self, sql) -> None:
        pass

    def fetchall(self):
        return [['stats_events']]

# def get_stats_events_partition_tables():
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_models.py::test_get_stats_events_partition_tables -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_get_stats_events_partition_tables(app, db):
    with patch('invenio_db.db.session.execute', side_effect=DbExec):
        assert get_stats_events_partition_tables() == ['stats_events']


# def make_stats_events_partition_table(year, month):
# .tox/c1/bin/pytest --cov=invenio_stats tests/test_models.py::test_make_stats_events_partition_table -v -s -vv --cov-branch --cov-report=term --cov-config=tox.ini --basetemp=/code/modules/invenio-stats/.tox/c1/tmp
def test_make_stats_events_partition_table(app, db):
    with patch('sqlalchemy.event.listen', return_value=True):
        assert make_stats_events_partition_table(2023, 1) == 'stats_events_202301'
