import tempfile
from pathlib import Path
import pytest

from datetime import datetime, date

from pyspark.sql import SparkSession
from traffic_counter.traffic_counter import get_top_half_hours, load_data, total_number_of_cars, \
                                get_cars_per_day, get_period_with_least_cars, get_schema

from traffic_counter.utils import get_spark_session

@pytest.fixture(autouse=True)
def spark_session()->SparkSession:
    return get_spark_session("test" + __name__)

@pytest.fixture(autouse=True)
def sample_data_1(spark_session):
    return spark_session.createDataFrame(
        [
            [datetime.fromisoformat("2022-01-01 12:00:00"), 10],
            [datetime.fromisoformat("2022-01-01 12:30:00"), 20],
            [datetime.fromisoformat("2022-01-01 13:00:00"), 30],
            [datetime.fromisoformat("2022-01-01 13:30:00"), 40]
        ],
        schema=get_schema()
    )

@pytest.fixture(autouse=True)
def sample_data_2(spark_session):
    return spark_session.createDataFrame(
        [
            [datetime.fromisoformat("2022-01-01 12:00:00"), 10],
            [datetime.fromisoformat("2022-01-01 12:30:00"), 20],
            [datetime.fromisoformat("2022-01-01 13:00:00"), 30],
            [datetime.fromisoformat("2022-01-01 13:30:00"), 11],
            [datetime.fromisoformat("2022-01-01 14:00:00"), 21],
            [datetime.fromisoformat("2022-01-01 14:30:00"), 31],
            [datetime.fromisoformat("2022-01-01 15:00:00"), 12],
            [datetime.fromisoformat("2022-01-01 15:30:00"), 22],
            [datetime.fromisoformat("2022-01-01 16:00:00"), 32],
            [datetime.fromisoformat("2022-01-01 16:30:00"), 13]
        ],
        schema=get_schema()
    )

def test_load_data(spark_session):
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(tmpdir + "/data.txt", "w") as _f:
            _f.write("2021-12-01T05:00:00 5\n")
            _f.write("2021-12-01T05:00:00 5")
        df = load_data(spark_session,Path(tmpdir))
        assert df.collect()[0][1] == 5

def test_number_of_cars(sample_data_1):
    total_number_of_cars(sample_data_1) == 100

def test_get_cars_per_day(sample_data_1):
    row = get_cars_per_day(sample_data_1).collect()[0]
    assert date(2022,1,1) == row[0]
    assert 100 == row[1]

def test_get_top_half_hours(sample_data_2):
    rows = get_top_half_hours(sample_data_2, 3).collect()
    assert len(rows) == 3
    assert rows[0][1] == 32
    assert rows[2][1] == 30

def test_get_period_with_least_cars(sample_data_2):
    rows = get_period_with_least_cars(sample_data_2).collect()
    assert len(rows) == 3
    assert rows[0][1] == 10
    assert rows[1][1] == 20
    assert rows[2][1] == 30