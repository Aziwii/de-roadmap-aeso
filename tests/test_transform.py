import pytest
from pyspark.sql import SparkSession
import sys
import os
from src.transform import transform_data

# Make src/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

@pytest.fixture(scope="session")
def spark():
    """creates one shared SparkSession for all tests in this file. session setup is slow so this keeps it fast"""
    return SparkSession.builder.appName("test-aeso").master("local[1]").getOrCreate()


def make_price_json(spark, records):
    """Builds a fake raw pool-price DataFrame matching AESO's real envelope shape. 
    Using parallelize then the spark.read.json mimics how the json is read, tests can use explode()"""
    payload = {"return": {"Pool Price Report": records}}
    rdd = spark.sparkContext.parallelize([payload])
    return spark.read.json(rdd)


def make_ail_json(spark, records):
    """Builds a fake raw AIL DataFrame matching AESO's real envelope shape."""
    payload = {"return": {"Actual Forecast Report": records}}
    rdd = spark.sparkContext.parallelize([payload])
    return spark.read.json(rdd)


def test_transform_joins_matching_hours(spark):
    price_records = [
        {"begin_datetime_mpt": "2026-08-01 00:00", "begin_datetime_utc": "2026-08-01 06:00",
         "pool_price": "21.63", "forecast_pool_price": "23.35", "rolling_30day_avg": "31.99"},
        {"begin_datetime_mpt": "2026-08-01 01:00", "begin_datetime_utc": "2026-08-01 07:00",
         "pool_price": "18.87", "forecast_pool_price": "22.10", "rolling_30day_avg": "32.00"},
    ]
    ail_records = [
        {"begin_datetime_mpt": "2026-08-01 00:00", "begin_datetime_utc": "2026-08-01 06:00",
         "alberta_internal_load": "10089", "forecast_alberta_internal_load": "10100"},
        {"begin_datetime_mpt": "2026-08-01 01:00", "begin_datetime_utc": "2026-08-01 07:00",
         "alberta_internal_load": "9950", "forecast_alberta_internal_load": "9980"},
    ]

    df_price_raw = make_price_json(spark, price_records)
    df_ail_raw = make_ail_json(spark, ail_records)

    result = transform_data(df_price_raw, df_ail_raw)

    assert result.count() == 2
    row = result.filter(result.begin_datetime_mpt == "2026-08-01 00:00:00").collect()[0]
    assert row["pool_price"] == 21.63 #confirms cast worked
    assert row["alberta_internal_load"] == 10089
    assert "begin_datetime_utc" not in result.columns  # confirms drop() worked


def test_transform_drops_unmatched_hours(spark):
    """If AIL is missing an hour that price has, inner join should exclude it."""
    price_records = [
        {"begin_datetime_mpt": "2026-08-01 00:00", "begin_datetime_utc": "2026-08-01 06:00",
         "pool_price": "21.63", "forecast_pool_price": "23.35", "rolling_30day_avg": "31.99"},
        {"begin_datetime_mpt": "2026-08-01 01:00", "begin_datetime_utc": "2026-08-01 07:00",
         "pool_price": "18.87", "forecast_pool_price": "22.10", "rolling_30day_avg": "32.00"},
    ]
    ail_records = [
        {"begin_datetime_mpt": "2026-08-01 00:00", "begin_datetime_utc": "2026-08-01 06:00",
         "alberta_internal_load": "10089", "forecast_alberta_internal_load": "10100"},
        # missing 01:00 on purpose
    ]

    df_price_raw = make_price_json(spark, price_records)
    df_ail_raw = make_ail_json(spark, ail_records)

    result = transform_data(df_price_raw, df_ail_raw)

    assert result.count() == 1  # only the matching hour survives the inner join