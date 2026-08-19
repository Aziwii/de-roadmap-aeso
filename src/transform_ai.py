"""
src/transform_ai.py
Transforms raw AESO JSON payloads into cleaned, strongly typed Parquet datasets using PySpark.
Handles APIM wrapper objects and safely parses empty string numbers to NULL.
"""

import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw")
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR", "data/clean")


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AESO_Transform_Pipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")  # Allow non-fatal nulls on invalid casts
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def extract_records(json_path: str, report_name: str) -> list:
    """
    Extracts the array of records from the AESO JSON payload.
    """
    if not os.path.exists(json_path):
        logging.warning(f"File not found: {json_path}")
        return []

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 1. AESO APIM wrapper: {"return": {"Pool Price Report": [...]}}
    if isinstance(data, dict) and "return" in data and isinstance(data["return"], dict):
        return data["return"].get(report_name, [])

    # 2. Direct wrapper: {"Pool Price Report": [...]}
    if isinstance(data, dict) and report_name in data:
        return data.get(report_name, [])

    # 3. Raw list
    if isinstance(data, list):
        return data

    logging.error(f"Unexpected JSON structure in {json_path}")
    return []


def safe_cast_double(col_name: str):
    """
    Safely casts strings to Double, converting empty strings, whitespace, and nulls to NULL.
    """
    return F.when(
        (F.col(col_name).isNull()) | (F.trim(F.col(col_name)) == ""), None
    ).otherwise(F.col(col_name)).cast(DoubleType())


def transform():
    spark = get_spark_session()
    logging.info("PySpark session initialized.")
    os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

    # =========================================================================
    # 1. Pool Price Transformation
    # =========================================================================
    pool_price_file = os.path.join(RAW_DATA_DIR, "pool_price_raw.json")
    price_records = extract_records(pool_price_file, "Pool Price Report")

    if price_records:
        logging.info(f"Extracted {len(price_records)} pool price records. Transforming with PySpark...")
        
        price_schema = StructType([
            StructField("begin_datetime_utc", StringType(), True),
            StructField("begin_datetime_mpt", StringType(), True),
            StructField("pool_price", StringType(), True),
            StructField("forecast_pool_price", StringType(), True),
            StructField("rolling_30day_avg", StringType(), True),
        ])

        df_price = spark.createDataFrame(price_records, schema=price_schema)

        df_price_clean = df_price \
            .withColumn("begin_datetime_utc", F.to_timestamp(F.col("begin_datetime_utc"))) \
            .withColumn("begin_datetime_mpt", F.to_timestamp(F.col("begin_datetime_mpt"))) \
            .withColumn("pool_price", safe_cast_double("pool_price")) \
            .withColumn("forecast_pool_price", safe_cast_double("forecast_pool_price")) \
            .withColumn("rolling_30day_avg", safe_cast_double("rolling_30day_avg")) \
            .filter(F.col("begin_datetime_utc").isNotNull()) \
            .dropDuplicates(["begin_datetime_utc"])

        price_clean_out = os.path.join(CLEAN_DATA_DIR, "pool_price_clean")
        df_price_clean.write.mode("overwrite").parquet(price_clean_out)
        logging.info(f"Successfully saved {df_price_clean.count()} clean price records -> {price_clean_out}")
    else:
        logging.warning("No pool price records found to transform.")

    # =========================================================================
    # 2. Actual Forecast Load Transformation
    # =========================================================================
    load_file = os.path.join(RAW_DATA_DIR, "actual_forecast_load_raw.json")
    load_records = extract_records(load_file, "Actual Forecast Report")

    if load_records:
        logging.info(f"Extracted {len(load_records)} load records. Transforming with PySpark...")
        
        load_schema = StructType([
            StructField("begin_datetime_utc", StringType(), True),
            StructField("begin_datetime_mpt", StringType(), True),
            StructField("alberta_internal_load", StringType(), True),
            StructField("forecast_alberta_internal_load", StringType(), True),
        ])

        df_load = spark.createDataFrame(load_records, schema=load_schema)

        df_load_clean = df_load \
            .withColumn("begin_datetime_utc", F.to_timestamp(F.col("begin_datetime_utc"))) \
            .withColumn("begin_datetime_mpt", F.to_timestamp(F.col("begin_datetime_mpt"))) \
            .withColumn("alberta_internal_load", safe_cast_double("alberta_internal_load")) \
            .withColumn("forecast_alberta_internal_load", safe_cast_double("forecast_alberta_internal_load")) \
            .filter(F.col("begin_datetime_utc").isNotNull()) \
            .dropDuplicates(["begin_datetime_utc"])

        load_clean_out = os.path.join(CLEAN_DATA_DIR, "load_clean")
        df_load_clean.write.mode("overwrite").parquet(load_clean_out)
        logging.info(f"Successfully saved {df_load_clean.count()} clean load records -> {load_clean_out}")
    else:
        logging.warning("No load records found to transform.")

    spark.stop()


if __name__ == "__main__":
    transform()