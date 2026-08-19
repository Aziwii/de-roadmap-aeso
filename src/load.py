import os
from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
import uuid
from psycopg2.extras import execute_values


def get_connection():
    """get connection to docker db"""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

def build_records(rows, source_run_id):
    """Function: spark rows + run_id -> list of 8-tuples for the upsert"""
    return [
        (
            row.begin_datetime_mpt, 
            row.pool_price, 
            row.forecast_pool_price, 
            row.rolling_30day_avg, 
            row.alberta_internal_load, 
            row.forecast_alberta_internal_load, 
            source_run_id,  # Column 7: created_run_id
            source_run_id,  # Column 8: updated_run_id
        )
        for row in rows
    ]

def upsert_records_and_aggregate(conn, records, source_run_id):
    """Runs the upsert, returns verified loaded count. raises on failure.
        And recalculates daily aggregations. ATOMICALLY
    """
    if not records:
        return 0

    upsert_query = """
        INSERT INTO raw_hourly_grid (
            begin_datetime_mpt, 
            pool_price, 
            forecast_pool_price, 
            rolling_30day_avg, 
            alberta_internal_load, 
            forecast_alberta_internal_load, 
            created_run_id, 
            updated_run_id
        )
        VALUES %s
        ON CONFLICT (begin_datetime_mpt) DO UPDATE SET
            pool_price = EXCLUDED.pool_price,
            forecast_pool_price = EXCLUDED.forecast_pool_price,
            rolling_30day_avg = EXCLUDED.rolling_30day_avg,
            alberta_internal_load = EXCLUDED.alberta_internal_load,
            forecast_alberta_internal_load = EXCLUDED.forecast_alberta_internal_load,
            updated_run_id = EXCLUDED.updated_run_id;
    """

    # Matches SELECT and GROUP BY on DATE(begin_datetime_mpt)
    aggregate_query = """
        INSERT INTO daily_grid_agg (
            grid_date,
            avg_price,
            min_price,
            max_price,
            stddev_price,
            avg_load,
            hours_reported,
            computed_at
        )
        SELECT 
            DATE(begin_datetime_mpt) AS grid_date,
            ROUND(AVG(pool_price)::NUMERIC, 2) AS avg_price,
            MIN(pool_price) AS min_price,
            MAX(pool_price) AS max_price,
            ROUND(COALESCE(STDDEV(pool_price), 0)::NUMERIC, 2) AS stddev_price,
            ROUND(AVG(alberta_internal_load)::NUMERIC, 2) AS avg_load,
            COUNT(*) AS hours_reported,
            NOW() AS computed_at
        FROM raw_hourly_grid
        WHERE DATE(begin_datetime_mpt) IN %s
        GROUP BY DATE(begin_datetime_mpt)
        ON CONFLICT (grid_date) DO UPDATE SET
            avg_price = EXCLUDED.avg_price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            stddev_price = EXCLUDED.stddev_price,
            avg_load = EXCLUDED.avg_load,
            hours_reported = EXCLUDED.hours_reported,
            computed_at = EXCLUDED.computed_at;
    """

    cur = conn.cursor()
    try:
        # Step 1 - Raw upsert
        execute_values(cur, upsert_query, records)

        # Step 2 - Aggregate for the unique dates in the current payload
        unique_dates = tuple({r[0].date() for r in records if r[0] is not None})

        if unique_dates:
            cur.execute(aggregate_query, (unique_dates,))

        # Step 3 - Verify using updated_run_id
        cur.execute(
            "SELECT COUNT(*) FROM raw_hourly_grid WHERE updated_run_id = %s",
            (source_run_id,)
        )
        loaded_count = cur.fetchone()[0]

        unique_records_count = len({r[0] for r in records})
        assert loaded_count == unique_records_count, f"Mismatch: expected {unique_records_count}, found {loaded_count}"

        # Step 4 - Commit transaction
        conn.commit()
        return loaded_count

    except Exception as e:
        conn.rollback()
        print(f"Load failed, rolled back: {e}")
        raise
    finally:
        cur.close()


if __name__ == "__main__":
    load_dotenv()
    DATA_DIR = os.getenv("DATA_DIR")

    # create spark session
    spark = SparkSession.builder.appName('aeso-load').getOrCreate()
    df = spark.read.parquet(f"{DATA_DIR}/clean/hourly_grid_performance.parquet")
    rows = df.collect() #list of spark row objects

    #generate a uuid for the rows
    source_run_id = str(uuid.uuid4())

    records = build_records(rows, source_run_id)
    conn = get_connection()

    try:
        rowcount = upsert_records_and_aggregate(conn, records, source_run_id)
        print(f"Successfully loaded {rowcount} rows. source_run_id={source_run_id}")


    finally:
        conn.close()