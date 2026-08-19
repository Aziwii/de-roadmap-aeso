"""
src/load_ai.py
Idempotent loader from clean Parquet files to PostgreSQL using SQLAlchemy/psycopg2.
Matches custom credentials: aeso_user / aeso_secure_password / aeso_market_db
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Database connection credentials
DB_USER = os.getenv("POSTGRES_USER", "aeso_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "aeso_secure_password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "aeso_market_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR", os.path.join(os.path.dirname(__file__), "../data/clean"))


def load_parquet_to_postgres():
    engine = create_engine(DATABASE_URL)
    logging.info(f"Connecting to database {DB_NAME} at {DB_HOST}:{DB_PORT} as {DB_USER}...")

    # --- 1. Upsert Pool Price ---
    price_clean_path = os.path.join(CLEAN_DATA_DIR, "pool_price_clean")
    if os.path.exists(price_clean_path):
        df_price = pd.read_parquet(price_clean_path)
        with engine.begin() as conn:
            df_price.to_sql("stage_pool_price", conn, if_exists="replace", index=False)
            upsert_price_sql = """
                INSERT INTO silver_pool_price (
                    begin_datetime_utc, begin_datetime_mpt, pool_price, forecast_pool_price, rolling_30day_avg
                )
                SELECT 
                    begin_datetime_utc, begin_datetime_mpt, pool_price, forecast_pool_price, rolling_30day_avg 
                FROM stage_pool_price
                ON CONFLICT (begin_datetime_utc) 
                DO UPDATE SET
                    begin_datetime_mpt = EXCLUDED.begin_datetime_mpt,
                    pool_price = EXCLUDED.pool_price,
                    forecast_pool_price = EXCLUDED.forecast_pool_price,
                    rolling_30day_avg = EXCLUDED.rolling_30day_avg,
                    updated_at = NOW();
                DROP TABLE IF EXISTS stage_pool_price;
            """
            conn.execute(text(upsert_price_sql))
            logging.info(f"Successfully upserted {len(df_price)} records into 'silver_pool_price'.")

    # --- 2. Upsert Load ---
    load_clean_path = os.path.join(CLEAN_DATA_DIR, "load_clean")
    if os.path.exists(load_clean_path):
        df_load = pd.read_parquet(load_clean_path)
        with engine.begin() as conn:
            df_load.to_sql("stage_load", conn, if_exists="replace", index=False)
            upsert_load_sql = """
                INSERT INTO silver_actual_forecast_load (
                    begin_datetime_utc, begin_datetime_mpt, alberta_internal_load, forecast_alberta_internal_load
                )
                SELECT 
                    begin_datetime_utc, begin_datetime_mpt, alberta_internal_load, forecast_alberta_internal_load 
                FROM stage_load
                ON CONFLICT (begin_datetime_utc) 
                DO UPDATE SET
                    begin_datetime_mpt = EXCLUDED.begin_datetime_mpt,
                    alberta_internal_load = EXCLUDED.alberta_internal_load,
                    forecast_alberta_internal_load = EXCLUDED.forecast_alberta_internal_load,
                    updated_at = NOW();
                DROP TABLE IF EXISTS stage_load;
            """
            conn.execute(text(upsert_load_sql))
            logging.info(f"Successfully upserted {len(df_load)} records into 'silver_actual_forecast_load'.")


if __name__ == "__main__":
    load_parquet_to_postgres()