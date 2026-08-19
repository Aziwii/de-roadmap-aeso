
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode
import glob

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR")

if not DATA_DIR:
    raise ValueError("DATA_DIR environment variable is not set. Check your .env file.")

spark = SparkSession.builder.appName("aeso").getOrCreate()

def transform_data(df_price_raw, df_ail_raw):
    df_prices = df_price_raw.select(
        explode(col("return.`Pool Price Report`")).alias("record")
    ).select("record.*")

    df_ail = df_ail_raw.select(
        explode(col("return.`Actual Forecast Report`")).alias("record")
    ).select("record.*")

    # cast the columns to the proper data types
    df_casted_prices = df_prices.withColumn("begin_datetime_mpt", to_timestamp(col("begin_datetime_mpt"), "yyyy-MM-dd HH:mm")) \
        .withColumn("forecast_pool_price", col("forecast_pool_price").cast("double")) \
        .withColumn("pool_price", col("pool_price").cast("double")) \
        .withColumn("rolling_30day_avg", col("rolling_30day_avg").cast("double"))

    df_casted_ail = df_ail.withColumn("begin_datetime_mpt", to_timestamp(col("begin_datetime_mpt"), "yyyy-MM-dd HH:mm")) \
        .withColumn("alberta_internal_load", col("alberta_internal_load").cast("int")) \
        .withColumn("forecast_alberta_internal_load", col("forecast_alberta_internal_load").cast("int"))

    #dont need the utc time col
    df_casted_prices = df_casted_prices.drop("begin_datetime_utc")
    df_casted_ail = df_casted_ail.drop("begin_datetime_utc")

    #join the two dataframes on the begin_datetime_mpt column
    df_joined = df_casted_prices.join(df_casted_ail, on="begin_datetime_mpt", how="inner")

    #gaurdrail against duplicate timestamps after the join
    assert df_joined.dropDuplicates(["begin_datetime_mpt"]).count() == df_joined.count(), "Duplicate timestamps found"

    return df_joined

def save_partquet(df_joined, output_path):
    #writes dataframe to disk
    df_joined.write.mode("overwrite").parquet(output_path)
    print(f"Successfully saved joined data to: {output_path}")

if __name__ == "__main__":
    #glob for resolving the file parth into a list before handing over to pyspark
    price_files = glob.glob(f"{DATA_DIR}/raw/raw_pool_prices_*.json")
    ail_files = glob.glob(f"{DATA_DIR}/raw/raw_ail_*.json")

    df_price_raw = spark.read.option("multiline", "true").json(price_files)
    df_ail_raw = spark.read.option("multiline", "true").json(ail_files)

    df_joined = transform_data(df_price_raw, df_ail_raw)
    save_partquet(df_joined, f"{DATA_DIR}/clean/hourly_grid_performance.parquet")