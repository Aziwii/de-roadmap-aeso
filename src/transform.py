# %%
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR")

if not DATA_DIR:
    raise ValueError("DATA_DIR environment variable is not set. Check your .env file.")

spark = SparkSession.builder.appName("aeso").getOrCreate()

# %%
df_price_raw = spark.read.option("multiline", "true").json(f"{DATA_DIR}/raw/raw_pool_prices_2026-08-01_to_2026-08-10.json")
df_price_raw.printSchema()
df_price_raw.show(5, truncate=False)

df_ail_raw = spark.read.option("multiline", "true").json(f"{DATA_DIR}/raw/raw_ail_2026-08-01_to_2026-08-10.json")
df_ail_raw.printSchema()
df_ail_raw.show(5, truncate=False)

# %%
# Explode the nested JSON arrays to flatten the dataframes
from pyspark.sql.functions import explode, col

df_prices = df_price_raw.select(
    explode(col("return.`Pool Price Report`")).alias("record")
).select("record.*")

df_ail = df_ail_raw.select(
    explode(col("return.`Actual Forecast Report`")).alias("record")
).select("record.*")

df_prices.printSchema()
df_prices.show(3, truncate=False)

df_ail.printSchema()
df_ail.show(3, truncate=False)

# %%
# cast the columns to the proper data types
from pyspark.sql.functions import col, to_timestamp

df_casted_prices = df_prices.withColumn("begin_datetime_mpt", to_timestamp(col("begin_datetime_mpt"), "yyyy-MM-dd HH:mm")) \
    .withColumn("forecast_pool_price", col("forecast_pool_price").cast("double")) \
    .withColumn("pool_price", col("pool_price").cast("double")) \
    .withColumn("rolling_30day_avg", col("rolling_30day_avg").cast("double"))

df_casted_prices = df_casted_prices.drop("begin_datetime_utc")
df_casted_prices.printSchema()
df_casted_prices.show(5, truncate=False)

df_casted_ail = df_ail.withColumn("begin_datetime_mpt", to_timestamp(col("begin_datetime_mpt"), "yyyy-MM-dd HH:mm")) \
    .withColumn("alberta_internal_load", col("alberta_internal_load").cast("int")) \
    .withColumn("forecast_alberta_internal_load", col("forecast_alberta_internal_load").cast("int"))
   
df_casted_ail = df_casted_ail.drop("begin_datetime_utc")
df_casted_ail.printSchema()
df_casted_ail.show(5, truncate=False)

# %%
#join the two dataframes on the begin_datetime_mpt column
df_joined = df_casted_prices.join(df_casted_ail, on="begin_datetime_mpt", how="inner")
df_joined.printSchema()
df_joined.show(5, truncate=False)

# %%
#check for any rows where the pool price or forecast pool price is 0.0
df_joined.filter((col("pool_price") == 0.0) | (col("forecast_pool_price") == 0.0)).show()

#checking all row counts 
print("pool price rows: ", df_casted_prices.count())
print("ail rows: ", df_casted_ail.count())
print("joined rows: ", df_joined.count())
print("Unique rows: ", df_joined.dropDuplicates(["begin_datetime_mpt"]).count())

# %%
# save the joined dataframe to a parquet file
output_path = f"{DATA_DIR}/clean/hourly_grid_performance.parquet"
df_joined.write.mode("overwrite").parquet(output_path)
print(f"Successfully saved joined data to: {output_path}")

check = spark.read.parquet(f"{DATA_DIR}/clean/hourly_grid_performance.parquet")
check.show(5, truncate=False)
print(check.count())
# %%
