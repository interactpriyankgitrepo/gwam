# Databricks notebook source
# MAGIC     %run ../data/dataset
# MAGIC     

# COMMAND ----------

dbutils.widgets.text("env","dev")
p_env = dbutils.widgets.get("env")
print(p_env)

# COMMAND ----------

# MAGIC %run ../config/config_connection

# COMMAND ----------

output_path_parquet,output_path_csv,output_path_gold,input_stocks_path,input_mutual_funds_path = set_connection_by_env(p_env)
print(output_path_parquet,output_path_csv,output_path_gold,input_stocks_path,input_mutual_funds_path)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
def price_adjusted(x):
    return x * .97

# Register UDF
price_adjusted_udf = udf(price_adjusted, IntegerType())

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, col

def prepare_business_aggregations(df,output_path_gold):
    # Ensure the 'volume' column is of numeric type
    df = df.withColumn("volume", col("volume").cast("integer"))
    df = df.withColumn("price_adjusted", price_adjusted(df["price"]))
    df.write.mode("overwrite").parquet(output_path_gold)
    return df
    

# COMMAND ----------

silver_mutual_funds_parquet_df = spark.read.parquet(output_path_parquet,header=True, inferSchema=True)

# COMMAND ----------

display(silver_mutual_funds_parquet_df)

# COMMAND ----------

business_aggregations_df = prepare_business_aggregations(silver_mutual_funds_parquet_df,output_path_gold)

# COMMAND ----------

display(business_aggregations_df)