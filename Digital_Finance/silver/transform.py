# Databricks notebook source
# MAGIC     %run ../data/dataset

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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def process_stock_and_fund_data(input_stocks_path: str, input_mutual_funds_path: str, output_path: str, output_format: str = "parquet"):
    # Initialize SparkSession
 

    # Load JSON data
    stocks_df = spark.read.json(input_stocks_path)
    mutual_funds_df = spark.read.json(input_mutual_funds_path)

    # Basic transformations
    # Flatten the nested structure and parse timestamps
    stocks_df = stocks_df.select(
        "stock_id", "company_id", "symbol", "price", "volume",
        to_timestamp(col("timestamp")).alias("stock_timestamp"),
        col("details.exchange").alias("exchange"),
        col("details.sector").alias("sector")
    )

    mutual_funds_df = mutual_funds_df.select(
        "fund_id", "name", "company_id", "holdings",
        to_timestamp(col("timestamp")).alias("fund_timestamp")
    )

    # Deduplicate data
    stocks_df = stocks_df.dropDuplicates(["stock_id"])
    #display(stocks_df)
    mutual_funds_df = mutual_funds_df.dropDuplicates(["fund_id"])
    #display(mutual_funds_df)

    # Join operation: Find mutual funds with stocks using 'company_id'
    joined_df = stocks_df.join(mutual_funds_df, on="company_id", how="inner")

    # Enrich joined data by calculating the total value of holdings in each mutual fund
    joined_df_enriched = joined_df.withColumn("total_value", col("price") * col("holdings"))
    #display(joined_df_enriched)
    # Write the DataFrame to the specified format
    if output_format.lower() == "parquet":
        joined_df_enriched.write.mode("overwrite").parquet(output_path)
    elif output_format.lower() == "csv":
        joined_df_enriched.write.mode("overwrite").csv(output_path, header=True)
    else:
        raise ValueError("Unsupported output format. Please choose 'parquet' or 'csv'.")



# Sample usage
# process_stock_and_fund_data("stocks.json", "mutual_funds.json", "output_parquet", "parquet")

# COMMAND ----------

#output_path,input_stocks_path,input_mutual_funds_path
process_stock_and_fund_data(input_stocks_path, input_mutual_funds_path, output_path_parquet, "parquet")

# COMMAND ----------

process_stock_and_fund_data(input_stocks_path, input_mutual_funds_path, output_path_csv, "csv")

# COMMAND ----------

files = dbutils.fs.ls(output_path_parquet)
display(files)


# COMMAND ----------

silver_mutual_funds_parquet = spark.read.parquet(output_path_parquet,header=True, inferSchema=True)

# COMMAND ----------

silver_mutual_funds_csv = spark.read.csv(output_path_csv,header=True, inferSchema=True)

# COMMAND ----------

display(silver_mutual_funds_csv)

# COMMAND ----------

display(silver_mutual_funds_parquet)