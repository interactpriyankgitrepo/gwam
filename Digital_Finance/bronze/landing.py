# Databricks notebook source
# MAGIC     %run ../data/dataset

# COMMAND ----------

display(stocks)

# COMMAND ----------

display(mutual_funds)

# COMMAND ----------

from pyspark.sql import DataFrame
def save_dataframe_as_json(df: DataFrame, output_path: str, mode: str = "overwrite"):
    """
    Save a PySpark DataFrame as a JSON file.

    Parameters:
    - df (DataFrame): The PySpark DataFrame to be saved.
    - output_path (str): The directory path where the JSON files will be saved.
    - mode (str): The write mode, e.g., "overwrite", "append", "ignore", or "error".

    Returns:
    None
    """
    df = spark.createDataFrame(df)
    # Write the DataFrame to the specified path in JSON format
    df.write.mode(mode).json(output_path)
    print(f"DataFrame successfully saved as JSON to {output_path}")

# COMMAND ----------

 save_dataframe_as_json(stocks,"/dbfs/digital_finance/input/stocks.json")

# COMMAND ----------

 save_dataframe_as_json(mutual_funds,"/dbfs/digital_finance/input/stocks.json")

# COMMAND ----------

files = dbutils.fs.ls("/dbfs/digital_finance/input/")
display(files)