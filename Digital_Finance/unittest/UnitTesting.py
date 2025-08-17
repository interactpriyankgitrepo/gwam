# Databricks notebook source
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
def price_adjusted(x):
    return x * .97

# COMMAND ----------

import unittest

class Testpriceadjusted(unittest.TestCase):
    def test_price_adjusted(self):
        # Test cases for the add_numbers function
        self.assertEqual(price_adjusted(100), 97)
        self.assertEqual(price_adjusted(200), 194)
        self.assertEqual(price_adjusted(0), 0)
        #self.assertEqual(price_adjusted(-1, -1), -2)
        #self.assertEqual(price_adjusted(2.5, 2.5), 5.0)

# Run the tests
suite = unittest.TestLoader().loadTestsFromTestCase(Testpriceadjusted)
unittest.TextTestRunner().run(suite)

# COMMAND ----------

import unittest
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row

class TestStockAndFundPipeline(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestStockAndFundPipeline") \
            .master("local[1]") \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_process_stock_and_fund_data(self):
        # Test data
        stocks_data = [
            Row(stock_id=1, symbol="AAPL", price=150.0, volume=10000),
            Row(stock_id=2, symbol="GOOGL", price=2800.0, volume=20000),
            Row(stock_id=3, symbol="AMZN", price=3400.0, volume=15000)
        ]

        mutual_funds_data = [
            Row(fund_id=1, name="Tech Fund", symbol="AAPL", holdings=5000),
            Row(fund_id=2, name="Growth Fund", symbol="AMZN", holdings=3000)
        ]

        stocks_df = self.spark.createDataFrame(stocks_data)
        mutual_funds_df = self.spark.createDataFrame(mutual_funds_data)

        output_path = "test_output_parquet"

        # Running the process_stock_and_fund_data function
        def mock_process_stock_and_fund_data(stocks_df, mutual_funds_df, output_path):
            joined_df = stocks_df.join(mutual_funds_df, on="symbol", how="inner")
            joined_df_enriched = joined_df.withColumn("total_value", col("price") * col("holdings"))
            joined_df_enriched.write.mode("overwrite").parquet(output_path)

        mock_process_stock_and_fund_data(stocks_df, mutual_funds_df, output_path)

        # Load the result and verify
        result_df = self.spark.read.parquet(output_path)
        
        # Expected data
        expected_data = [
            Row(stock_id=1, symbol="AAPL", price=150.0, volume=10000, fund_id=1, name="Tech Fund", holdings=5000, total_value=750000.0),
            Row(stock_id=3, symbol="AMZN", price=3400.0, volume=15000, fund_id=2, name="Growth Fund", holdings=3000, total_value=10200000.0)
        ]

        expected_df = self.spark.createDataFrame(expected_data)

        # Assert DataFrame equality
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

        # Clean up
        shutil.rmtree(output_path)

if __name__ == '__main__':
    unittest.main()