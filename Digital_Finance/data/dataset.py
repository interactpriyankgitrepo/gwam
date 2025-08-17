# Databricks notebook source
#json sample dataset

# COMMAND ----------

stocks = [
  {
    "stock_id": 1,
    "company_id": 101,
    "symbol": "AAPL",
    "price": 150.0,
    "volume": 10000,
    "timestamp": "2023-10-01T10:00:00",
    "details": {
      "exchange": "NASDAQ",
      "sector": "Technology"
    }
  },
  {
    "stock_id": 2,
    "company_id": 102,
    "symbol": "GOOGL",
    "price": 2800.0,
    "volume": 20000,
    "timestamp": "2023-10-01T11:00:00",
    "details": {
      "exchange": "NASDAQ",
      "sector": "Technology"
    }
  },
  {
    "stock_id": 3,
    "company_id": 103,
    "symbol": "AMZN",
    "price": 3400.0,
    "volume": 15000,
    "timestamp": "2023-10-01T12:00:00",
    "details": {
      "exchange": "NASDAQ",
      "sector": "E-commerce"
    }
  }
]

# COMMAND ----------

mutual_funds = [
  {
    "fund_id": 1,
    "name": "Tech Fund",
    "company_id": 101,
    "holdings": 5000,
    "timestamp": "2023-10-01T10:30:00"
  },
  {
    "fund_id": 2,
    "name": "Growth Fund",
    "company_id": 103,
    "holdings": 3000,
    "timestamp": "2023-10-01T11:30:00"
  },
  {
    "fund_id": 3,
    "name": "Index Fund",
    "company_id": 104,
    "holdings": 7000,
    "timestamp": "2023-10-01T12:30:00"
  }
]