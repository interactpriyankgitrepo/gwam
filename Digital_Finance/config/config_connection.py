# Databricks notebook source
def set_connection_by_env(env):
   
    if env.lower() == 'dev':
       

        # Config required to read MDMF DB
        output_path_parquet = "/dbfs/digital_finance/output/silver_mutual_funds.parquet"
        output_path_csv = "/dbfs/digital_finance/output/silver_mutual_funds.csv"
        output_path_gold = "/dbfs/digital_finance/output/gold_business_aggregation.parquet"
        input_stocks_path = "/dbfs/digital_finance/input/stocks.json"
        input_mutual_funds_path = "/dbfs/digital_finance/input/mutual_funds.json"
        

    elif env.lower() == 'qa':

        # Config required to read MDMF DB

        output_path_parquet = "/dbfs/digital_finance/qa/output/silver_mutual_funds.parquet"
        output_path_csv = "/dbfs/digital_finance/qa/output/silver_mutual_funds.csv"
        output_path_gold = "/dbfs/digital_finance/qa/output/gold_business_aggregation.parquet"
        input_stocks_path = "/dbfs/digital_finance/qa/input/stocks.json"
        input_mutual_funds_path = "/dbfs/digital_finance/qa/input/mutual_funds.json"

    elif env.lower() == 'preprod':


        # Config required to read MDMF DB

        output_path_parquet = "/dbfs/digital_finance/preprod/output/silver_mutual_funds.parquet"
        output_path_csv = "/dbfs/digital_finance/preprod/output/silver_mutual_funds.csv"
        output_path_gold = "/dbfs/digital_finance/preprod/output/gold_business_aggregation.parquet"
        input_stocks_path = "/dbfs/digital_finance/preprod/input/stocks.json"
        input_mutual_funds_path = "/dbfs/digital_finance/preprod/input/mutual_funds.json"       
    elif env.lower() == 'prod':


        # Config required to read MDMF DB

        output_path_parquet = "/dbfs/digital_finance/prod/output/silver_mutual_funds.parquet"
        output_path_csv = "/dbfs/digital_finance/prod/output/silver_mutual_funds.csv"
        output_path_gold = "/dbfs/digital_finance/prod/output/gold_business_aggregation.parquet"
        input_stocks_path = "/dbfs/digital_finance/prod/input/stocks.json"
        input_mutual_funds_path = "/dbfs/digital_finance/prod/input/mutual_funds.json"           

        
        
    return output_path_parquet,output_path_csv,output_path_gold,input_stocks_path,input_mutual_funds_path