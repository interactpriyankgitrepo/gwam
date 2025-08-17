## Digital Finance Data Pipeline

## Overview

DIGITAL FINANCE is a scalable data pipeline designed to ingest, process, and store JSON event data from multiple sources, such as mobile apps, web apps, and APIs. This solution leverages Azure's cloud services to ensure efficient data handling, scalability, fault tolerance, and security.

## Architecture Components

### Data Ingestion

- **Azure Event Hubs**: Streams high-throughput JSON event data from various sources.
- **Azure IoT Hub**: Manages data ingestion from IoT devices.
- **Azure API Management**: Secures and manages API calls efficiently.

### Data Processing and Transformation

- **Azure Databricks**: Utilizes PySpark for distributed data processing.
  - **PySpark Streaming**: Real-time data processing.
  - **PySpark UDFs**: Custom transformations for enrichment.
- **Delta Lake**: Ensures ACID transactions and efficient data handling.

### Data Storage

- **Azure Data Lake Storage (ADLS) Gen2**: Stores raw and processed data.
- **Delta Tables**: Efficient storage for querying and machine learning.

### Security and Access Management

- **Azure Key Vault**: Secures sensitive credentials.
- **Azure Active Directory**: Manages authentication and authorization.

### Monitoring and Fault Tolerance

- **Azure Monitor**: Provides comprehensive monitoring and alerting.
- **Azure Databricks / Apache Airflow**: Schedules and monitors data tasks.

### Disaster Recovery and Scalability

- **Geo-Redundant Storage (GRS)**: Replicates data across regions for disaster recovery.
- **Azure Databricks Autoscaling**: Adjusts resources dynamically based on workload.

### DevOps

- **CI/CD with Jenkins**: Automates integration and deployment processes.
  - **Jenkins Pipelines**: Manages CI/CD using Jenkinsfiles.

## Data Flow

1. **Ingestion**: JSON event data is captured by Azure Event Hubs.
2. **Processing**: PySpark in Databricks processes the data in real-time.
3. **Storage**: Transformed data is stored in ADLS as Delta Tables.
4. **Access**: Data scientists and analysts access data through Databricks Notebooks or analytics tools.

## Reflection

### Trade-offs

- **Complexity vs. Flexibility**: Balancing flexible data transformations with pipeline complexity.
- **Cost vs. Performance**: Managing cost while ensuring high performance.
- **Real-time vs. Batch Processing**: Tuning resources for low-latency, real-time processing.

### Improvements

- **Enhanced Monitoring**: Implement more granular monitoring systems.
- **Optimization**: Further optimize PySpark jobs for better performance.
- **Data Quality Framework**: Integrate a robust data validation system.
- **Automated Testing**: Expand automated testing for pipeline components.

### Scaling

- **Partitioning and Sharding**: Optimize Event Hubs and Delta Tables.
- **Elastic Scaling**: Use Event Hubs' auto-inflate feature for throughput adjustment.
- **Advanced Caching**: Implement caching for faster data retrieval.
- **Micro-batching**: Tune PySpark Streaming for efficient processing.

## Conclusion

This architecture provides a robust, scalable, and secure solution for data management, leveraging Azure's capabilities for high availability and disaster recovery. The use of PySpark and Delta Lake facilitates real-time data transformation tailored to business needs.

## Disaster Recovery Features

Azure supports robust disaster recovery with geo-redundant storage and automatic failover mechanisms, ensuring data protection and minimal downtime.

## Overview if you are running in local machine environment

This project implements a data pipeline using PySpark to process stock market and mutual fund data. The pipeline reads JSON files, performs transformations, and writes the data in the desired format (Parquet or CSV). We are calling this project as DIGITAL FINANCE. For pyspark we are using databricks if you want to run into local pyspark environment. Follow the below steps

## Setup Instructions
   - Create virtual environment: `python -m venv .venv_gwam`
   - Activate virtual environment: `source .venv_gwam/Scripts/activate`
   - Deactivate virtual environment: `deactivate`
   - Install requirements.txt `pip install -r requirements.txt`


## Setup Instructions

1. **Prerequisites**:
   - Install PySpark: `pip install pyspark`
   - If writing to Parquet, ensure `pyspark` package supports it by default.
   
2. **Data Files**:
   - Place `stocks.json` and `mutual_funds.json` in your local directory or cloud storage.
   
3. **Run the Pipeline**:
   - Execute the script: 
     ```bash
     python transform.py
     ```

4. **Unit Tests**:
   - Run the unit tests using:
     ```bash
     python -m unittest UnitTesting.py
     ```
## Recommendation is to run in databricks because of managed service and security compliance. It provides better collaborative environment.
## Approach

- **Data Loading**: Reads JSON files from the specified path.
- **Transformations**: Includes deduplication, timestamp parsing, and joining datasets on a numerical identifier (`company_id`).
- **Data Writing**: Writes the transformed data to either Parquet or CSV format based on user input.
- **Testing**: Includes unit tests to verify key transformation logic, such as deduplication.

Explanation
Script: The main script handles loading, transforming, and writing data. It supports deduplication and timestamp parsing.
Unit Test: Tests the deduplication logic to ensure duplicates are removed based on stock_id.
README: Provides setup instructions and an explanation of the pipeline's approach.

This setup provides a complete solution for processing and transforming stock and mutual fund data using PySpark, including necessary unit tests and documentation.
