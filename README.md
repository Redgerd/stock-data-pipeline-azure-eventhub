# Stock Data Pipeline with Azure Event Hubs, Kafka, and Delta Lake on Azure Databricks

## Overview

This project implements a real-time data pipeline to ingest, process, and analyze stock market data. It is designed to run on Azure Databricks, leveraging Spark for processing and Delta Lake for storage. The pipeline reads raw data from Azure Event Hubs, uses Apache Kafka as a message bus, and stores processed data in Delta Lake for efficient querying and analysis. The pipeline is built using PySpark and [dlt](https://dlthub.com/). The repository includes a Kafka producer script, which *must* be running to provide data to the pipeline, and a Kafka consumer script for reading from the stream.

## Features

* **Real-time Data Ingestion:** Reads streaming data from Azure Event Hubs via Apache Kafka.  *Requires the Kafka producer to be running.*
* **Data Processing:** Cleanses, transforms, and enriches the data using PySpark.
* **Multi-Layered Architecture:** Organizes data into bronze, silver, and gold layers for increasing levels of refinement.
    * **Bronze Layer:** Stores raw data as is.
    * **Silver Layer:** Stores cleansed and transformed data.
    * **Gold Layer:** Stores aggregated data for reporting and analysis.
* **Scalable Storage:** Uses Delta Lake for reliable and scalable data storage.
* **Data Quality:** Ensures data quality through filtering and validation steps.
* **Modular Design:** Uses `dlt` for a modular and maintainable pipeline structure.
* **Azure Databricks Optimized:** Designed to run efficiently on Azure Databricks.
* **Kafka Integration:** Includes producer (essential for data input) and consumer scripts for interacting with the Kafka stream.

## Architecture

The data pipeline follows this architecture:

1.  **Data Source:** Stock market data is streamed from a provider into Azure Event Hubs.
2.  **Ingestion:**
    * Spark Structured Streaming reads data from Azure Event Hubs using the Kafka connector (within `stock-data-pipeline-azure-eventhub.ipynb`).
    * `Kafka(Producer).ipynb` *must* be running to simulate sending data to Kafka.  The main pipeline will not function without this data source.
3.  **Processing:**
    * **Bronze Layer:** Raw data from Kafka is stored in Delta Lake.
    * **Silver Layer:** Data is cleaned and transformed (e.g., filtering, data type conversion).
    * **Gold Layer:** Data is aggregated (e.g., calculating daily trading volume, average price).
4.  **Storage:** Delta Lake stores the data in Parquet format, providing ACID properties and schema enforcement.
5.  **Consumption:**
    * The processed data in the gold layer can be used for reporting, analysis, and visualization.
    * `Kafka(Consumer).ipynb` demonstrates how to read data from the Kafka topic.

## Prerequisites

* Azure Account
* Azure Event Hubs
* Apache Kafka
* Azure Databricks Workspace
* Python 3.x
* `dlt` library
* `PySpark` (Included in Databricks)
* `confluent-kafka`
* `requests`

## Installation

1.  **Clone the repository:**

    ```bash
    git clone <your_repository_url>
    cd stock-data-pipeline-azure-eventhub
    ```

2.  **Install the required Python packages:**

    ```
    dlt
    confluent_kafka
    requests
    azure-eventhub
    ```

    )

3.  **Configure Azure Event Hubs and Kafka:**

    * Set up an Azure Event Hubs instance.
    * Configure Kafka to connect to Azure Event Hubs. You'll need the connection string.

4.  **Set up your Azure Databricks Workspace:**

    * Create a Databricks workspace.
    * Create a cluster with the appropriate Spark version and configuration.
    * Import the `stock-data-pipeline-azure-eventhub.ipynb` notebook into your Databricks workspace.
    * Install the required libraries (`dlt`, `confluent_kafka`, `requests`, and `azure-eventhub`) on the Databricks cluster using a Databricks init script or by running `%pip install` at the beginning of the notebook.

## Usage

1.  **Configure the pipeline:**

    * Update the `stock-data-pipeline-azure-eventhub.ipynb` notebook in your Databricks workspace with your Azure Event Hubs connection details, Kafka settings, and any other environment-specific configurations. Specifically, the `kafka_bootstrap_servers` and `connection_string` variables in the Bronze Layer code.

2.  **Run the pipeline:**

    * *Before* executing the `stock-data-pipeline-azure-eventhub.ipynb` notebook, you *must* start the Kafka producer.

    * Execute the `kafka/Kafka(Producer).ipynb` notebook to simulate sending stock data to the Kafka topic. You will need to configure the Kafka producer with the appropriate settings (Kafka broker address, topic name). This can be done from your local machine or within Databricks if you set up a job.  *This producer must be running for the pipeline to receive data.*

    * Once the producer is running, execute the `stock-data-pipeline-azure-eventhub.ipynb` notebook cells in Databricks to start the streaming pipeline. The data will be ingested, processed, and stored in Delta Lake.

3.  **Consume data from Kafka:**

    * Run the `kafka/Kafka(Consumer).ipynb` notebook to read data from the Kafka topic. You will need to configure the Kafka consumer with the appropriate settings (Kafka broker address, topic name, consumer group). This can be done from your local machine or within Databricks.

4.  **Query the data:**

    * Use Spark SQL within the `stock-data-pipeline-azure-eventhub.ipynb` notebook to query the processed data in the gold layer for reporting and analysis.

## dlt Pipeline

This project uses `dlt` to define the data pipeline within the `stock-data-pipeline-azure-eventhub.ipynb` notebook. The pipeline is defined, with `@dlt.table` decorators defining the bronze, silver, and gold layers.

## Screenshots

### Bronze Layer Data

![WhatsApp Image 2025-05-11 at 00 09 22_5459ea8c](https://github.com/user-attachments/assets/1ec24f51-5340-4f53-8172-aaf28275b727)

*This image shows the raw stock data as it is initially ingested into the bronze layer.*

### Silver Layer Data

![WhatsApp Image 2025-05-11 at 00 10 02_b251a64f](https://github.com/user-attachments/assets/1552a753-f9d5-412f-869e-291da9da94e3)

*This image shows the cleansed and transformed stock data in the silver layer.*

### Gold Layer Data

![WhatsApp Image 2025-05-11 at 00 10 33_30f78f81](https://github.com/user-attachments/assets/cbb3078c-9d60-42a0-800c-365c7d764f53)

*This image shows the aggregated stock data in the gold layer, ready for analysis.*

### Data Flow

![image](https://github.com/user-attachments/assets/185b14fc-9143-4cde-abcc-8b3251757064)

