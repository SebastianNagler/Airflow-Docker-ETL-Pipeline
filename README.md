# **Scalable Automated Cloud ETL for Swiss Public Transport Data**

## **High-Level Objective**

This project implements a scalable, automated ETL pipeline. Its purpose is to retrieve daily Swiss public transport data from the [SIRI-PT API](https://opentransportdata.swiss/en/cookbook/realtime-prediction-cookbook/siri-et-pt/), transform the large XML datasets into the Parquet format, and store them in a partitioned AWS S3 data lake for later analysis.  
The entire process is orchestrated using Apache Airflow, with all tasks containerized via Docker.

## **Architecture Diagram**

```mermaid
graph TD
   classDef default fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef cloud fill:#d4e6f1,stroke:#3498db,color:#000;
    classDef storage fill:#fdebd0,stroke:#f39c12,color:#000;
    classDef compute fill:#e8f8f5,stroke:#1abc9c,color:#000;
    classDef orchestrator fill:#f4ecf7,stroke:#8e44ad,color:#000;

    subgraph "External Source" [fa:fa-cloud]
        direction TB
        API[fa:fa-database OpenTransportData.swiss SIRI-PT API]
        class API cloud;
    end

    subgraph id_local_env ["Local Docker Environment"]
        direction TB
        subgraph "Apache Airflow (Orchestrator)" [fa:fa-wind]
            A[fa:fa-clock Airflow Scheduler]
            class A orchestrator;
        end

        subgraph "Docker Host (Execution)" [fa:fa-docker]
            D_E["fa:fa-docker extract_task (Container)"]
            D_T["fa:fa-docker transform_task (Container)"]
            class D_E,D_T compute;
        end
    end

    subgraph "Data Lake (AWS S3)" [fa:fa-aws]
        direction TB
        S3_Raw["fa:fa-folder siri_pt_raw/ (Raw XML)"]
        S3_Processed["fa:fa-file-excel siri_pt/ (Processed Parquet)"]
        class S3_Raw,S3_Processed storage;
    end

    A -- 1. Triggers 'extract_raw_data' @daily --> D_E
    D_E -- 2. GET Request (w/ API Key) --> API
    D_E -- 3. Stream-Uploads XML --> S3_Raw
    A -- 4. Triggers 'transform_to_parquet' (on success) --> D_T
    D_T -- 5. Reads/Streams XML from --> S3_Raw
    D_T -- 6. Writes Parquet Chunks to --> S3_Processed
   ```

## **Tech Stack**

| Category | Technology | Purpose |
| :---- | :---- | :---- |
| **Orchestration** | Apache Airflow | Scheduling, monitoring, and executing the daily ETL pipeline. |
| **Containerization** | Docker, Docker Compose | Creating reproducible environments for Airflow and the ETL tasks. |
| **Data Lake** | AWS S3 | Storing raw XML data and processed, partitioned Parquet files. |
| **Data Processing** | Python | Language for ETL logic. |
|  | Pandas, PyArrow | Data structuring and high-performance, memory-efficient Parquet writing. |
|  | xml.etree.ElementTree | Memory-efficient streaming parser (iterparse) for large XML files. |
| **Data Access** | s3fs, boto3 | Interacting with AWS S3 for direct-to/from-cloud I/O. |
|  | requests | Extracting raw data from the SIRI-PT HTTPS endpoint. |

## **Key Design Features**

This pipeline was created with the following data engineering best practices to improve scalability, security, and robustness.

### **1\. Secure and Externalized Configuration**

No API keys or AWS credentials are hardcoded; all sensitive information is managed by assigning variables in Apache Airflow. Airflow injects the variables as environment variables into a given new ETL container at the moment the tasks executes.

### **2\. Scalable, Low-Memory Transformation**

Since the raw SIRI-PT XML data is larger than 500 megabytes (on most days), loading the entire file into memory (e.g., via `pandas.read_xml()`) risks causing an Out-Of-Memory (OOM) error.  
This pipeline solves this by via streaming. It uses Python's ET.iterparse() to read the XML file incrementally and thereby limit memory consumption. Furthermore, after extracting XML data onto S3, it is accumulated into chunks node-by-node while being parsed; once a chunk reaches a defined size (`CHUNK_SIZE`), it is converted to a PyArrow table and written to S3 using pyarrow.parquet.ParquetWriter.

### **3\. Stateless, Cloud-Native Design**

The ETL tasks are entirely stateless. The Docker containers running the extract and transform logic do not use the local filesystem for data. During extraction, data is streamed directly from the API to the S3 raw layer. During transformation, data is streamed directly from the S3 raw layer and written directly to the S3 processed layer. Consequently, the tasks can be run on any container-based service without modification.
 
### **4\. Granular and Idempotent Orchestration**

The Airflow DAG is intentionally split into two distinct, dependent tasks: extract\_raw\_data (executed first) and transform\_to\_parquet (executed subsequently).
If the transform step fails, it can be re-run independently without re-executing the extract step. This saves time, reduces frequency of API calls, and simplifies debugging.

## **How to Run (Locally)**

### **Prerequisites**

* Docker & Docker Compose  
* AWS Account with an S3 bucket and IAM user (with s3:PutObject, s3:GetObject permissions).  
* SIRI-PT API Key from [OpenTransportData.swiss](https://api-manager.opentransportdata.swiss).

### **1\. Configuration**

1. **Clone the Repository:**  
   * `git clone https://github.com/sebastiannagler/Airflow-Docker-ETL-Pipeline.git`

   * `cd Airflow-Docker-ETL-Pipeline`

2. Create .env File:  
   * Create a .env file by duplicating .env.example:  
   * `cp .env.example .env`

3. Edit .env:  
   * Run `id /u` in the terminal and replace the value of `AIRFLOW_UID` by the result.
   * If using macOs, run `stat -f '%g' /var/run/docker.sock` in the terminal; if using Linux, run `stat -c '%g' /var/run/docker.sock` in the terminal. Replace the value of `DOCKER_GID` by the result.

### **2\. Build the ETL Image**

The Airflow DockerOperator requires the ETL image (my-siri-etl:latest) to be available on the Docker host. 

Build it locally by running `docker build \-t my-siri-etl:latest .`.

### **3\. Launch Airflow**

Run the local Airflow cluster using Docker Compose:  
`docker-compose up \-d`

It may take a few minutes for all Airflow services (scheduler, webserver, worker) to initialize.

### **4\. Set Airflow Variables**

1. Open the Airflow UI in your browser: [http://localhost:8080](http://localhost:8080)
2. Log in (default: airflow / airflow).  
3. Navigate to `Admin` \-\> `Variables`.  
4. Create the following five variables. The DockerOperator in the DAG will read these and pass them to the ETL container.  
   * aws\_access\_key\_id: Your AWS IAM user's access key.  
   * aws\_secret\_access\_key: Your AWS IAM user's secret key.  
   * siri\_api\_key: Your SIRI-PT API key.  
   * s3\_bucket\_name: The name of your S3 bucket.
   * aws\_region: The AWS region for your S3 bucket (e.g., `eu-central-2` for Switzerland).

### **5\. Run the Pipeline**

1. On the Airflow UI homepage, find the siri\_pt\_daily\_etl DAG.  
2. Click the toggle on the left to un-pause it.  
3. To run it immediately, click the "Play" button (Trigger) on the right. Otherwise, it will run on its next scheduled @daily interval.

## **Pipeline Showcase**

If both the extraction and execution were successful, the Airflow UI Grid view will look similar to the one shown below. Two corresponding green checkmarks should appear below a green bar representing the task.

![alt text](assets/Screenshot-Airflow-success.png)

An AWS S3 directory structure that categorizes files based on formats (.xml, .parquet) and date (year, month, day) is automatically extended for every processed data file. This approach (partition pruning) enables AWS Athena to avoid reading all files when only one is relevant to a given query. The screenshots below taken from the AWS S3 console view provide two examples.

![alt text](assets/S3-Raw-Partition.jpeg)

![alt text](assets/S3-Transformed-Partition.jpeg)