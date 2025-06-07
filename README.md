# Change Data Capture (CDC) Pipeline with Kafka, Debezium, Spark & S3

This project implements a real-time **Change Data Capture (CDC) pipeline** using Dockerized PostgreSQL, Kafka, Debezium, and Apache Spark Structured Streaming. It captures changes from a PostgreSQL database and streams them to Amazon S3 in Parquet format.

## Tech Stack

| Layer                  | Technology                                        |
|------------------------|---------------------------------------------------|
| **Database**           | PostgreSQL (Docker container)                     |
| **CDC Connector**      | Debezium PostgreSQL Connector (via Kafka Connect) |
| **Streaming Platform** | Kafka + Zookeeper + Kafka UI (Docker)            |
| **Data Processor**     | Apache Spark Structured Streaming (PySpark)       |
| **Storage**            | Amazon S3 (Sink + Checkpoint store)               |
| **Orchestration**      | Docker Compose + Tailscale VPN                    |
| **Cluster**            | Spark Cluster (1 Master, 1 Worker on AWS EC2)     |


##  Architecture Overview

```
[PostgreSQL (Docker)] → [Debezium Kafka Connector] → [Kafka (Docker)]
                                     ↓
                             [Kafka UI for inspection]
                                     ↓
     ┌─────────────────────── Local VM (Driver) ─────────────────────┐
     │  Structured Streaming Driver (PySpark)                        │
     │  Connects to Spark Master (EC2) via Tailscale                 │
     └───────────────────────────────────────────────────────────────┘
                                     ↓
                     [Spark Executors on EC2 (Master + Worker)]
                                     ↓
                      → Amazon S3 (Parquet Output + Checkpoints)
```

Note: Tailscale VPN can be skipped/ not needed if the machine that runs docker is reachable from public network and has unique IP.

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/yourname/cdc-pipeline-pyspark.git
cd cdc-pipeline-pyspark
```

### 2. Spin up Docker containers (PostgreSQL, Kafka, Debezium)

```bash
docker-compose up -d
```

> Ensure `docker-compose.yml` includes:
- PostgreSQL (We added our startup_script to initdb so it adds the csv files to our tables during initialization and also config files required for debezium are moved to /postgres/config)
- Zookeeper
- Kafka: The docker-compose.yml defines different broker URLs for internal (within Docker network) and external (public network) access. Ensure the advertised.listeners is correctly set to the Tailscale IP (or public IP) to allow the Spark driver node to connect.
- Kafka Connect with Debezium plugin
- Kafka UI

### 3. Configure Debezium Connector

Use REST API to register Debezium connector to monitor specific PostgreSQL tables.

Run this from the same machine where the Docker containers are running. If you're using an external IP or Tailscale, replace localhost with the external Kafka Connect host.

```bash
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @Docker/debezium/register-postgres-connector.json
```

### 4. Spark Setup

- Spark installed manually on EC2 instances (Master + Worker) - Make sure that we copy the config files from config_files_for_cluster to /opt/spark/conf (I ran my job in resource constrained environment, feel free to change the configs as needed). 
- After installation - move [kafka-clients-3.4.1.jar,spark-token-provider-kafka-0-10_2.12-3.5.5.jar,spark-sql-kafka-0-10_2.12-3.5.5.jar,aws-java-sdk-bundle-1.12.262.jar,hadoop-aws-3.3.4.jar] to /opt/spark/jars in all driver node, master node and worker node.
- Configure AWS credentials (e.g., via ~/.aws/credentials, IAM role, or environment variables) on all Spark nodes. Ensure the IAM role or user has s3:PutObject, s3:ListBucket, and s3:GetObject permissions for both the output and checkpoint paths.
- Spark master and worker configured with Tailscale IPs (We need to add our node that runs docker to this network and driver node if it is different). 
- Have security group attached to our EC2 instances such that there is nothing that will be blocking our driver-cluster connection.
- before running the spark-submit - zip your spark_app so that all the dependent packages are sent to the spark cluster [below command assumes that we have set our configs in /opt/spark.conf/spark-default.conf in driver node).

### 5. Run Spark Streaming Job from Local VM

```bash
spark-submit   --master spark://<tailscale_master_ip>:7077   --py-files spark_files.zip   main.py --job flights
```

- Driver process runs locally
- Executors run on AWS EC2 cluster
- Output and checkpoint written to Amazon S3

## Project Structure

```
cdc-pipeline-spark-kafka/
├── README.md                            # Project documentation
├── data/
│   └── synthetic_data.py                # Script to generate sample data
├── Docker/
│   ├── docker-compose.yml              # Docker Compose to run PostgreSQL, Kafka, Debezium, etc.
│   ├── debezium/
│   │   ├── register-postgres-connector.json   # Connector config for Debezium
│   │   └── validate_connector.json            # Connector validation request
│   └── postgres/
│       ├── config/
│       │   ├── pg_hba.conf             # PostgreSQL authentication config
│       │   └── postgres.conf           # PostgreSQL server config
│       ├── data/
│       │   ├── bookings.csv            # Sample bookings data
│       │   ├── customer.csv            # Sample customer data
│       │   └── flights.csv             # Sample flight data
│       └── scripts/
│           └── startup_script.sql      # SQL script to initialize DB
├── spark_app/
│   ├── main.py                         # Driver script to trigger CDC jobs
│   ├── config/
│   │   ├── schema.py                   # Schema definitions
│   │   └── settings.py                 # Spark and AWS config
│   ├── config_files_for_cluster/
│   │   ├── spark-default.conf          # Spark defaults for cluster execution
│   │   └── spark-env.sh                # Spark environment config
│   ├── jobs/
│   │   ├── bookings_cdc.py             # CDC logic for bookings table
│   │   ├── customers_cdc.py            # CDC logic for customers table
│   │   └── flights_cdc.py              # CDC logic for flights table
│   └── utils/
│       ├── CDCBaseClass.py             # Reusable base class for CDC logic
│       ├── exceptions.py               # Custom exceptions
│       └── logger_config.py            # Logging configuration

```

##  Logging & Monitoring

- Kafka UI to inspect topics
- Spark master UI (`<tailscale-master-ip>:8080`) to monitor job execution (for spark application UI - <tailscale-driver-ip>:4040. worker UI - <tailscale-worker-ip>:8081)
- Logs written via custom logger in `utils/logger.py`

##  Features

- Real-time CDC from PostgreSQL
- Modular Spark jobs for each table
- Decoupled config and exception handling
- Uses Tailscale VPN for secure distributed setup
- Checkpointing for fault-tolerance
- Output in **Parquet** format (S3-optimized)

## Future Improvements

- Add unit tests for job logic  
- Implement schema evolution handling  
- Integrate data catalog (e.g., AWS Glue)  
- Add data validation and alerting pipeline