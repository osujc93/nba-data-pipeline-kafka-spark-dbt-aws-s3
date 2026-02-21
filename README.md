# NBA-Data-Pipeline
Data pipeline for batch + incremental ingestion of NBA player boxscores using Kafka, Spark, Dbt, AWS S3 and more.

![Diagram](/assets/PipelineDiagram.png)

Contents
=================

<!--ts-->
   * [Summary](#summary)
   * [Setup](#Setup)
   * [Tables](#tables) 
   * [Kafka (KRaft Mode)](#kafka-kraft-mode)
      * [Kafka-UI (Optional)](#kafka-ui-optional)
   * [Spark (Standalone)](#spark-standalone)
      * [Zookeeper HA](#zookeeper-ha)
   * [Dbt](#dbt)
   * [AWS-S3](#aws-s3)
   * [Hive-Metastore](#hive-metastore)
   * [Trino](#trino)
   * [Superset](#superset)
   * [Airflow](#airflow)
   * [Postgres](#postgres)
<!--te-->

Summary
============

### Extract

- Kafka to fetch player boxscores from NBA stats API and send to Kafka topic.

- Took data with initial batch ingestion of boxscores from 1946 to current date. and then daily from the last ingestion date.

- The endpoint has rate limits. So to prevent timeouts, I used a token-bucket rate limiter to space out requests, one every seven seconds. 

- And set ThreadPoolExecutor with max_workers=1 so each API request finishes fully before the next one starts.

- After the initial batch finishes, it inserts a last-ingestion date into Postgres, so that subsequent incremental runs know exactly where to resume from.

- The incremental runs look up the last-ingested date in Postgres, fetches only the new data from that date forward, sends it to Kafka, then updates the last-ingested date for the next run.

### Load

- Spark job to process data from topic to Iceberg tables stored in my S3 bucket.

- Read from Kafka’s earliest offset to capture all boxscores. 

- The maximum consumed offset for each partition is extracted, incremented by one, stored in a Postgres table as JSON. And then used as the starting offset in subsequent runs so the pipeline resumes exactly where it left off without duplicating any records.

- I made the mistake of over-partitioning my Iceberg table at first, which caused Spark to create a bunch of tiny files across many partitions. Most of the runtime went to Iceberg manifest/metadata work and S3 commit overhead rather than actually writing data.

- Switching to a simpler partition combo like <code>season</code> and <code>season_type</code> reduced the number of files and made the job finish in minutes.

### Transform 

- Dbt models to transform the original <code>nba_player_boxscores</code> table.

- Consolidate individual player-level stats into a single team-level row per game.

- Apply SCD Type 2 merges in Iceberg tables to track historical changes for players and teams.

- Aggregate boxscores into advanced stats based on those defined in NBA's stats glossary.

- I ran my dbt models through Trino on using an Iceberg catalog. dbt produces the SQL, Trino executes it on my Iceberg tables, and Hive Metastore keeps track of the S3 file paths, so the transformed results are saved back to my S3 bucket.

### Storage

- All Iceberg table data, metadata, and Spark job logs are stored in my S3 bucket. 

- Hive serves as the central metastore that manages the schemas, partitions, and table definitions for Iceberg.

### Query

- Either using Trino-cli in terminal or

- SQL Lab in Superset

### Visualize

- Use Trino's endpoint as a datasource in Superset to visualize data from Iceberg tables through interactive charts and dashboards.

### Orchestration

- Once all containers are up and running, Airflow automates entire workflow

- Set <code>is_paused_upon_creation</code> to False so that dag starts once Airflow is running

- With <code>schedule_interval="@daily"</code>, the workflow is every 24 hours at midnight EDT.

- For airflow to dynamically decide which set of tasks to run, I use a <code>BranchPythonOperator</code> to check the <code>init_ingestion_done</code> Variable.

- If it’s <code>False</code>, Airflow runs my initial batch chain to ingest all boxscores. And once finished, it sets the flag to <code>True</code> so future daily runs automatically take the incremental path. This way I do the huge historical load only once, then each day I process just the new data.

## Setup

32 gb of memory for VM/computer 

1. Clone repository:
   ```sh
   $ git clone https://github.com/osujc93/nba-data-pipeline-kafka-spark-dbt-aws-s3
   $ cd nba-data-pipeline-kafka-spark-dbt-aws-s3
   ```

2. Build and start services:
   
   Ensure you have Docker & Docker Compose installed on your machine.
   
   ```sh
   $ docker-compose build airflow-base spark-base

   $ docker-compose up -d
   ```

3. Once containers are up and running, wait a few mins for Airflow to initialize, and then navigate to the following:
   
   Airflow UI - http://localhost:8082

    - Username: <code>airflow12</code>
   
    - Password: <code>airflow12</code>

   Kafka UI - http://localhost:8084

Kafka (KRaft Mode)
===================

Cluster runs in KRaft mode, with a dedicated controller and 3 brokers.

The controller manages all cluster metadata, replacing the need for ZooKeeper. The brokers store and replicate the topic data.

### Kafka-UI (Optional)

View Kafka-UI in web browser:
```bash
localhost:8084
```
