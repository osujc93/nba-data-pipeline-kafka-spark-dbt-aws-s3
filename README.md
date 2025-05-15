# NBA-Data-Pipeline
Data pipeline for NBA player boxscores using Kafka, Spark, Dbt, AWS S3 and more.

![Diagram](/assets/nbaPipelineDiagram.png)

Contents
=================

<!--ts-->
   * [Summary](#summary)
   * [Deployment](#deployment)
   * [Kafka (KRaft Mode)](#kafka-kraft-mode)
      * [Kafka-UI (Optional)](#kafka-ui-optional)
   * [Spark (Standalone)](#spark-standalone)
      * [Zookeeper HA](#zookeeper-ha)
   * [Dbt](#dbt)
      * [Data Modeling](#data-modeling)
   * [AWS-S3](#aws-s3)
   * [Trino](#trino)
      * [Hive-Metastore](#hive-metastore)
   * [Superset](#superset)
   * [Spark-MLib](#spark-mlib)
      * [MLflow](#mlflow)
   * [Airflow](#airflow)
   * [Postgres](#postgres)
      * [pgAdmin (Optional)](#pgadmin-optional)
<!--te-->

Summary
============

### Extract

- Kafka to fetch player boxscores from NBA stats API and send to Kafka topic.

- Took data with initial batch ingestion of boxscores from 1946 to current date. and then daily from the last ingestion date.

- To not fire off requests and cause 443 or 503 HTTP errors, used a token-bucket rate limiter to space out requests, one every seven seconds. 

- And set ThreadPoolExecutor with max_workers=1 so each API request finishes fully before the next one starts. This helped with not flooding the API and prevent timeouts. 

- After the initial batch finishes, it inserts a last-ingestion date into Postgres, so that subsequent incremental runs know exactly where to resume from.

- The incremental runs look up the last-ingested date in Postgres, fetches only the new data from that date forward, sends it to Kafka, then updates the last-ingested date for the next run.

### Load

- Spark job to process data from topic to Iceberg tables stored in my S3 bucket.

- Read from Kafka’s earliest offset to capture all boxscores. 

- The maximum consumed offset for each partition is extracted, incremented by one, stored in a Postgres table as JSON. And then used as the starting offset in subsequent runs so the pipeline resumes exactly where it left off without duplicating any records.

### Transform 

- Dbt models to transform the original <code>nba_player_boxscores</code> table.

- Consolidate individual player-level stats into a single team-level row per game.

- Apply SCD Type 2 merges in Iceberg tables to track historical changes for players and teams.

- Aggregate boxscores into advanced stats based on those defined in NBA's stats glossary.

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

Deployment
============

Ran all apps containerized with docker compose

32 gb of memory for VM/computer 

```bash
$ docker-compose build airflow-base spark-base

$ docker-compose up -d
```

Kafka (KRaft Mode)
===================

Cluster runs in KRaft mode, with a dedicated controller and 3 brokers.

The controller manages all cluster metadata, replacing the need for ZooKeeper. The brokers store and replicate the topic data.

### Kafka-UI (Optional)

View Kafka-UI in web browser:
```bash
localhost:8084
```
