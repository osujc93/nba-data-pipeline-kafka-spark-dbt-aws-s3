# NBA-Data-Pipeline
Data pipeline for NBA player boxscores using Kafka, Spark, Dbt, AWS S3 and more.

![Diagram](/assets/nbaDiagram2.png)

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
