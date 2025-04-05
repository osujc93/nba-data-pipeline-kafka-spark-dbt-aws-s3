# NBA-Data-Pipeline
Data pipeline for NBA player boxscores using Kafka, Spark, Dbt, AWS S3 and more.

![Diagram](/assets/nbaDiagram.png)

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
