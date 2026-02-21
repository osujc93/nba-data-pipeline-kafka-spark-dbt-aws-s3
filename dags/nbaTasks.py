import os
from typing import Any
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from dags.dagUtils import DockerOperatorWithCleanup


class NBATasks:
    """
    A static utility class to build tasks for the DAG.

    Each method returns an Airflow Operator instance representing a step
    (BashOperator, DockerOperatorWithCleanup, etc.).
    """

    @staticmethod
    def start_task() -> EmptyOperator:
        """
        Create an EmptyOperator to mark the start of the DAG.
        """
        return EmptyOperator(task_id="start")

    @staticmethod
    def do_initial_chain() -> EmptyOperator:
        """
        Create an EmptyOperator marking the start of the initial ingestion chain.
        """
        return EmptyOperator(task_id="do_initial_chain")

    @staticmethod
    def skip_initial_chain() -> EmptyOperator:
        """
        Create an EmptyOperator marking a skip of the initial chain.
        """
        return EmptyOperator(task_id="skip_initial_chain")
    
    @staticmethod
    def end_task() -> EmptyOperator:
        """
        Create an EmptyOperator to mark the end of the DAG.
        """
        return EmptyOperator(task_id="end")    

    @staticmethod
    def first_ingestion_kafka_task() -> BashOperator:
        """
        Create a BashOperator to run a Python script that kicks off the initial
        Kafka ingestion for NBA data.
        """
        return BashOperator(
            task_id="first_ingestion_kafka",
            bash_command=(
                "python3 /opt/airflow/kafkaProducers/firstIngestion/"
                "main.py"
            ),
        )

    @staticmethod
    def first_ingestion_spark_task() -> DockerOperatorWithCleanup:
        """
        Submits the first ingestion Spark job.
        """
        return DockerOperatorWithCleanup(
            task_id="first_ingestion_spark",
            image="spark-client:latest",
            container_name="spark-client-first-batch",
            mount_tmp_dir=False,
            api_version="auto",
            auto_remove="success",
            environment={
                "PYSPARK_PYTHON": "/usr/bin/python3",
                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
                "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "AWS_REGION": os.environ.get("AWS_REGION", ""),
            },
            command="""
                spark-submit \
                --master spark://spark-master1:7077,spark-master2:7077,spark-master3:7077 \
                --deploy-mode client \
                --name NBA_Player_Boxscores_Batch \
                --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
                --conf "spark.sql.catalog.spark_catalog.type=hive" \
                --conf "spark.sql.warehouse.dir=s3a://nelodatawarehouse933/warehouse" \
                --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
                --conf "spark.sql.iceberg.target-file-size-bytes=134217728" \
                --py-files /opt/airflow/sparkJobs/firstBatch/firstBatchPackage.zip \
                --verbose \
                /opt/airflow/sparkJobs/firstBatch/main.py
            """,
            docker_url="unix://var/run/docker.sock",
            network_mode="nelo-data-pipeline",
        )

    @staticmethod
    def dbt_one_time_task() -> BashOperator:
        """
        Runs dbt in full-refresh mode for the one-time chain.
        """
        return BashOperator(
            task_id="dbt_one_time",
            bash_command="""
                cd /dbt &&
                dbt deps &&

                dbt run --models player_boxscores_view --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models team_boxscores_view --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models dim_player --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models dim_team --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models dim_season --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models dim_game --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models fact_player_boxscores --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models fact_team_boxscores --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models fact_player_adv_stats --full-refresh --fail-fast --debug --log-format=json &&
                dbt run --models fact_team_adv_stats --full-refresh --fail-fast --debug --log-format=json

                python -c "from airflow.models import Variable; Variable.set('init_ingestion_done','True')"
            """,
        )

    @staticmethod
    def spark_xgboost_team_classification() -> DockerOperatorWithCleanup:
        """
        Submits a Spark job for XGBoost classification on Team data.
        """
        return DockerOperatorWithCleanup(
            task_id="spark_xgboost_team_classification",
            image="spark-client:latest",
            container_name="spark-client-team-classification",
            mount_tmp_dir=False,
            api_version="auto",
            auto_remove="success",
            environment={
                "PYSPARK_PYTHON": "/usr/bin/python3",
                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
                "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "AWS_REGION": os.environ.get("AWS_REGION", ""),
            },
            command="""
                spark-submit \
                --master spark://spark-master1:7077,spark-master2:7077,spark-master3:7077 \
                --deploy-mode client \
                --name NBA_Team_XGBoost_Classification \
                --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
                --conf "spark.sql.warehouse.dir=s3a://nelodatawarehouse933/warehouse" \
                --py-files /opt/airflow/sparkJobs/xgboost4j/teams/classification/teamsClassificationPackage.zip \
                --verbose \
                /opt/airflow/sparkJobs/xgboost4j/teams/classification/main.py
            """,
            docker_url="unix://var/run/docker.sock",
            network_mode="nelo-data-pipeline",
        )

    @staticmethod
    def spark_xgboost_player_classification() -> DockerOperatorWithCleanup:
        """
        Submits a Spark job for XGBoost classification on Player data.
        """
        return DockerOperatorWithCleanup(
            task_id="spark_xgboost_player_classification",
            image="spark-client:latest",
            container_name="spark-client-player-classification",
            mount_tmp_dir=False,
            api_version="auto",
            auto_remove="success",
            environment={
                "PYSPARK_PYTHON": "/usr/bin/python3",
                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
                "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "AWS_REGION": os.environ.get("AWS_REGION", ""),
            },
            command="""
                spark-submit \
                --master spark://spark-master1:7077,spark-master2:7077,spark-master3:7077 \
                --deploy-mode client \
                --name NBA_Player_XGBoost_Classification \
                --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
                --conf "spark.sql.warehouse.dir=s3a://nelodatawarehouse933/warehouse" \
                --py-files /opt/airflow/sparkJobs/xgboost4j/players/classification/playersClassificationPackage.zip \
                --verbose \
                /opt/airflow/sparkJobs/xgboost4j/players/classification/main.py
            """,
            docker_url="unix://var/run/docker.sock",
            network_mode="nelo-data-pipeline",
        )

    @staticmethod
    def incremental_kafka_task() -> BashOperator:
        return BashOperator(
            task_id="incremental_kafka",
            bash_command="python3 /opt/airflow/kafkaProducers/incremental/main.py",
        )

    @staticmethod
    def incremental_spark_task() -> DockerOperatorWithCleanup:
        """
        Submits the incremental ingestion Spark job.
        """
        return DockerOperatorWithCleanup(
            task_id="incremental_spark",
            image="spark-client:latest",
            container_name="spark-client-incremental",
            mount_tmp_dir=False,
            api_version="auto",
            auto_remove="success",
            environment={
                "PYSPARK_PYTHON": "/usr/bin/python3",
                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
                "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "AWS_REGION": os.environ.get("AWS_REGION", ""),
            },
            command="""
                spark-submit \
                --master spark://spark-master1:7077,spark-master2:7077,spark-master3:7077 \
                --deploy-mode client \
                --name NBA_Player_Boxscores_Batch_Incremental \
                --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
                --conf "spark.sql.warehouse.dir=s3a://nelodatawarehouse933/warehouse" \
                --verbose \
                /sparkJobs/incremental/main.py
            """,
            docker_url="unix://var/run/docker.sock",
            network_mode="nelo-data-pipeline",
        )

    @staticmethod
    def dbt_daily_task() -> BashOperator:
        return BashOperator(
            task_id="dbt_daily",
            bash_command="""
                cd /dbt
                dbt deps &&

                dbt run --models player_boxscores_view --fail-fast --debug --log-format=json &&
                dbt run --models team_boxscores_view --fail-fast --debug --log-format=json &&
                dbt run --models dim_player --fail-fast --debug --log-format=json &&
                dbt run --models dim_team --fail-fast --debug --log-format=json &&
                dbt run --models dim_season --fail-fast --debug --log-format=json &&
                dbt run --models dim_game --fail-fast --debug --log-format=json &&
                dbt run --models fact_player_boxscores --fail-fast --debug --log-format=json &&
                dbt run --models fact_team_boxscores --fail-fast --debug --log-format=json &&
                dbt run --models fact_player_adv_stats --fail-fast --debug --log-format=json &&
                dbt run --models fact_team_adv_stats --fail-fast --debug --log-format=json
            """,
        )

    @staticmethod
    def superset_daily_task() -> BashOperator:
        return BashOperator(
            task_id="superset_daily",
            bash_command="python3 /app/superset_nba_setup.py",
        )