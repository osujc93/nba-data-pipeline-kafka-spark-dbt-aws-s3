"""
DAG definition for NBA Stats workflow.
"""

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.task.trigger_rule import TriggerRule

from dags.dagUtils import BranchDecider
from dags.nbaTasks import NBATasks

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

# LOCAL TIMEZONE
local_tz = pendulum.timezone("America/New_York")

with DAG(
    dag_id="NBA_Stats_WorkFlow_OOP",
    default_args=default_args,
    description=(
        "Workflow: ingestion + daily increments"
    ),
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["etl", "nba"],
) as dag:
    # ---------------------------------------------------
    # 1) Tasks & Branching
    # ---------------------------------------------------
    start = NBATasks.start_task()
    do_initial_chain = NBATasks.do_initial_chain()
    skip_initial_chain = NBATasks.skip_initial_chain()

    first_ingestion_kafka = NBATasks.first_ingestion_kafka_task()
    first_ingestion_spark = NBATasks.first_ingestion_spark_task()
    dbt_one_time = NBATasks.dbt_one_time_task()

    incremental_kafka = NBATasks.incremental_kafka_task()
    incremental_spark = NBATasks.incremental_spark_task()
    dbt_daily = NBATasks.dbt_daily_task()

    spark_xgboost_team_classification = NBATasks.spark_xgboost_team_classification()
    spark_xgboost_player_classification = NBATasks.spark_xgboost_player_classification()

    superset_daily = NBATasks.superset_daily_task()

    end = NBATasks.end_task()

    # Branch decider
    decide_task = BranchPythonOperator(
        task_id="decide_initial_or_incremental",
        python_callable=BranchDecider.decide_initial_or_incremental,
    )

    # ---------------------------------------------------
    # 2) Join operator to unify ingestion branches
    # ---------------------------------------------------
    join_after_ingestion = EmptyOperator(
        task_id="join_after_ingestion",
        trigger_rule=TriggerRule.ONE_SUCCESS,  # proceed if EITHER branch succeeds
    )

    # ---------------------------------------------------
    # 3) DAG Structure
    # ---------------------------------------------------
    # Start -> Decide branch
    start >> decide_task

    # One-time ingestion branch
    decide_task >> do_initial_chain
    (
        do_initial_chain
        >> first_ingestion_kafka
        >> first_ingestion_spark
        >> dbt_one_time
        >> join_after_ingestion
    )

    # Daily incremental ingestion branch
    decide_task >> skip_initial_chain
    (
        skip_initial_chain
        >> incremental_kafka
        >> incremental_spark
        >> dbt_daily
        >> join_after_ingestion
    )

    # After ingestion is done, XGBoost tasks run
    join_after_ingestion >> spark_xgboost_team_classification
    spark_xgboost_team_classification >> spark_xgboost_player_classification

    # Then Superset task
    spark_xgboost_player_classification >> superset_daily

    superset_daily >> end
