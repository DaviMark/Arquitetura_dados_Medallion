from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'portfolio_medallion_architecture',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    ingest_bronze = DatabricksSubmitRunOperator(
        task_id='ingest_jira_to_bronze',
        databricks_conn_id='databricks_default',
        spark_python_task={
            'python_file': 'dbfs:/scripts/script_ingestion/jira/ingest_api_to_bronze.py'
        }
    )

    transform_silver = DatabricksSubmitRunOperator(
        task_id='transform_jira_silver',
        databricks_conn_id='databricks_default',
        spark_python_task={
            'python_file': 'dbfs:/scripts/utils/run_sql.py',
            'parameters': ['dbfs:/scripts/script_transform/jira/silver/jira.sql']
        }
    )

    transform_gold = DatabricksSubmitRunOperator(
        task_id='transform_jira_gold',
        databricks_conn_id='databricks_default',
        spark_python_task={
            'python_file': 'dbfs:/scripts/utils/run_sql.py',
            'parameters': ['dbfs:/scripts/script_transform/jira/gold/jira.sql']
        }
    )

    ingest_bronze >> transform_silver >> transform_gold