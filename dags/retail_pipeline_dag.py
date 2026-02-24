from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="retail_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual for now; later we’ll make it daily
    catchup=False,
    tags=["retail", "etl", "dbt"],
) as dag:

    run_etl = BashOperator(
        task_id="run_python_etl",
        bash_command="python /opt/airflow/scripts/etl_orders.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/retail_dbt && dbt run",
    )

    forecast_revenue = BashOperator(
    task_id="forecast_revenue",
    bash_command="python /opt/airflow/scripts/forecast_revenue.py",
)

    dbt_run >> forecast_revenue

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/retail_dbt && dbt test",
    )

    run_etl >> dbt_run >> dbt_test