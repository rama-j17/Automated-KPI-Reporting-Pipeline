from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

with DAG(
    dag_id="gcs_kpi_monthly",
    schedule_interval="0 2 5 * *",   # 02:00 on the 5th of month
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["amex","kpi"],
) as dag:

    run_month = "{{ ds | ds_format('%Y-%m-01','%Y-%m') }}"

    validate = PythonOperator(
        task_id="dq_check",
        python_callable=lambda: subprocess.run(["python","src/dq/great_expect.py"], check=True)
    )

    etl = SparkSubmitOperator(
        task_id="spark_kpi",
        application="src/etl/pipeline.py",
        application_args=[run_month],
        conn_id="spark_default"
    )

    report = PythonOperator(
        task_id="pdf_report",
        python_callable=lambda m: subprocess.run(["python","src/report/report.py","--month",m], check=True),
        op_args=[run_month]
    )

    validate >> etl >> report
