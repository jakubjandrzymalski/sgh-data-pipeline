import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Dodanie głównego katalogu projektu do ścieżki Pythona
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

default_args = {
    'owner': 'student_sgh',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Funkcje wykonawcze z leniwym importem (Lazy Import)
def run_extraction():
    from src.cloud_s3.s3_extract_cloud import data_extraction_alpha_vantage
    data_extraction_alpha_vantage()

def run_transform_to_silver():
    from src.cloud_s3.s3_transform_cloud import transform_to_silver
    transform_to_silver()

def run_enrich_to_gold():
    from src.cloud_s3.s3_transform_cloud import enrich_to_gold
    enrich_to_gold()

with DAG(
    dag_id='sgh_financial_data_pipeline',
    default_args=default_args,
    description='Automated ETL: Alpha Vantage API -> S3 Raw -> PySpark -> S3 Processed',
    schedule='0 18 * * 1-5',  # Zmieniono schedule_interval na schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['sgh', 'etl', 'pyspark', 's3'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_alpha_vantage_to_s3',
        python_callable=run_extraction
    )

    task_transform_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=run_transform_to_silver
    )

    task_transform_gold = PythonOperator(
        task_id='enrich_to_gold',
        python_callable=run_enrich_to_gold
    )

    task_extract >> task_transform_silver >> task_transform_gold