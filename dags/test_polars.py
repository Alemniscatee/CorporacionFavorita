from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import polars as pl

def prueba_polars():
    df = pl.DataFrame({"columna": [1, 2, 3]})
    print(f"Polars funcionando. Versión: {pl.__version__}")

with DAG(
    dag_id="prueba_instalacion_polars",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    tarea_test = PythonOperator(
        task_id="test_libreria",
        python_callable=prueba_polars
    )
