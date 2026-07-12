from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Ruta absoluta dentro del contenedor de Airflow hacia tus scripts
SCRIPTS_DIR = "/opt/airflow/dags/scripts"

default_args = {
    'owner': 'user', 
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'favorita_pipeline',
    default_args=default_args,
    description='Pipeline ETL y EDA para Corporación Favorita usando Polars',
    schedule_interval=None, 
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=['etl', 'polars', 'postgres'],
) as dag:

    # Tarea 1 - Cargar datos
    t1_cargar_datos = BashOperator(
        task_id='cargar_datos',
        bash_command=f'python {SCRIPTS_DIR}/cargar_datos.py'
    )

    # Tarea 2 - EDA Inicial
    t2_eda_inicial = BashOperator(
        task_id='eda_inicial',
        bash_command=f'python {SCRIPTS_DIR}/eda_inicial.py'
    )

    # Tarea 3 - Limpiar datos
    t3_limpiar_datos = BashOperator(
        task_id='limpiar_datos',
        bash_command=f'python {SCRIPTS_DIR}/limpiar_datos.py'
    )

    # Tarea 4 - Consolidar
    t4_consolidar = BashOperator(
        task_id='consolidar',
        bash_command=f'python {SCRIPTS_DIR}/consolidar.py'
    )

    # Tarea 5 - EDA Profundo
    t5_eda_profundo = BashOperator(
        task_id='eda_profundo',
        bash_command=f'python {SCRIPTS_DIR}/eda_profundo.py'
    )

    # Tarea 6 - Exportar a PostgreSQL
    t6_exportar_postgres = BashOperator(
        task_id='exportar_postgres',
        bash_command=f'python {SCRIPTS_DIR}/exportar_postgres.py'
    )

    # Definir la secuencia estricta del pipeline
    t1_cargar_datos >> t2_eda_inicial >> t3_limpiar_datos >> t4_consolidar >> t5_eda_profundo >> t6_exportar_postgres
