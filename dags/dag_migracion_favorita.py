from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# IMPORTACIÓN MODULAR DESDE LA CARPETA SCRIPTS
from scripts.descargar import ejecutar_descarga
from scripts.limpiar import ejecutar_limpieza
from scripts.transformar import ejecutar_transformacion
from scripts.validar import ejecutar_validacion
from scripts.cargar import ejecutar_carga
from scripts.reporte import ejecutar_reporte

with DAG(
    dag_id="pipeline_migracion_la_favorita",
    description="Pipeline ETL modular y secuencial utilizando Polars",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["la_favorita", "polars", "produccion"]
) as dag:

    # Enlazamos cada tarea a la función importada de cada archivo .py
    t1 = PythonOperator(task_id="descargar_datos", python_callable=ejecutar_descarga)
    t2 = PythonOperator(task_id="limpiar_datos", python_callable=ejecutar_limpieza)
    t3 = PythonOperator(task_id="transformar_datos", python_callable=ejecutar_transformacion)
    t4 = PythonOperator(task_id="validar_calidad", python_callable=ejecutar_validacion)
    t5 = PythonOperator(task_id="cargar_postgres", python_callable=ejecutar_carga)
    t6 = PythonOperator(task_id="generar_reporte", python_callable=ejecutar_reporte)

    # El flujo secuencial riel por riel
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
