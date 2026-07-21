import os
import time
import json
import polars as pl
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Importar funciones correctas de los scripts
from cargar_datos import cargar_todo
from limpiar_datos import ejecutar_limpieza
from consolidar import consolidar

# Cargar variables de entorno desde la ruta absoluta dentro del contenedor
load_dotenv('/opt/airflow/dags/.env')

DB_USER = os.getenv("DB_USER", "azureuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Analisisdedatos2026")
DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "proyecto_favorita")

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_DIR / "eda_output"
OUT_DIR.mkdir(exist_ok=True)

def exportar_dataframe(df, tabla):
    """
    Exporta un DataFrame de Polars a PostgreSQL usando SQLAlchemy.
    """
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    t0 = time.perf_counter()
    df.write_database(tabla, engine, if_table_exists='replace')
    elapsed = time.perf_counter() - t0
    return {
        "tabla": tabla,
        "filas": df.height,
        "tiempo_seg": round(elapsed, 2),
        "columnas": df.width
    }

def ejecutar_exportacion():
    """
    Ejecuta la exportación completa: carga, limpieza, consolidación y exportación.
    """
    metricas = {}

    print("[exportar] Cargando datos...")
    dfs = cargar_todo()  # dict con claves: train, stores, transactions, oil, holidays

    print("[exportar] Limpiando datos...")
    limpios, _ = ejecutar_limpieza(dfs)  # limpios es un dict con los DataFrames limpios

    print("[exportar] Consolidando datos...")
    df_consolidado, _ = consolidar(limpios)  # df_consolidado es un pl.DataFrame

    # Exportar tabla principal
    print(f"[exportar] Exportando ventas_consolidado ({df_consolidado.height} registros)...")
    metricas["ventas_consolidado"] = exportar_dataframe(df_consolidado, "ventas_consolidado")

    # Exportar tablas EDA desde los archivos Parquet generados por eda_profundo.py
    parquet_files = list(OUT_DIR.glob("eda_*.parquet"))
    if parquet_files:
        print(f"[exportar] Se encontraron {len(parquet_files)} archivos Parquet de EDA.")
        for pq_file in parquet_files:
            tabla = pq_file.stem  # nombre sin extensión
            print(f"[exportar] Exportando {tabla} desde {pq_file.name}...")
            try:
                df_eda = pl.read_parquet(pq_file)
                metricas[tabla] = exportar_dataframe(df_eda, tabla)
            except Exception as e:
                print(f"[exportar] Error al leer {pq_file.name}: {e}. Se omite.")
    else:
        print("[exportar] No se encontraron archivos Parquet de EDA en la carpeta eda_output.")

    # Guardar métricas
    with open(OUT_DIR / "metricas_exportacion.json", "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)

    print("[exportar] Exportacion completada")
    print(f"[exportar] Resumen: {len(metricas)} tablas exportadas")
    return metricas

if __name__ == "__main__":
    ejecutar_exportacion()
