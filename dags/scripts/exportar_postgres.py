import os
import time
import json
import polars as pl
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
from cargar_datos import cargar_todos
from limpiar_datos import limpiar_todos
from consolidar import consolidar_datos

# Cargar variables de entorno
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
    Exporta un DataFrame de Polars directamente a PostgreSQL.
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

def exportar_parquet(path: Path, tabla: str) -> dict:
	t0 = time.perf_counter()
	df = pl.read_parquet(path)
	chunk_size = 100_000
	total_filas = df.height
	for i in range(0, total_filas, chunk_size):
		chunk = df.slice(i, chunk_size)
		chunk.write_database(
			table_name=tabla,
			connection=engine,
			if_table_exists="replace" if i == 0 else "append"
		)
	dt = round(time.perf_counter() - t0, 4)
	print(f"[exportar_postgres] {tabla:<40} -> {df.height:>8,} filas, {df.width} cols ({dt}s)")
	return {"tabla": tabla, "filas":df.height, "columnas":df.width, "segundos":dt}

def ejecutar_exportacion() -> dict:
	metricas = {}
	
	metricas["ventas_consolidado"] = exportar_parquet(
	OUT_DIR/"consolidado.parquet", "ventas_consolidado"
	)
	for parquet_path in sorted(OUT_DIR.glob("eda_*.parquet")):
		nombre_tabla = parquet_path.stem
		metricas[nombre_tabla] = exportar_parquet(parquet_path, nombre_tabla)
	return metricas
def ejecutar_exportacion():
    """
    Ejecuta la exportación de todos los DataFrames generados por el EDA profundo.
    """
    metricas = {}

    # 1. Cargar y consolidar los datos (desde cero)
    print("[exportar] Cargando datos...")
    train, stores, transactions, oil, holidays = cargar_todos()

    print("[exportar] Limpiando datos...")
    train_c, stores_c, transactions_c, oil_c, holidays_c = limpiar_todos(train, stores, transactions, oil, holidays)

    print("[exportar] Consolidando datos...")
    df_consolidado = consolidar_datos(train_c, stores_c, transactions_c, oil_c, holidays_c)

    # 2. Exportar tabla principal
    print(f"[exportar] Exportando ventas_consolidado ({df_consolidado.height} registros)...")
    metricas["ventas_consolidado"] = exportar_dataframe(df_consolidado, "ventas_consolidado")

    # 3. Exportar tablas de EDA (si existen)
    parquet_files = list(OUT_DIR.glob("*.parquet"))
    if parquet_files:
        for pq_file in parquet_files:
            tabla = pq_file.stem
            print(f"[exportar] Exportando {tabla} desde {pq_file.name}...")
            try:
                df_eda = pl.read_parquet(pq_file)
                metricas[tabla] = exportar_dataframe(df_eda, tabla)
            except Exception as e:
                print(f"[exportar] Error al leer {pq_file.name}: {e}")
    else:
        print("[exportar] No se encontraron archivos Parquet de EDA. Solo se exporto ventas_consolidado.")

    # 4. Guardar métricas
    with open(OUT_DIR / "metricas_exportacion.json", "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    ejecutar_exportacion()
