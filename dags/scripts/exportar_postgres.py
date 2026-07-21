import os
import time
import json
import polars as pl
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER", "azureuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Analisisdedatos2026")
DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "proyecto_favorita")

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_DIR/"eda_output"

engine = create_engine(
	f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

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
if __name__ == "__main__":
	print("Iniciando Exportacion")
	metricas = ejecutar_exportacion()
	with open(OUT_DIR/"metricas_exportacion.json", "w", encoding = "utf-8") as f:
		json.dump(metricas, f, indent=2, ensure_ascii=False)
	print("Se ha finalizado con la exportacion")
