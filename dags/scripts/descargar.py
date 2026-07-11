"""def ejecutar_descarga():
    print('-> Script modular: Descargando datos fuentes...')"""

# Tarea 2 del DAG: eda_inicial

import json
import polars as pl
from pathlib import Path

import importlib.util
spec = importlib.util.spec_from_file_location("carga", Path(__file__).parent / "01_carga.py")
carga = importlib.util.module_from_spec(spec)
spec.loader.exec_module(carga)

# Ruta relativa: scripts/../eda_output
OUT_DIR = Path(__file__).resolve().parent.parent / "eda_output"
OUT_DIR.mkdir(exist_ok=True)


def diagnostico_calidad(nombre: str, df: pl.DataFrame) -> dict:
    n_filas = df.height
    nulos_por_col = {}
    for col in df.columns:
        n_nulos = df[col].null_count()
        nulos_por_col[col] = {
            "nulos": int(n_nulos),
            "porcentaje": round(100 * n_nulos / n_filas, 3) if n_filas else 0.0,
        }

    n_duplicados = n_filas - df.unique().height

    fecha_cols = [c for c, t in zip(df.columns, df.dtypes) if t == pl.Date]
    rango_fechas = {}
    for c in fecha_cols:
        rango_fechas[c] = {
            "min": str(df[c].min()),
            "max": str(df[c].max()),
        }

    return {
        "archivo": f"{nombre}.csv",
        "filas": n_filas,
        "columnas": df.width,
        "tipos_de_dato": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
        "nulos_por_columna": nulos_por_col,
        "filas_duplicadas": int(n_duplicados),
        "rango_fechas": rango_fechas,
    }


def ejecutar_eda_inicial(dfs: dict) -> dict:
    reporte = {"eda": "inicial (pre-limpieza)", "archivos": {}}
    for nombre, df in dfs.items():
        reporte["archivos"][nombre] = diagnostico_calidad(nombre, df)
        d = reporte["archivos"][nombre]
        print(f"[eda_inicial] {nombre:<14} filas={d['filas']:>9,} "
              f"duplicados={d['filas_duplicadas']:>5} "
              f"nulos_totales={sum(v['nulos'] for v in d['nulos_por_columna'].values())}")
    return reporte


if __name__ == "__main__":
    dfs, _ = carga.cargar_todo()
    reporte = ejecutar_eda_inicial(dfs)
    out_path = OUT_DIR / "eda_inicial.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)
    print(f"\nReporte guardado en {out_path}")
