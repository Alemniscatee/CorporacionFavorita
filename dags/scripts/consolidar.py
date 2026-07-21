""" def ejecutar_transformacion():
    print('-> Script modular: Aplicando lógica de negocio...') """
# Tarea 4 del DAG: consolidar

import time
import polars as pl
from pathlib import Path
import importlib.util

def _load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parent / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

carga = _load_module("carga", "cargar_datos.py")
limpieza = _load_module("limpieza", "limpiar_datos.py")


def _preparar_holidays_por_dia(holidays: pl.DataFrame) -> pl.DataFrame:

    h = holidays.with_columns(
        (pl.col("type") == "Holiday").alias("_es_holiday_real")
    ).sort(["date", "_es_holiday_real"], descending=[False, True])

    representativo = h.group_by("date", maintain_order=True).agg(
        pl.col("type").first().alias("tipo_feriado"),
        pl.col("locale").first().alias("locale_feriado"),
        pl.col("locale_name").first().alias("locale_name_feriado"),
        pl.col("description").first().alias("descripcion_feriado"),
        pl.col("transferred").first().alias("transferred_feriado"),
    )
    es_feriado_nacional = (
        h.filter((pl.col("locale") == "National") & (pl.col("transferred") == False))
        .select("date")
        .unique()
        .with_columns(pl.lit(True).alias("es_feriado_nacional"))
    )
    out = representativo.join(es_feriado_nacional, on="date", how="left").with_columns(
        pl.col("es_feriado_nacional").fill_null(False)
    )
    return out


def consolidar(limpios: dict) -> tuple[pl.DataFrame, dict]:
    t0 = time.perf_counter()
    train = limpios["train"]
    stores = limpios["stores"]
    transactions = limpios["transactions"]
    oil = limpios["oil"].select(["date", "dcoilwtico"])
    holidays_dia = _preparar_holidays_por_dia(limpios["holidays"])

    filas_train = train.height

    df = (
        train
        .join(stores, on="store_nbr", how="left")
        .join(transactions, on=["store_nbr", "date"], how="left")
        .join(oil, on="date", how="left")
        .join(holidays_dia, on="date", how="left")
        .with_columns(
            pl.col("es_feriado_nacional").fill_null(False),
        )
    )

    metricas = {
        "filas_train_base": filas_train,
        "filas_consolidado": df.height,
        "columnas_consolidado": df.width,
        "segundos": round(time.perf_counter() - t0, 4),
        "filas_agregadas_por_join": df.height - filas_train,
    }
    return df, metricas


if __name__ == "__main__":
    dfs, _ = carga.cargar_todo()
    limpios, _ = limpieza.ejecutar_limpieza(dfs)
    consolidado, metricas = consolidar(limpios)

    print(f"[consolidar] {metricas['filas_train_base']:,} filas base -> "
          f"{metricas['filas_consolidado']:,} filas consolidadas, "
          f"{metricas['columnas_consolidado']} columnas ({metricas['segundos']}s)")
    print(consolidado.head(3))

    # Ruta relativa: scripts/../eda_output
    out_dir = Path(__file__).resolve().parent.parent / "eda_output"
    out_dir.mkdir(exist_ok=True)
    consolidado.write_parquet(out_dir / "consolidado.parquet")

    import json
    with open(out_dir / "metricas_consolidacion.json", "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False, default=str)
