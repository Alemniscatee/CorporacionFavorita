# Tarea 1 del DAG (favorita_pipeline): cargar_datos

import csv
import time
import polars as pl
from pathlib import Path

# Rutas relativas al proyecto: scripts/../datasets y scripts/../eda_output

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "datasets"


def reparar_train_doble_csv(path: Path) -> Path:
    
    filas_reparadas = 0
    filas_fixed = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        filas_fixed.append(header)
        for row in reader:
            if len(row) == 1:
                row = next(csv.reader([row[0]]))
                filas_reparadas += 1
            filas_fixed.append(row)

    if filas_reparadas == 0:
        return path

    path_reparado = path.with_name(path.stem + "_reparado.csv")
    with open(path_reparado, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(filas_fixed)

    print(f"[carga_datos] {path.name}: {filas_reparadas:,} filas con doble-codificacion "
          f"CSV reparadas -> {path_reparado.name}")
    return path_reparado


def cargar_train(path: Path = DATA_DIR / "train.csv") -> pl.DataFrame:
    """Ventas diarias por tienda/familia/promocion. Archivo principal."""
    path = reparar_train_doble_csv(path)
    return pl.read_csv(
        path,
        schema_overrides={
            "id": pl.Int64,
            "store_nbr": pl.Int64,
            "family": pl.Utf8,
            "sales": pl.Float64,
            "onpromotion": pl.Int64,
        },
        try_parse_dates=True,
    )


def cargar_stores(path: Path = DATA_DIR / "stores.csv") -> pl.DataFrame:
    """Metadata de las 54 tiendas: ciudad, provincia, tipo y cluster."""
    return pl.read_csv(
        path,
        schema_overrides={
            "store_nbr": pl.Int64,
            "city": pl.Utf8,
            "state": pl.Utf8,
            "type": pl.Utf8,
            "cluster": pl.Int64,
        },
    )


def cargar_transactions(path: Path = DATA_DIR / "transactions.csv") -> pl.DataFrame:
    """Numero de transacciones por tienda y fecha."""
    return pl.read_csv(
        path,
        schema_overrides={"store_nbr": pl.Int64, "transactions": pl.Int64},
        try_parse_dates=True,
    )


def cargar_oil(path: Path = DATA_DIR / "oil.csv") -> pl.DataFrame:
    """Precio diario del petroleo (WTI). Tiene huecos en fines de semana/feriados."""
    return pl.read_csv(
        path,
        schema_overrides={"dcoilwtico": pl.Float64},
        try_parse_dates=True,
    )


def cargar_holidays(path: Path = DATA_DIR / "holidays_events.csv") -> pl.DataFrame:
    """Feriados nacionales, regionales y locales de Ecuador."""
    return pl.read_csv(
        path,
        schema_overrides={
            "type": pl.Utf8,
            "locale": pl.Utf8,
            "locale_name": pl.Utf8,
            "description": pl.Utf8,
            "transferred": pl.Boolean,
        },
        try_parse_dates=True,
    )


def cargar_todo() -> dict:

    loaders = {
        "train": cargar_train,
        "stores": cargar_stores,
        "transactions": cargar_transactions,
        "oil": cargar_oil,
        "holidays": cargar_holidays,
    }
    dfs, metricas = {}, {}
    for nombre, fn in loaders.items():
        t0 = time.perf_counter()
        try:
            df = fn()
        except Exception as e:
            raise RuntimeError(f"Fallo al cargar {nombre}.csv: {e}") from e
        dt = time.perf_counter() - t0
        dfs[nombre] = df
        metricas[nombre] = {"filas": df.height, "columnas": df.width, "segundos": round(dt, 4)}
        print(f"[carga_datos] {nombre:<14} -> {df.height:>9,} filas x {df.width} cols "
              f"({dt:.3f}s)")
    return dfs, metricas


if __name__ == "__main__":
    dfs, metricas = cargar_todo()
    import json
    out_dir = PROJECT_DIR / "eda_output"
    out_dir.mkdir(exist_ok=True)
    with open(out_dir / "metricas_carga.json", "w") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)
