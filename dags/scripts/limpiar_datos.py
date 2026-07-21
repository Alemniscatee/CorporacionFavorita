"""def ejecutar_limpieza():
    print('-> Script modular: Limpiando nulos y duplicados...')"""

# Tarea 3 del DAG: limpiar_datos

import time
import polars as pl
from pathlib import Path
import importlib.util

spec = importlib.util.spec_from_file_location("carga", Path(__file__).parent / "cargar_datos.py")
carga = importlib.util.module_from_spec(spec)
spec.loader.exec_module(carga)


def _moda(df: pl.DataFrame, col: str):
    serie = df[col].drop_nulls()
    if serie.len() == 0:
        return None
    return serie.mode()[0]


def _mediana(df: pl.DataFrame, col: str):
    return df[col].median()


def limpiar_generico(nombre: str, df: pl.DataFrame, col_fecha: str | None = "date") -> tuple[pl.DataFrame, dict]:
    """Limpieza estandar aplicable a stores, transactions, holidays, train."""
    filas_antes = df.height
    nulos_antes = {c: df[c].null_count() for c in df.columns}

    # 1) duplicados
    df = df.unique()
    duplicados_eliminados = filas_antes - df.height

    # 2) tipos de datos: fecha a pl.Date, forzar tipos numericos esperados
    if col_fecha and col_fecha in df.columns and df[col_fecha].dtype != pl.Date:
        df = df.with_columns(pl.col(col_fecha).cast(pl.Date))

    # 3) imputacion de nulos: numericas -> mediana, categoricas -> moda
    imputaciones = {}
    for c in df.columns:
        if df[c].null_count() == 0:
            continue
        dtype = df[c].dtype
        if dtype in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64):
            valor = _mediana(df, c)
            df = df.with_columns(pl.col(c).fill_null(valor))
            imputaciones[c] = {"estrategia": "mediana", "valor": valor}
        elif dtype == pl.Boolean:
            valor = _moda(df, c)
            df = df.with_columns(pl.col(c).fill_null(valor))
            imputaciones[c] = {"estrategia": "moda", "valor": valor}
        else:  # texto / categorica
            valor = _moda(df, c)
            df = df.with_columns(pl.col(c).fill_null(valor))
            imputaciones[c] = {"estrategia": "moda", "valor": valor}

    metricas = {
        "filas_antes": filas_antes,
        "filas_despues": df.height,
        "duplicados_eliminados": duplicados_eliminados,
        "nulos_antes": {c: int(v) for c, v in nulos_antes.items() if v > 0},
        "imputaciones": imputaciones,
    }
    return df, metricas


def limpiar_oil(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """oil.csv: interpolacion lineal temporal para dcoilwtico."""
    filas_antes = df.height
    nulos_antes = int(df["dcoilwtico"].null_count())

    df = df.unique()
    duplicados_eliminados = filas_antes - df.height

    if df["date"].dtype != pl.Date:
        df = df.with_columns(pl.col("date").cast(pl.Date))

    df = df.sort("date")
    df = df.with_columns(pl.col("dcoilwtico").interpolate())
    # bordes (si el primer/ultimo valor es nulo, la interpolacion lineal no
    # los cubre) se rellenan con el valor valido mas cercano.
    df = df.with_columns(
        pl.col("dcoilwtico").fill_null(strategy="forward").fill_null(strategy="backward")
    )

    metricas = {
        "filas_antes": filas_antes,
        "filas_despues": df.height,
        "duplicados_eliminados": duplicados_eliminados,
        "nulos_antes": {"dcoilwtico": nulos_antes},
        "imputaciones": {"dcoilwtico": {"estrategia": "interpolacion_lineal + ffill/bfill en bordes"}},
    }
    return df, metricas


def ejecutar_limpieza(dfs: dict) -> tuple[dict, dict]:
    limpios, metricas_totales = {}, {}
    for nombre, df in dfs.items():
        t0 = time.perf_counter()
        if nombre == "oil":
            limpio, m = limpiar_oil(df)
        elif nombre == "stores":
            limpio, m = limpiar_generico(nombre, df, col_fecha=None)
        else:
            limpio, m = limpiar_generico(nombre, df, col_fecha="date")
        m["segundos"] = round(time.perf_counter() - t0, 4)
        limpios[nombre] = limpio
        metricas_totales[nombre] = m
        print(f"[limpiar_datos] {nombre:<14} {m['filas_antes']:>9,} -> {m['filas_despues']:>9,} filas "
              f"(dup. eliminados={m['duplicados_eliminados']}, "
              f"cols imputadas={len(m['imputaciones'])})")
    return limpios, metricas_totales


if __name__ == "__main__":
    dfs, _ = carga.cargar_todo()
    limpios, metricas = ejecutar_limpieza(dfs)

    import json
    # Ruta relativa: scripts/../eda_output
    out_dir = Path(__file__).resolve().parent.parent / "eda_output"
    out_dir.mkdir(exist_ok=True)
    with open(out_dir / "metricas_limpieza.json", "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False, default=str)

    # verificacion rapida: ya no deberia haber nulos criticos
    assert limpios["oil"]["dcoilwtico"].null_count() == 0
    assert limpios["train"]["sales"].null_count() == 0
    print("\nVerificacion OK: sin nulos remanentes en columnas criticas.")
