""" def ejecutar_reporte():
    print('-> Script modular: Generando KPIs y reportes...') """
# Tarea 5 del DAG: eda_profundo

import time
import json
import polars as pl
from pathlib import Path
import importlib.util

def _load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parent / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

carga = _load_module("carga", "01_carga.py")
limpieza = _load_module("limpieza", "03_limpieza.py")
consolidacion = _load_module("consolidacion", "04_consolidar.py")

# Ruta relativa: scripts/../eda_output
OUT_DIR = Path(__file__).resolve().parent.parent / "eda_output"
OUT_DIR.mkdir(exist_ok=True)


# ---------- A. Ventas generales -------------------------------------------

def ventas_por_familia(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("family")
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("sales").mean().alias("ventas_promedio"))
        .sort("ventas_totales", descending=True)
        .with_columns(
            (pl.col("ventas_totales") / pl.col("ventas_totales").sum() * 100)
            .round(2).alias("pct_del_total")
        )
    )


def ranking_tiendas(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("store_nbr")
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("city").first(), pl.col("state").first(), pl.col("type").first())
        .sort("ventas_totales", descending=True)
        .with_columns(pl.int_range(1, pl.len() + 1).alias("ranking"))
    )


def ventas_por_ciudad_provincia(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by(["city", "state"])
        .agg(pl.col("sales").mean().alias("ventas_promedio"),
             pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )


def evolucion_temporal(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"),
                         pl.col("date").dt.year().alias("anio"))
        .group_by(["anio", "anio_mes"])
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("anio_mes")
    )


# ---------- B. Estacionalidad y feriados -----------------------------------

def impacto_feriados(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("es_feriado_nacional")
        .agg(pl.col("sales").mean().alias("ventas_promedio_dia"),
             pl.col("sales").sum().alias("ventas_totales"),
             pl.len().alias("n_registros"))
        .with_columns(
            pl.when(pl.col("es_feriado_nacional")).then(pl.lit("feriado_nacional"))
            .otherwise(pl.lit("dia_normal")).alias("tipo_dia")
        )
    )


def ventas_entorno_feriados(df: pl.DataFrame) -> pl.DataFrame:
    fechas_feriado = (
        df.filter(pl.col("es_feriado_nacional")).select("date").unique()["date"].to_list()
    )
    offsets = list(range(-3, 4))
    ventana = pl.DataFrame({"date_feriado": [f for f in fechas_feriado for _ in offsets],
                             "offset": offsets * len(fechas_feriado)})
    ventana = ventana.with_columns(
        (pl.col("date_feriado") + pl.duration(days=pl.col("offset"))).alias("date")
    )
    ventas_dia_familia = df.group_by(["date", "family"]).agg(pl.col("sales").sum().alias("sales"))
    unido = ventana.join(ventas_dia_familia, on="date", how="inner")
    return (
        unido.group_by(["offset", "family"])
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
        .sort(["family", "offset"])
    )


def sensibilidad_familia_feriados(df: pl.DataFrame) -> pl.DataFrame:
    por_dia = df.group_by(["family", "es_feriado_nacional"]).agg(
        pl.col("sales").mean().alias("ventas_promedio")
    )
    pivot = por_dia.pivot(values="ventas_promedio", index="family", on="es_feriado_nacional")
    pivot = pivot.rename({"true": "venta_prom_feriado", "false": "venta_prom_normal"}) \
        if "true" in pivot.columns else pivot
    cols = pivot.columns
    col_feriado = "true" if "true" in cols else ("venta_prom_feriado" if "venta_prom_feriado" in cols else None)
    col_normal = "false" if "false" in cols else ("venta_prom_normal" if "venta_prom_normal" in cols else None)
    if col_feriado and col_normal:
        pivot = pivot.with_columns(
            ((pl.col(col_feriado) - pl.col(col_normal)) / pl.col(col_normal) * 100)
            .round(2).alias("variacion_pct_feriado_vs_normal")
        ).sort("variacion_pct_feriado_vs_normal", descending=True)
    return pivot


# ---------- C. Promociones ---------------------------------------------

def impacto_promociones(df: pl.DataFrame) -> pl.DataFrame:
    con_sin = df.with_columns((pl.col("onpromotion") > 0).alias("en_promocion"))
    resumen = (
        con_sin.group_by(["family", "en_promocion"])
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
    )
    pivot = resumen.pivot(values="ventas_promedio", index="family", on="en_promocion")
    col_true = "true" if "true" in pivot.columns else True
    col_false = "false" if "false" in pivot.columns else False
    if col_true in pivot.columns and col_false in pivot.columns:
        pivot = pivot.rename({col_true: "venta_prom_con_promo", col_false: "venta_prom_sin_promo"})
        pivot = pivot.with_columns(
            ((pl.col("venta_prom_con_promo") - pl.col("venta_prom_sin_promo"))
             / pl.col("venta_prom_sin_promo") * 100).round(2).alias("uplift_pct_promocion")
        ).sort("uplift_pct_promocion", descending=True)
    return pivot


# ---------- D. Petroleo y economia --------------------------------------

def correlacion_petroleo_ventas(df: pl.DataFrame) -> pl.DataFrame:
    mensual = (
        df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"))
        .group_by("anio_mes")
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio"))
        .sort("anio_mes")
    )
    corr = mensual.select(pl.corr("ventas_totales", "precio_petroleo_promedio")).item()
    mensual = mensual.with_columns(pl.lit(round(corr, 4)).alias("correlacion_pearson_global"))
    return mensual


def lag_petroleo_ventas(df: pl.DataFrame, max_lag_meses: int = 6) -> pl.DataFrame:
    periodo = df.filter(
        (pl.col("date") >= pl.date(2015, 1, 1)) & (pl.col("date") <= pl.date(2016, 12, 31))
    )
    if periodo.height == 0:
        # Ver nota en sensibilidad_ciudad_petroleo: el train.csv provisto
        
        return pl.DataFrame(schema={"lag_meses": pl.Int64, "correlacion": pl.Float64})
    mensual = (
        periodo.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"))
        .group_by("anio_mes")
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("dcoilwtico").mean().alias("precio_petroleo"))
        .sort("anio_mes")
    )
    filas = []
    for lag in range(0, max_lag_meses + 1):
        petroleo_lag = mensual.select("anio_mes", pl.col("precio_petroleo").shift(lag).alias("petroleo_lag"))
        combinado = mensual.select("anio_mes", "ventas_totales").join(petroleo_lag, on="anio_mes")
        corr = combinado.select(pl.corr("ventas_totales", "petroleo_lag")).item()
        filas.append({"lag_meses": lag, "correlacion": None if corr is None else round(corr, 4)})
    return pl.DataFrame(filas)


def sensibilidad_ciudad_petroleo(df: pl.DataFrame) -> pl.DataFrame:
    periodo = df.filter((pl.col("date") >= pl.date(2015, 1, 1)) & (pl.col("date") <= pl.date(2016, 12, 31)))
    esquema_vacio = {"city": pl.Utf8, "correlacion_petroleo_ventas": pl.Float64}
    if periodo.height == 0:
        
        return pl.DataFrame(schema=esquema_vacio)

    mensual_ciudad = (
        periodo.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"))
        .group_by(["city", "anio_mes"])
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("dcoilwtico").mean().alias("precio_petroleo"))
    )
    filas = []
    for ciudad in mensual_ciudad["city"].unique().to_list():
        sub = mensual_ciudad.filter(pl.col("city") == ciudad).sort("anio_mes")
        if sub.height < 3:
            continue
        corr = sub.select(pl.corr("ventas_totales", "precio_petroleo")).item()
        filas.append({"city": ciudad, "correlacion_petroleo_ventas": None if corr is None else round(corr, 4)})
    if not filas:
        return pl.DataFrame(schema=esquema_vacio)
    return pl.DataFrame(filas).sort("correlacion_petroleo_ventas")


# ---------- E. Transacciones ---------------------------------------------

def transacciones_vs_ventas(df: pl.DataFrame) -> pl.DataFrame:
    por_tienda = (
        df.group_by("store_nbr")
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("transactions").sum().alias("transacciones_totales"))
    )
    corr = por_tienda.select(pl.corr("ventas_totales", "transacciones_totales")).item()
    return por_tienda.with_columns(pl.lit(round(corr, 4) if corr is not None else None)
                                    .alias("correlacion_pearson_global")).sort("ventas_totales", descending=True)


def ticket_promedio_tiendas(df: pl.DataFrame) -> pl.DataFrame:
    por_tienda = (
        df.group_by("store_nbr")
        .agg(pl.col("sales").sum().alias("ventas_totales"),
             pl.col("transactions").sum().alias("transacciones_totales"))
        .filter(pl.col("transacciones_totales") > 0)
        .with_columns((pl.col("ventas_totales") / pl.col("transacciones_totales"))
                      .alias("ticket_promedio"))
        .sort("ticket_promedio", descending=True)
    )
    return por_tienda


# ---------- Orquestacion ---------------------------------------------------

def ejecutar_eda_profundo(df: pl.DataFrame) -> dict:
    bloques = {
        "ventas_por_familia": ventas_por_familia,
        "ranking_tiendas": ranking_tiendas,
        "ventas_por_ciudad_provincia": ventas_por_ciudad_provincia,
        "evolucion_temporal": evolucion_temporal,
        "impacto_feriados": impacto_feriados,
        "ventas_entorno_feriados": ventas_entorno_feriados,
        "sensibilidad_familia_feriados": sensibilidad_familia_feriados,
        "impacto_promociones": impacto_promociones,
        "correlacion_petroleo_ventas": correlacion_petroleo_ventas,
        "lag_petroleo_ventas": lag_petroleo_ventas,
        "sensibilidad_ciudad_petroleo": sensibilidad_ciudad_petroleo,
        "transacciones_vs_ventas": transacciones_vs_ventas,
        "ticket_promedio_tiendas": ticket_promedio_tiendas,
    }
    resultados, metricas = {}, {}
    for nombre, fn in bloques.items():
        t0 = time.perf_counter()
        resultado = fn(df)
        dt = round(time.perf_counter() - t0, 4)
        resultados[nombre] = resultado
        metricas[nombre] = {"filas": resultado.height, "segundos": dt}
        resultado.write_parquet(OUT_DIR / f"eda_{nombre}.parquet")
        print(f"[eda_profundo] {nombre:<32} -> {resultado.height:>6} filas ({dt}s) "
              f"guardado en eda_output/eda_{nombre}.parquet")
    return resultados, metricas


if __name__ == "__main__":
    dfs, _ = carga.cargar_todo()
    limpios, _ = limpieza.ejecutar_limpieza(dfs)
    consolidado, _ = consolidacion.consolidar(limpios)
    resultados, metricas = ejecutar_eda_profundo(consolidado)

    with open(OUT_DIR / "metricas_eda_profundo.json", "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)

    print("\n--- Vista rapida de resultados clave ---")
    print("\nTop 5 familias por ventas:")
    print(resultados["ventas_por_familia"].head(5))
    print("\nTop 5 tiendas por ventas:")
    print(resultados["ranking_tiendas"].head(5))
    print("\nImpacto feriados:")
    print(resultados["impacto_feriados"])
    print("\nCorrelacion petroleo-ventas (mensual):")
    print(resultados["correlacion_petroleo_ventas"].select(
        "anio_mes", "ventas_totales", "precio_petroleo_promedio", "correlacion_pearson_global"
    ).head(3))
