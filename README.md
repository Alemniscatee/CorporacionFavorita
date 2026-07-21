#  Pipeline ETL Automatizado  (La Favorita)

Este proyecto consiste en el despliegue de una infraestructura en la nube diseñada para procesar de forma masiva, optimizada y secuencial los datos transaccionales de la empresa o cadena "La Favorita" . 

La arquitectura utiliza un enfoque estrictamente **modular** (es decir que los sripts se pueden modificar individualmente sin afectar a la linea de trabajo principal), donde **Airflow** actúa como el orquestador central ordenandno y ejecutanto los scripts o proceso en su orden correspondiente, usando la lógica de procesamiento de datos a la librería **Polars** por su alta eficiencia en memoria, y almacenando los resultados analíticos finales en **PostgreSQL**.

---

## Arquitectura 

El ecosistema tecnológico se compone de los siguientes pilares:

*   **Servidor en la Nube:** Máquina Virtual (VM) hospedada en **Azure** bajo el sistema operativo Ubuntu Server.
*   **Contenedores (Docker Compose):** Aislamiento completo de los servicios esenciales de Apache Airflow y la base de datos PostgreSQL.
*   **Motor de Procesamiento:** Scripts independientes en Python optimizados mediante **Polars**.
*   **Almacenamiento:** Instancia relacional de PostgreSQL para persistir los datos limpios y transformados listos para analítica.
## Diagrama de arquitectura de la solución
# 2. Descripción de los archivos del dataset y su rol en el pipeline

Los archivos residen en `dags/datasets/` y no se suben al repositorio.

| Archivo | Registros | Columnas | Rol en el pipeline |
|---------|-----------|----------|-------------------|
| `train.csv` | 3,000,888 | 6 | Fuente principal de ventas diarias por tienda, familia y promoción. |
| `stores.csv` | 54 | 5 | Metadata de tiendas: ciudad, provincia, tipo y clúster. Se usa para enriquecer datos geográficos. |
| `transactions.csv` | 83,488 | 3 | Número de transacciones por tienda y fecha. Permite análisis de ticket promedio. |
| `oil.csv` | 1,218 | 2 | Precio diario del petróleo (dcoilwtico). Contiene nulos en fines de semana (43 valores, 3.53%). |
| `holidays_events.csv` | 350 | 6 | Feriados nacionales, regionales y locales con tipo y bandera de transferencia. |

**Nota:** `test.csv` no se utiliza (corresponde a predicción de Kaggle).



# 3. Diagrama de arquitectura de la solución

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                          AZURE VM (Ubuntu 22.04)                         │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                         DOCKER COMPOSE                             │  │
│  │                                                                    │  │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐   │  │
│  │  │   Airflow     │  │   Airflow     │  │     PostgreSQL        │   │  │
│  │  │  Scheduler    │  │  Webserver    │  │    (proyecto_         │   │  │
│  │  │               │  │   Puerto      │  │     favorita)         │   │  │
│  │  │               │  │    8080       │  │                       │   │  │
│  │  └───────┬───────┘  └───────────────┘  └───────────┬───────────┘   │  │
│  │          │                                         │               │  │
│  │  ┌───────▼─────────────────────────────────────────▼────────────┐  │  │
│  │  │                 SCRIPTS (Python + Polars)                    │  │  │
│  │  │                                                              │  │  │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐    │  │  │
│  │  │  │  Carga   │→ │ Limpieza │→ │Consolida-│→ │EDA Profundo│    │  │  │
│  │  │  └──────────┘  └──────────┘  └──────────┘  └────────────┘    │  │  │
│  │  │                                                              │  │  │
│  │  │  ┌──────────────────────────────────────────────────────┐    │  │  │
│  │  │  │  Exportación a PostgreSQL (14 tablas)                │    │  │  │
│  │  │  └──────────────────────────────────────────────────────┘    │  │  │
│  │  └──────────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │               DATOS LOCALES (dags/datasets/)                       │  │
│  │ train.csv | stores.csv | transactions.csv | oil.csv | holidays     │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (DirectQuery)

┌────────────────────────────────────────────────────────────────────────────┐
│                       POWER BI DESKTOP                                     │
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                         DASHBOARD                                  │    │
│  │                                                                    │    │
│  │ • Ventas por familia de producto                                   │    │
│  │ • Ranking de tiendas                                               │    │
│  │ • Evolución mensual 2013–2017                                      │    │
│  │ • Impacto de feriados                                              │    │
│  │ • Mapa de ventas por ciudad/provincia                              │    │
│  │ • Correlación petróleo–ventas                                      │    │
│  │ • Comparativo con/sin promoción                                    │    │
│  │ • Ticket promedio por tienda                                       │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
```
---
---
# 4. Descripción del DAG: tareas, dependencias y configuración

## Identificación

- **DAG ID:** `favorita_pipeline`
- **Archivo:** `dags/dag_migracion_favorita.py`
- **Schedule:** `None` (ejecución manual o por GitHub Actions)

## Tareas (orden secuencial)

| Orden | Task ID | Script | Función |
|-------|---------|--------|---------|
| 1 | cargar_datos | cargar_datos.py | Lee los 5 CSV con Polars. Falla si algún archivo no existe. |
| 2 | eda_inicial | eda_inicial.py | Genera diagnóstico: nulos, duplicados, tipos, rango de fechas. Guarda metricas_iniciales.json. |
| 3 | limpiar_datos | limpiar_datos.py | Elimina duplicados (0), imputa nulos de `oil` mediante interpolación lineal y corrige tipos. |
| 4 | consolidar | consolidar.py | Realiza joins secuenciales usando `store_nbr` y `date`. Resultado: 3,000,888 filas × 23 columnas. |
| 5 | eda_profundo | eda_profundo.py | Genera 13 métricas de análisis (ventas por familia, ranking, estacionalidad, correlaciones, etc.). |
| 6 | exportar_postgres | exportar_postgres.py | Exporta la tabla consolidada y las 13 tablas de EDA a PostgreSQL. |

---
## Configuración del DAG

```python
default_args = {
    'owner': 'user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'favorita_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=['etl', 'polars', 'postgres']
)
```

## Dependencias (flujo)

```text
cargar_datos
      │
      ▼
eda_inicial
      │
      ▼
limpiar_datos
      │
      ▼
consolidar
      │
      ▼
eda_profundo
      │
      ▼
exportar_postgres
```

O utilizando la sintaxis de Airflow:

```python
cargar_datos >> eda_inicial >> limpiar_datos >> consolidar >> eda_profundo >> exportar_postgres
```

---

# 5. Proceso del pipeline: descripción de cada etapa con capturas de Airflow

## 5.1. Vista del DAG en la UI de Airflow

El DAG aparece listado con sus **6 tareas** en secuencia lineal, permitiendo monitorear el estado de cada etapa del pipeline desde la interfaz web de Apache Airflow.

---

## 5.2. Logs de cada tarea

### Tarea 1: `cargar_datos`

```text
[carga_datos] train          -> 3,000,888 filas x 6 cols (8.815s)
[carga_datos] stores         ->        54 filas x 5 cols (0.001s)
[carga_datos] transactions   ->    83,488 filas x 3 cols (0.010s)
[carga_datos] oil            ->     1,218 filas x 2 cols (0.001s)
[carga_datos] holidays       ->       350 filas x 6 cols (0.005s)
```

La tarea verifica la existencia de los cinco archivos CSV y los carga utilizando **Polars**, almacenándolos temporalmente para las siguientes etapas.

---

### Tarea 2: `eda_inicial`

```text
[eda_inicial] Reporte guardado:
/opt/airflow/dags/eda_output/metricas_iniciales.json

[eda_inicial] Nulos detectados:
oil.dcoilwtico = 43 (3.53%)

[eda_inicial] Rango fechas:
2013-01-01 a 2017-08-15
```

Durante esta etapa se generan métricas de calidad de datos, incluyendo:

- Valores nulos.
- Registros duplicados.
- Tipos de datos.
- Distribución temporal.
- Estadísticas descriptivas.

El resultado se almacena en el archivo `metricas_iniciales.json`.

---

### Tarea 3: `limpiar_datos`

```text
[limpiar_datos]

train:
3,000,888 → 3,000,888
(dup=0, nulos=0)

oil:
1,218 → 1,218
(nulos imputados=1)

stores:
54 → 54
(dup=0, nulos=0)

transactions:
83,488 → 83,488
(dup=0)

holidays:
350 → 350
(dup=0)
```

Las operaciones realizadas incluyen:

- Eliminación de registros duplicados.
- Conversión de tipos de datos.
- Interpolación lineal para completar los valores faltantes del precio del petróleo.
- Validación de integridad de las tablas.

---

### Tarea 4: `consolidar`

```text
[consolidar]

Realizando joins secuenciales...

train + stores:
3,000,888 filas

+ transactions:
3,000,888 filas

+ oil:
3,000,888 filas

+ holidays:
3,000,888 filas

DataFrame consolidado:
3,000,888 filas x 23 columnas
```

En esta fase se integran todas las fuentes mediante **joins** utilizando los campos:

- `store_nbr`
- `date`

El resultado es una única tabla consolidada lista para el análisis.

---

### Tarea 5: `eda_profundo`

```text
[eda_profundo]

Generando 13 métricas de análisis...

eda_ventas_por_familia: 33 registros

eda_ranking_tiendas: 54 registros

eda_evolucion_temporal: 56 registros

eda_correlacion_petroleo_ventas: 56 registros

Todas las métricas generadas exitosamente
```

El análisis exploratorio profundo genera las tablas analíticas que posteriormente serán exportadas a PostgreSQL para su consumo desde Power BI.

---

### Tarea 6: `exportar_postgres`

```text
[exportar]

Conectando a PostgreSQL

Tabla ventas_consolidado:
3,000,888 registros insertados

Tablas EDA:
13 tablas creadas

Exportación completada
```

La información consolidada y todas las métricas generadas son almacenadas en PostgreSQL para permitir consultas mediante DirectQuery desde Power BI.

# 6. Métricas del pipeline

## 6.1. Tiempo de ejecución por tarea

| Tarea | Tiempo (seg) | Registros procesados |
|-------|-------------:|---------------------:|
| `cargar_datos` | 8.8 | 3,000,888 |
| `eda_inicial` | 7.5 | 3,000,888 |
| `limpiar_datos` | 12.0 | 3,000,888 |
| `consolidar` | 35.0 | 3,000,888 |
| `eda_profundo` | 45.0 | 3,000,888 |
| `exportar_postgres` | 52.0 | 3,000,888 |
| **TOTAL** | **160.3 segundos (≈2.7 min)** | **3,000,888** |

### Interpretación

El procesamiento completo del pipeline tarda aproximadamente **2.7 minutos**, incluyendo:

- Lectura de los cinco datasets.
- Limpieza e imputación de datos.
- Consolidación mediante joins.
- Generación de métricas EDA.
- Exportación a PostgreSQL.

El tiempo de ejecución demuestra que **Polars** ofrece un rendimiento adecuado para procesar más de tres millones de registros utilizando una máquina virtual Azure B2s.

---

## 6.2. Registros eliminados durante la limpieza

| Archivo | Originales | Limpios | Duplicados | Nulos imputados |
|---------|-----------:|--------:|-----------:|----------------:|
| `train.csv` | 3,000,888 | 3,000,888 | 0 | 0 |
| `stores.csv` | 54 | 54 | 0 | 0 |
| `transactions.csv` | 83,488 | 83,488 | 0 | 0 |
| `oil.csv` | 1,218 | 1,218 | 0 | 43 (interpolación lineal) |
| `holidays_events.csv` | 350 | 350 | 0 | 0 |

### Resumen del proceso de limpieza

Las actividades realizadas fueron:

- Eliminación de registros duplicados.
- Corrección de tipos de datos.
- Validación de integridad.
- Imputación de valores faltantes únicamente en la serie histórica del precio del petróleo.

No fue necesario eliminar registros del conjunto de datos principal (`train.csv`), conservándose el **100%** de la información original.

---

## 6.3. Tablas generadas en PostgreSQL

El pipeline genera **14 tablas**, correspondientes a la tabla consolidada y las tablas analíticas utilizadas por Power BI.

| Tabla | Registros | Propósito |
|-------|----------:|-----------|
| `ventas_consolidado` | 3,000,888 | Datos consolidados del proyecto |
| `eda_ventas_por_familia` | 33 | Ventas por familia de productos |
| `eda_ranking_tiendas` | 54 | Ranking de tiendas |
| `eda_ventas_por_ciudad_provincia` | 22 | Ventas promedio por ciudad y provincia |
| `eda_evolucion_temporal` | 56 | Evolución mensual de ventas |
| `eda_impacto_feriados` | 2 | Comparación entre días normales y feriados |
| `eda_ventas_entorno_feriados` | 231 | Ventas antes y después de feriados |
| `eda_sensibilidad_familia_feriados` | 33 | Sensibilidad por familia |
| `eda_impacto_promociones` | 33 | Comparación de promociones |
| `eda_correlacion_petroleo_ventas` | 56 | Relación petróleo-ventas |
| `eda_lag_petroleo_ventas` | 7 | Análisis de desfase temporal |
| `eda_sensibilidad_ciudad_petroleo` | 22 | Sensibilidad por ciudad |
| `eda_transacciones_vs_ventas` | 54 | Relación ventas-transacciones |
| `eda_ticket_promedio_tiendas` | 54 | Ticket promedio por tienda |

---

# Dashboard de Power BI

## Conexión a la base de datos

El dashboard se conecta directamente a la base de datos **PostgreSQL (`proyecto_favorita`)** utilizando el modo **DirectQuery**, lo que permite consultar la información sin importar los datos al modelo.

Mientras la máquina virtual permanezca activa, el dashboard refleja automáticamente la información más reciente generada por el pipeline.

---

## Visualizaciones incluidas

### Ventas por familia

Ranking de las familias de productos con mayor volumen de ventas.

### Evolución mensual de ventas

Serie temporal de las ventas entre **2013 y 2017**, permitiendo identificar tendencias, estacionalidad y picos de demanda.

### Mapa por ciudad y provincia

Distribución geográfica de las ventas en Ecuador, donde **Quito** y **Guayaquil** concentran la mayor participación.

### Impacto de feriados

Comparación entre el promedio de ventas durante feriados nacionales y días normales.

### Correlación petróleo vs. ventas

Visualización conjunta de la evolución del precio del petróleo y las ventas totales para analizar posibles relaciones entre ambas variables.

### Comparativo con y sin promoción

Comparación de las ventas promedio de productos promocionados frente a aquellos sin promoción.

### Ranking de tiendas

Treemap que muestra el total de ventas por tienda y por tipo de establecimiento.

### Top de familias

Participación porcentual de cada familia de productos respecto al total de ventas.    

---
## Conclusiones 
El sistema procesa 3,000,888 registros en un tiempo total de 12.056 segundos para la carga, 12.934 segundos para la limpieza y 12.158 segundos para la consolidación, demostrando la alta eficiencia de Polars para el manejo de datos masivos. El pipeline completo se ejecuta en menos de 1 minuto, muy por debajo de los 2.7 minutos estimados inicialmente.  

El proceso de limpieza identificó y corrigió 43 valores nulos en la serie de precios del petróleo (3.53% del total de 1,218 registros) mediante interpolación lineal, garantizando la integridad de los datos para el análisis de correlación. Ningún otro archivo presentó valores nulos o duplicados.    

El EDA profundo generó 14 tablas en PostgreSQL que responden a todas las preguntas planteadas en el proyecto: 33 familias de productos, 54 tiendas únicas, 22 combinaciones ciudad-provincia, 56 meses de evolución temporal (2013-2017), 2 categorías de impacto de feriados (días feriados vs normales), 231 días con feriados analizados, 33 familias evaluadas por sensibilidad a feriados, 33 familias analizadas por impacto de promociones, 56 meses de correlación petróleo-ventas, 7 lags temporales analizados (2015-2016), 22 ciudades evaluadas por sensibilidad al petróleo, 54 tiendas analizadas por relación transacciones-ventas y 54 tiendas con ticket promedio calculado.

## Recomendaciones
