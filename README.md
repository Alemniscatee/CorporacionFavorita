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
┌────────────────────────────────────────────────────────────────────────────┐
│                          AZURE VM (Ubuntu 22.04)                          │
│                                                                           │
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
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐     │  │  │
│  │  │  │  Carga   │→│ Limpieza │→│Consolida-│→│EDA Profundo│     │  │  │
│  │  │  └──────────┘  └──────────┘  └──────────┘  └────────────┘     │  │  │
│  │  │                                                              │  │  │
│  │  │  ┌──────────────────────────────────────────────────────┐     │  │  │
│  │  │  │  Exportación a PostgreSQL (14 tablas)                │     │  │  │
│  │  │  └──────────────────────────────────────────────────────┘     │  │  │
│  │  └──────────────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │               DATOS LOCALES (dags/datasets/)                       │   │
│  │ train.csv | stores.csv | transactions.csv | oil.csv | holidays    │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (DirectQuery)

┌────────────────────────────────────────────────────────────────────────────┐
│                       POWER BI DESKTOP                                    │
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
# 6. Métricas del pipeline

## 6.1 Tiempo de ejecución por tarea

| Tarea | Tiempo (seg) | Registros procesados |
|--------|-------------:|--------------------:|
| cargar_datos | 9.5 | 3,000,888 |
| eda_inicial | 8.2 | 3,000,888 |
| limpiar_datos | 12.4 | 3,000,888 |
| consolidar | 35.7 | 3,000,888 |
| eda_profundo | 45.3 | 3,000,888 |
| exportar_postgres | 52.1 | 3,000,888 |
| **TOTAL** | **163.2 (2.7 min)** | **3,000,888** |

---

## 6.2 Registros eliminados durante la limpieza

| Archivo | Originales | Limpios | Duplicados | Nulos imputados |
|---------|-----------:|--------:|-----------:|----------------:|
| train.csv | 3,000,888 | 3,000,888 | 0 | 0 |
| stores.csv | 54 | 54 | 0 | 0 |
| transactions.csv | 83,488 | 83,488 | 0 | 0 |
| oil.csv | 1,218 | 1,218 | 0 | 214 (interpolación) |
| holidays_events.csv | 350 | 350 | 0 | 0 |

---

## 6.3 Tablas generadas en PostgreSQL (14 tablas)

| Tabla | Registros | Propósito |
|-------|----------:|-----------|
| ventas_consolidado | 3,000,888 | Datos maestros consolidados |
| eda_ventas_por_familia | 33 | Volumen de ventas por categoría |
| eda_ranking_tiendas | 54 | Ranking de tiendas por ventas |
| eda_ventas_por_ciudad_provincia| 22 | Ventas promedio geográficas |
| eda_evolucion_temporal | 56 | Tendencia mensual (2013–2017) |
| eda_impacto_feriados | 2 | Comparativo feriado vs normal |
| eda_ventas_entorno_feriados | 231 | Días previos/posteriores a feriados |
| eda_sensibilidad_familia_feriados | 33 | Familias más sensibles a feriados |
| eda_impacto_promociones | 33 | Promedio de ventas con/sin promoción |
| eda_correlacion_petroleo_ventas | 56 | Correlación mensual petróleo-ventas |
| eda_lag_petroleo_ventas | 7 | Lag temporal (2015–2016) |
| eda_sensibilidad_ciudad_petroleo | 22 | Ciudades más sensibles al petróleo |
| eda_transacciones_vs_ventas | 54 | Relación transacciones-ventas por tienda |
| eda_ticket_promedio_tiendas | 54 | Ticket promedio por tienda |
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
