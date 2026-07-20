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

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                          AZURE VM (Ubuntu 22.04)                          │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                         DOCKER COMPOSE                              │  │
│  │                                                                     │  │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐  │  │
│  │  │   Airflow     │  │   Airflow     │  │     PostgreSQL        │  │  │
│  │  │  Scheduler    │  │  Webserver    │  │    (proyecto_         │  │  │
│  │  │               │  │   (Puerto     │  │     favorita)         │  │  │
│  │  │               │  │    8080)      │  │                       │  │  │
│  │  └───────┬───────┘  └───────────────┘  └───────────┬───────────┘  │  │
│  │          │                                         │               │  │
│  │  ┌───────▼─────────────────────────────────────────▼───────────┐  │  │
│  │  │                 SCRIPTS (Python + Polars)                    │  │  │
│  │  │                                                              │  │  │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐     │  │
│  │  │  │  Carga   │→│ Limpieza │→│Consolida-│→│EDA Profundo│     │  │
│  │  │  └──────────┘  └──────────┘  └──────────┘  └────────────┘     │  │
│  │  │                                                              │  │
│  │  │  ┌──────────────────────────────────────────────────────┐     │  │
│  │  │  │  Exportación a PostgreSQL (14 tablas)                │     │  │
│  │  │  └──────────────────────────────────────────────────────┘     │  │
│  │  └──────────────────────────────────────────────────────────────┘ │
│  └────────────────────────────────────────────────────────────────────┘
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │               DATOS LOCALES (dags/datasets/)                       │   │
│  │  train.csv | stores.csv | transactions.csv | oil.csv | holidays   │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (DirectQuery)

┌────────────────────────────────────────────────────────────────────────────┐
│                       POWER BI DESKTOP                                     │
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                        DASHBOARD                                   │    │
│  │                                                                    │    │
│  │ • Ventas por familia de producto                                   │    │
│  │ • Ranking de tiendas                                               │    │
│  │ • Evolución mensual 2013-2017                                      │    │
│  │ • Impacto de feriados                                              │    │
│  │ • Mapa de ventas por ciudad/provincia                              │    │
│  │ • Correlación petróleo-ventas                                      │    │
│  │ • Comparativo con/sin promoción                                    │    │
│  │ • Ticket promedio por tienda                                       │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

---
---

## Estructura del  Pipeline

El pipeline de datos ejecuta 6 etapas secuenciales de forma lineal (flujo de riel). Si una etapa crítica falla, las siguientes se bloquean automáticamente para resguardar la integridad de la base de datos:

```text
[descargar_datos] ➔ [limpiar_datos] ➔ [transformar_datos] ➔ [validar_calidad] ➔ [cargar_postgres] ➔ [generar_reporte]



## Dashboard de Power BI

### Conexión a la base de datos
El dashboard se conecta directamente a la base de datos en PostgreSQL (`proyecto_favorita`) en modo **DirectQuery**,
con actualización en tiempo real para visualizar los datos más recientes en el pipeline, mientras la VM se encuentre activa.   

### Gráficos incluidos

**Ventas por familia** — 
Ranking de las categorías de producto con mayor volumen de ventas.

**Evolución mensual de ventas** — 
Serie temporal de ventas totales por mes, mostrando estacionalidad y picos de demanda a lo largo del periodo analizado.

**Mapa por ciudad/provincia** — 
Distribución geográfica de las ventas en Ecuador, en Quito y Guayaquil concentrando la mayor proporción del total.

**Impacto de feriados** —
Comparación de ventas promedio en feriado nacional vs día normal.

**Correlación petróleo vs ventas** (evolución del tiempo)— 
Evolución conjunta del precio del petróleo y las ventas totales, para observar si se mueven de forma similar.

**Comparativo con/sin promoción** — 
Ventas promedio de producto por familia y comparación entre unidades con y sin promoción.

**Ranking de tiendas** — 
Visualización tipo treemap del total de ventas por tienda y tipo de tienda.

**Top de familias** — 
Participación porcentual de cada familia de producto sobre el total de ventas.

