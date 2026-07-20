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

## Arquitectura del Proyecto

```text
┌───────────────────────────────────────────────────────────────────────────┐
│                          AZURE VM (Ubuntu 22.04)                          │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                         DOCKER COMPOSE                             │   │
│  │                                                                    │   │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐   │   │
│  │  │   Airflow     │  │   Airflow     │  │     PostgreSQL        │   │   │
│  │  │  Scheduler    │  │  Webserver    │  │   (proyecto_favorita) │   │   │
│  │  │               │  │   Puerto 8080 │  │                       │   │   │
│  │  └───────┬───────┘  └───────────────┘  └───────────┬───────────┘   │   │
│  │          │                                         │               │   │
│  │  ┌───────▼─────────────────────────────────────────▼────────────┐  │   │
│  │  │                 Scripts (Python + Polars)                    │  │   │
│  │  │                                                              │  │   │
│  │  │  ┌──────────┐  ┌──────────┐  ┌─────────────┐  ┌────────────┐ │  │   │
│  │  │  │  Carga   │→ │ Limpieza │→ │Consolidación│→ │EDA Profundo│ │  │   │
│  │  │  └──────────┘  └──────────┘  └─────────────┘  └────────────┘ │  │   │
│  │  │                                                              │  │   │
│  │  │  ┌────────────────────────────────────────────────────────┐  │  │   │
│  │  │  │ Exportación a PostgreSQL (14 tablas analíticas)        │  │  │   │
│  │  │  └────────────────────────────────────────────────────────┘  │  │   │
│  │  └──────────────────────────────────────────────────────────────┘  │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │               DATOS LOCALES (dags/datasets/)                       │   │
│  │ train.csv | stores.csv | transactions.csv | oil.csv | holidays     │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (DirectQuery)
┌────────────────────────────────────────────────────────────────────────────┐
│                           POWER BI DESKTOP                                 │
│                                                                            │
│  Dashboard conectado directamente a PostgreSQL mediante DirectQuery para   │
│  visualizar los datos procesados por el pipeline en tiempo casi real.      │
└────────────────────────────────────────────────────────────────────────────┘
```

---

# Estructura del Pipeline

El pipeline de datos está orquestado mediante **Apache Airflow** y ejecuta seis etapas de forma secuencial. Cada tarea depende de la finalización exitosa de la anterior, garantizando la consistencia de la información antes de almacenarla en la base de datos.

Si una etapa crítica falla, Airflow bloquea automáticamente las tareas posteriores para preservar la integridad del proceso.

```text
[descargar_datos]
        │
        ▼
[limpiar_datos]
        │
        ▼
[transformar_datos]
        │
        ▼
[validar_calidad]
        │
        ▼
[cargar_postgres]
        │
        ▼
[generar_reporte]
```

## Descripción de las etapas

| Etapa | Descripción |
|-------|-------------|
| **Descargar datos** | Obtiene los archivos CSV utilizados por el proyecto. |
| **Limpiar datos** | Corrige tipos de datos, elimina registros inválidos y trata valores faltantes. |
| **Transformar datos** | Consolida la información y genera las tablas analíticas utilizando Polars. |
| **Validar calidad** | Comprueba duplicados, consistencia e integridad antes de la carga. |
| **Cargar PostgreSQL** | Exporta las tablas finales a la base de datos `proyecto_favorita`. |
| **Generar reporte** | Registra métricas y resultados de la ejecución del pipeline. |

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
