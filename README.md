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

---

## Estructura del  Pipeline

El pipeline de datos ejecuta 6 etapas secuenciales de forma lineal (flujo de riel). Si una etapa crítica falla, las siguientes se bloquean automáticamente para resguardar la integridad de la base de datos:

```text
[descargar_datos] ➔ [limpiar_datos] ➔ [transformar_datos] ➔ [validar_calidad] ➔ [cargar_postgres] ➔ [generar_reporte]
