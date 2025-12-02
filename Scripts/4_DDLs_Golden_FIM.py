# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - CAPA GOLDEN FIM
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageLocation default ".";
# MAGIC create widget text catalogo default ".";

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalogo}.golden;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalogo}

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CREACIÓN DE TABLAS GOLDEN - KPIs Y MÉTRICAS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Métricas de Pagos por Periodo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.golden.kpi_pagos_periodo (
# MAGIC periodo_codigo STRING,
# MAGIC periodo_nombre STRING,
# MAGIC total_transacciones LONG,
# MAGIC monto_total_recaudado DOUBLE,
# MAGIC ticket_promedio DOUBLE,
# MAGIC alumnos_unicos_pagadores LONG,
# MAGIC pagos_validos LONG,
# MAGIC monto_valido DOUBLE,
# MAGIC fecha_primer_pago DATE,
# MAGIC fecha_ultimo_pago DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/kpi_pagos_periodo';

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Estado de Alumnos y su Comportamiento de Pago

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.golden.kpi_estado_alumnos_pagos (
# MAGIC id_alumno INT,
# MAGIC dni STRING,
# MAGIC nombre_completo STRING,
# MAGIC email STRING,
# MAGIC maestria STRING,
# MAGIC sede STRING,
# MAGIC total_pagos_realizados LONG,
# MAGIC total_invertido DOUBLE,
# MAGIC promedio_pago DOUBLE,
# MAGIC ultima_fecha_pago DATE,
# MAGIC periodos_pagados LONG,
# MAGIC estado_pago STRING,
# MAGIC segmento_valor STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/kpi_estado_alumnos_pagos';

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Rentabilidad por Maestrías

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.golden.kpi_maestrias_rentables (
# MAGIC maestria STRING,
# MAGIC sede STRING,
# MAGIC total_alumnos LONG,
# MAGIC alumnos_pagadores LONG,
# MAGIC ingreso_total DOUBLE,
# MAGIC ingreso_promedio_por_alumno DOUBLE,
# MAGIC total_pagos LONG
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/kpi_maestrias_rentables';