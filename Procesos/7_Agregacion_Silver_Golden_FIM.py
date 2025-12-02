# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### AGREGACIÓN - SILVER A GOLDEN (KPIs)
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text catalogo default ".";

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CÁLCULO DE KPIs Y MÉTRICAS DE NEGOCIO
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. KPI: Métricas de Pagos por Período

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE catalog_fim.golden.kpi_pagos_periodo
# MAGIC SELECT
# MAGIC p.periodo_codigo,
# MAGIC p.periodo_nombre,
# MAGIC COUNT(*) as total_transacciones,
# MAGIC SUM(p.monto) as monto_total_recaudado,
# MAGIC AVG(p.monto) as ticket_promedio,
# MAGIC COUNT(DISTINCT p.idAlumno) as alumnos_unicos_pagadores,
# MAGIC SUM(CASE WHEN p.pago_valido THEN 1 ELSE 0 END) as pagos_validos,
# MAGIC SUM(CASE WHEN p.pago_valido THEN p.monto ELSE 0 END) as monto_valido,
# MAGIC MIN(p.fecha_pago) as fecha_primer_pago,
# MAGIC MAX(p.fecha_pago) as fecha_ultimo_pago
# MAGIC FROM catalog_fim.silver.pagos_validados p
# MAGIC GROUP BY p.periodo_codigo, p.periodo_nombre;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar si la tabla tiene datos
# MAGIC SELECT COUNT(*) as total_registros 
# MAGIC FROM catalog_fim.golden.kpi_pagos_periodo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. KPI: Estado de Alumnos y Comportamiento de Pagos

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE catalog_fim.golden.kpi_estado_alumnos_pagos
# MAGIC SELECT
# MAGIC a.id_alumno,
# MAGIC a.dni,
# MAGIC CONCAT(a.primerNombre, ' ', a.apellidoPaterno) as nombre_completo,
# MAGIC a.email,
# MAGIC a.maestria,
# MAGIC a.sede,
# MAGIC COUNT(p.id) as total_pagos_realizados,
# MAGIC SUM(p.monto) as total_invertido,
# MAGIC AVG(p.monto) as promedio_pago,
# MAGIC MAX(p.fecha_pago) as ultima_fecha_pago,
# MAGIC COUNT(DISTINCT p.periodo_codigo) as periodos_pagados,
# MAGIC CASE
# MAGIC WHEN MAX(p.fecha_pago) IS NULL THEN 'SIN_PAGOS'
# MAGIC WHEN DATEDIFF(CURRENT_DATE, MAX(p.fecha_pago)) > 90 THEN 'MOROSO'
# MAGIC WHEN DATEDIFF(CURRENT_DATE, MAX(p.fecha_pago)) BETWEEN 30 AND 90 THEN 'PENDIENTE'
# MAGIC ELSE 'AL_DIA'
# MAGIC END as estado_pago,
# MAGIC CASE
# MAGIC WHEN SUM(p.monto) > 10000 THEN 'ALTO_VALOR'
# MAGIC WHEN SUM(p.monto) BETWEEN 5000 AND 10000 THEN 'MEDIO_VALOR'
# MAGIC ELSE 'BAJO_VALOR'
# MAGIC END as segmento_valor
# MAGIC FROM catalog_fim.silver.alumnos_completos a
# MAGIC LEFT JOIN catalog_fim.silver.pagos_validados p ON a.id_alumno = p.idAlumno AND p.pago_valido = true
# MAGIC GROUP BY a.id_alumno, a.dni, a.primerNombre, a.apellidoPaterno, a.email, a.maestria, a.sede;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. KPI: Rentabilidad por Maestrías

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE catalog_fim.golden.kpi_maestrias_rentables
# MAGIC SELECT
# MAGIC a.maestria,
# MAGIC a.sede,
# MAGIC COUNT(DISTINCT a.id_alumno) as total_alumnos,
# MAGIC COUNT(DISTINCT p.idAlumno) as alumnos_pagadores,
# MAGIC SUM(p.monto) as ingreso_total,
# MAGIC AVG(p.monto) as ingreso_promedio_por_alumno,
# MAGIC COUNT(p.id) as total_pagos
# MAGIC FROM catalog_fim.silver.alumnos_completos a
# MAGIC LEFT JOIN catalog_fim.silver.pagos_validados p ON a.id_alumno = p.idAlumno AND p.pago_valido = true
# MAGIC GROUP BY a.maestria, a.sede;