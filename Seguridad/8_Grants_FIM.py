# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN DE PERMISOS - SEGURIDAD FIM
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### ASIGNACIÓN DE PERMISOS POR EQUIPO
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permisos a Nivel de Catálogo

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog_fim TO `academico`;
# MAGIC GRANT USE CATALOG ON CATALOG catalog_fim TO `financiera`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permisos para Equipo Académico

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permisos en Bronze (Lectura y Escritura)
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_fim.bronze TO `academico`;
# MAGIC GRANT CREATE ON SCHEMA catalog_fim.bronze TO `academico`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permisos en Silver (Lectura)
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_fim.silver TO `academico`;
# MAGIC GRANT SELECT ON SCHEMA catalog_fim.silver TO `academico`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permisos en Golden (Lectura)
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_fim.golden TO `academico`;
# MAGIC GRANT SELECT ON SCHEMA catalog_fim.golden TO `academico`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permisos para Equipo Financiera (Solo Lectura)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permisos en Silver (Solo Lectura)
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_fim.silver TO `financiera`;
# MAGIC GRANT SELECT ON SCHEMA catalog_fim.silver TO `financiera`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permisos en Golden (Solo Lectura)
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_fim.golden TO `financiera`;
# MAGIC GRANT SELECT ON SCHEMA catalog_fim.golden TO `financiera`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### VERIFICACIÓN DE PERMISOS ASIGNADOS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de Grants en Tablas Golden

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE catalog_fim.golden.kpi_pagos_periodo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de Grants en Schema Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA catalog_fim.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de Grants en Catálogo

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG catalog_fim;