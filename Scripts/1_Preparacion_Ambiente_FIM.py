# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - PLATAFORMA DE DATOS FIM
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuración de Parámetros

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageName default ".";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Catálogo Principal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_fim;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de Esquemas por Capa

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_fim.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_fim.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_fim.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_fim.golden;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuración de Ubicaciones Externas
# MAGIC # Ubicación para archivos RAW (datos crudos)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-fim-raw`
# MAGIC URL 'abfss://fim-raw@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para archivos raw FIM';

# COMMAND ----------

# MAGIC %md
# MAGIC # Ubicación para tablas BRONZE (datos limpios básicos)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-fim-bronze`
# MAGIC URL 'abfss://fim-bronze@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para tablas bronze FIM';

# COMMAND ----------

# MAGIC %md
# MAGIC # Ubicación para tablas SILVER (datos enriquecidos)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-fim-silver`
# MAGIC URL 'abfss://fim-silver@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para tablas silver FIM';

# COMMAND ----------

# MAGIC %md
# MAGIC # Ubicación para tablas GOLDEN (datos master/curated)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-fim-golden`
# MAGIC URL 'abfss://fim-golden@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para tablas golden FIM';