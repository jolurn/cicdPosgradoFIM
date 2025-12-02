# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### REINICIO COMPLETO - PLATAFORMA FIM
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN DE PARÁMETROS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inicialización de Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storageName",".")
dbutils.widgets.text("containerName",".")
dbutils.widgets.text("catalogo",".")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de Parámetros

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
containerName = dbutils.widgets.get("containerName")
catalogo = dbutils.widgets.get("catalogo")

print("⚙️ PARÁMETROS CONFIGURADOS:")
print(f"   Storage: {storageName}")
print(f"   Container: {containerName}")
print(f"   Catálogo: {catalogo}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### ELIMINACIÓN DE TABLAS GOLDEN (KPIs)
# MAGIC ### =====================================================

# COMMAND ----------

# Celda única - Manejo automático de errores
tables_to_remove = [
    "catalog_fim.golden.kpi_pagos_periodo",
    "catalog_fim.golden.kpi_estado_alumnos_pagos", 
    "catalog_fim.golden.kpi_maestrias_rentables"
]

for table in tables_to_remove:
    try:
        spark.sql(f"ALTER SHARE segmento REMOVE TABLE {table}")
        print(f"✅ Removed from share: {table}")
    except Exception as e:
        # Si la tabla no existe en el share, continuamos sin error
        if "RESOURCE_DOES_NOT_EXIST" in str(e):
            print(f"ℹ️  Table not in share (skipping): {table}")
        else:
            # Si es otro error, lo mostramos pero continuamos
            print(f"⚠️  Other error with {table}: {str(e)[:100]}...")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG catalog_fim;
# MAGIC
# MAGIC DROP TABLE IF EXISTS catalog_fim.golden.kpi_pagos_periodo;
# MAGIC DROP TABLE IF EXISTS catalog_fim.golden.kpi_estado_alumnos_pagos;
# MAGIC DROP TABLE IF EXISTS catalog_fim.golden.kpi_maestrias_rentables;

# COMMAND ----------

print("✅ Tablas GOLDEN eliminadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### ELIMINACIÓN DE TABLAS SILVER (VISTAS ENRIQUECIDAS)
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_fim.silver.alumnos_completos;
# MAGIC DROP TABLE IF EXISTS catalog_fim.silver.pagos_validados;
# MAGIC DROP TABLE IF EXISTS catalog_fim.silver.maestrias_periodos;

# COMMAND ----------

print("✅ Tablas SILVER eliminadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### ELIMINACIÓN DE TABLAS BRONZE (DATOS CRUDOS)
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_estadocivil;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_tipodocumento;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_user;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_alumno;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_maestria;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_estadoacademico;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_periodo;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_sede;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_reporteeconomico;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_reporteeconomicopago;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_estadoboletap;
# MAGIC DROP TABLE IF EXISTS catalog_fim.bronze.pfimapp_conceptopago;

# COMMAND ----------

print("✅ Tablas BRONZE eliminadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### LIMPIEZA FÍSICA DE DATOS EN STORAGE
# MAGIC ### =====================================================

# COMMAND ----------

print("🗑️  INICIANDO LIMPIEZA FÍSICA DE DATOS...")

# Lista completa de tablas a limpiar físicamente
tablas_bronze = [
    "pfimapp_estadocivil", "pfimapp_tipodocumento", "pfimapp_user",
    "pfimapp_alumno", "pfimapp_maestria", "pfimapp_estadoacademico", 
    "pfimapp_periodo", "pfimapp_sede", "pfimapp_reporteeconomico",
    "pfimapp_reporteeconomicopago", "pfimapp_estadoboletap", "pfimapp_conceptopago"
]

tablas_silver = [
    "alumnos_completos", "pagos_validados", "maestrias_periodos"
]

tablas_golden = [
    "kpi_pagos_periodo", "kpi_estado_alumnos_pagos", "kpi_maestrias_rentables"
]

# Limpiar datos físicos de BRONZE
print("🔧 Limpiando datos BRONZE...")
for tabla in tablas_bronze:
    try:
        path = f"abfss://fim-bronze@{storageName}.dfs.core.windows.net/{tabla}"
        dbutils.fs.rm(path, True)
        print(f"   ✓ {tabla}")
    except Exception as e:
        print(f"   ⚠️  {tabla}: {str(e)}")

# Limpiar datos físicos de SILVER
print("🔧 Limpiando datos SILVER...")
for tabla in tablas_silver:
    try:
        path = f"abfss://fim-silver@{storageName}.dfs.core.windows.net/{tabla}"
        dbutils.fs.rm(path, True)
        print(f"   ✓ {tabla}")
    except Exception as e:
        print(f"   ⚠️  {tabla}: {str(e)}")

# Limpiar datos físicos de GOLDEN  
print("🔧 Limpiando datos GOLDEN...")
for tabla in tablas_golden:
    try:
        path = f"abfss://fim-golden@{storageName}.dfs.core.windows.net/{tabla}"
        dbutils.fs.rm(path, True)
        print(f"   ✓ {tabla}")
    except Exception as e:
        print(f"   ⚠️  {tabla}: {str(e)}")

print("🎯 LIMPIEZA FÍSICA COMPLETADA")

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### VERIFICACIÓN FINAL
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN catalog_fim.bronze;
# MAGIC SHOW TABLES IN catalog_fim.silver;
# MAGIC SHOW TABLES IN catalog_fim.golden;

# COMMAND ----------

print("=" * 50)
print("🎯 REINICIO COMPLETADO EXITOSAMENTE")
print("✅ Todas las tablas eliminadas del catálogo")
print("✅ Plataforma lista para re-inicialización")
print("=" * 50)