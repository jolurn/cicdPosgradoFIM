# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - INGESTA DIMENSIONES 2
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storageName",".")
dbutils.widgets.text("catalogo",".")

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

ruta_raw = f"abfss://fim-raw@{storageName}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### INGESTA DIMENSIONES 2 - TABLAS MAESTRAS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de Tablas a Ingresar

# COMMAND ----------

# Tablas dimensiones pequeñas - FASE 2
tablas_dimensiones_2 = [
  "pfimapp_sede",
  "pfimapp_estadoboletap",
  "pfimapp_conceptopago",
  "pfimapp_maestria",
  "pfimapp_periodo"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Función de Ingesta

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Función para ingestar CSV a Bronze
def ingestar_csv_a_bronze(tabla):
    csv_path = f"{ruta_raw}/{tabla}.csv"
    bronze_table = f"{catalogo}.bronze.{tabla}"

    print(f"📥 Ingestando: {tabla}")

    try:
        df = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
                .withColumn("fecha_ingesta", current_timestamp()))

        df.write.format("delta")\
            .option("mergeSchema", "true")\
            .mode("overwrite")\
            .saveAsTable(bronze_table)
        
        count = df.count()
        print(f"✅ {tabla}: {count} registros")
        return True

    except Exception as e:
        print(f"❌ Error en {tabla}: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución y Resumen de Ingesta

# COMMAND ----------

# Ejecutar ingesta para tablas dimensiones 2
resultados = []
for tabla in tablas_dimensiones_2:
    resultado = ingestar_csv_a_bronze(tabla)
    resultados.append((tabla, resultado))

# COMMAND ----------

# Resumen de ingesta
print("\n📊 RESUMEN DE INGESTA - DIMENSIONES 2:")
for tabla, estado in resultados:
    icon = "✅" if estado else "❌"
    print(f"{icon} {tabla}")

# COMMAND ----------

# VERIFICACIÓN DE DATOS DIMENSIONES 2
print("\n🔍 VERIFICACIÓN DE DIMENSIONES 2:")

for tabla in tablas_dimensiones_2:
    try:
        count = spark.sql(f"SELECT COUNT(*) as total FROM {catalogo}.bronze.{tabla}").collect()[0]['total']
        print(f"\n📋 {tabla}: {count} registros")
        if count > 0:
            spark.sql(f"SELECT * FROM {catalogo}.bronze.{tabla} LIMIT 3").show()
        else:
            print("❌ TABLA VACÍA")
    except Exception as e:
        print(f"❌ Error verificando {tabla}: {str(e)}")