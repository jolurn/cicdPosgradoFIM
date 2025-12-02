# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - INGESTA USUARIOS
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
# MAGIC ### INGESTA USUARIOS - TABLAS CON DEPENDENCIAS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de Tablas de Usuarios

# COMMAND ----------

# Tabla usuario (depende de tipo documento y estado civil)
tablas_usuarios = [
    "pfimapp_user"
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
            .option("overwriteSchema", "true")\
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

# Ejecutar ingesta para tabla usuario
resultados = []
for tabla in tablas_usuarios:
    resultado = ingestar_csv_a_bronze(tabla)
    resultados.append((tabla, resultado))

# COMMAND ----------

# Resumen de ingesta
print("\n📊 RESUMEN DE INGESTA - USUARIOS:")
for tabla, estado in resultados:
    icon = "✅" if estado else "❌"
    print(f"{icon} {tabla}")

# COMMAND ----------

# VERIFICACIÓN DE USUARIOS
print("\n🔍 VERIFICACIÓN DE TABLA USUARIOS:")

for tabla in tablas_usuarios:  # Solo "pfimapp_user"
    try:
        count = spark.sql(f"SELECT COUNT(*) as total FROM {catalogo}.bronze.{tabla}").collect()[0]['total']
        print(f"\n📋 {tabla}: {count} registros")
        if count > 0:
            spark.sql(f"SELECT * FROM {catalogo}.bronze.{tabla} LIMIT 3").show()
        else:
            print("❌ TABLA VACÍA")
    except Exception as e:
        print(f"❌ Error verificando {tabla}: {str(e)}")