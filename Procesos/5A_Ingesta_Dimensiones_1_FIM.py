# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - INGESTA DE DATOS
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parámetros y Variables

# COMMAND ----------

dbutils.widgets.text("storageName",".")
dbutils.widgets.text("catalogo",".")

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

ruta_raw = f"abfss://fim-raw@{storageName}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### INGESTA DIMENSIONES 1 - TABLAS MAESTRAS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de Tablas a Ingresar

# COMMAND ----------

# Tablas dimensiones pequeñas - FASE 1
tablas_dimensiones_1 = [
    "pfimapp_estadocivil",
    "pfimapp_tipodocumento",
    "pfimapp_estadoacademico"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Función de Ingesta

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Función para ingestar CSV a Bronze CON mergeSchema
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

# Ejecutar ingesta para tablas dimensiones 1
resultados = []
for tabla in tablas_dimensiones_1:
    resultado = ingestar_csv_a_bronze(tabla)
    resultados.append((tabla, resultado))

# COMMAND ----------

# Resumen de ingesta
print("\n📊 RESUMEN DE INGESTA - DIMENSIONES 1:")
for tabla, estado in resultados:
    icon = "✅" if estado else "❌"
    print(f"{icon} {tabla}")

# COMMAND ----------

# VERIFICACIÓN DE DATOS INGERIDOS
print("\n🔍 VERIFICACIÓN DE DATOS INGERIDOS:")

for tabla in tablas_dimensiones_1:
    try:
        # Contar registros en la tabla creada
        count = spark.sql(f"SELECT COUNT(*) as total FROM {catalogo}.bronze.{tabla}").collect()[0]['total']
        
        # Mostrar sample de datos
        print(f"\n📋 {tabla}: {count} registros")
        if count > 0:
            spark.sql(f"SELECT * FROM {catalogo}.bronze.{tabla} LIMIT 3").show()
        else:
            print("❌ TABLA VACÍA - Revisar archivo CSV")
            
    except Exception as e:
        print(f"❌ Error verificando {tabla}: {str(e)}")

# VERIFICAR ARCHIVOS EN RAW
print("\n📁 VERIFICANDO ARCHIVOS EN RAW:")
try:
    archivos = dbutils.fs.ls(ruta_raw)
    print("✅ Archivos encontrados en RAW:")
    for archivo in archivos:
        print(f"   📄 {archivo.name}")
except Exception as e:
    print(f"❌ Error accediendo a RAW: {str(e)}")