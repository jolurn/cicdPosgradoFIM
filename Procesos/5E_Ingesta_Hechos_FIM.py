# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - INGESTA HECHOS/PAGOS
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
# MAGIC ### INGESTA HECHOS - TABLAS TRANSACCIONALES
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de Tablas de Hechos/Pagos

# COMMAND ----------

# Tablas de hechos/pagos (dependen de alumno y dimensiones)
tablas_hechos = [
    "pfimapp_reporteeconomico",
    "pfimapp_reporteecoconceptopago"
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
        
        # ✅ NUEVO: Mostrar sample de datos
        print(f"🔍 SAMPLE DE {tabla}:")
        df.show(3, truncate=False)
        
        return True

    except Exception as e:
        print(f"❌ Error en {tabla}: {str(e)}")
        return False    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución y Resumen de Ingesta

# COMMAND ----------

# Ejecutar ingesta para tablas de hechos
resultados = []
for tabla in tablas_hechos:
    resultado = ingestar_csv_a_bronze(tabla)
    resultados.append((tabla, resultado))

# COMMAND ----------

# Resumen de ingesta
print("\n📊 RESUMEN DE INGESTA - HECHOS/PAGOS:")
for tabla, estado in resultados:
    icon = "✅" if estado else "❌"
    print(f"{icon} {tabla}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VERIFICACIÓN ESPECÍFICA DE HECHOS/PAGOS

# COMMAND ----------

print("\n" + "="*60)
print("🔍 VERIFICACIÓN ESPECÍFICA DE HECHOS/PAGOS")
print("="*60)

# Verificar pfimapp_reporteeconomico
print("\n📋 VERIFICACIÓN pfimapp_reporteeconomico:")
try:
    verif_reporte = spark.sql(f"""
        SELECT 
            COUNT(*) as total,
            COUNT(idAlumno) as con_idAlumno,
            COUNT(alumno_id) as con_alumno_id,
            COUNT(idConceptoPago) as con_idConceptoPago,
            COUNT(idPeriodo) as con_idPeriodo,
            COUNT(monto) as con_monto,
            COUNT(fechaPago) as con_fechaPago
        FROM {catalogo}.bronze.pfimapp_reporteeconomico
    """).collect()[0]
    
    print(f"📊 Total: {verif_reporte['total']}")
    print(f"🔹 idAlumno: {verif_reporte['con_idAlumno']}")
    print(f"🔹 alumno_id: {verif_reporte['con_alumno_id']}")
    print(f"🔹 idConceptoPago: {verif_reporte['con_idConceptoPago']}")
    print(f"🔹 idPeriodo: {verif_reporte['con_idPeriodo']}")
    print(f"🔹 monto: {verif_reporte['con_monto']}")
    print(f"🔹 fechaPago: {verif_reporte['con_fechaPago']}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")

# COMMAND ----------

# Verificar pfimapp_reporteecoconceptopago
print("\n📋 VERIFICACIÓN pfimapp_reporteecoconceptopago:")
try:
    verif_concepto = spark.sql(f"""
        SELECT 
            COUNT(*) as total,
            COUNT(idReporteEconomico) as con_idReporteEconomico,
            COUNT(reporteEconomico_id) as con_reporteEconomico_id,
            COUNT(idConceptoPago) as con_idConceptoPago,
            COUNT(conceptoPago_id) as con_conceptoPago_id,
            COUNT(monto) as con_monto,
            COUNT(fechaPago) as con_fechaPago
        FROM {catalogo}.bronze.pfimapp_reporteecoconceptopago
    """).collect()[0]
    
    print(f"📊 Total: {verif_concepto['total']}")
    print(f"🔹 idReporteEconomico: {verif_concepto['con_idReporteEconomico']}")
    print(f"🔹 reporteEconomico_id: {verif_concepto['con_reporteEconomico_id']}")
    print(f"🔹 idConceptoPago: {verif_concepto['con_idConceptoPago']}")
    print(f"🔹 conceptoPago_id: {verif_concepto['con_conceptoPago_id']}")
    print(f"🔹 monto: {verif_concepto['con_monto']}")
    print(f"🔹 fechaPago: {verif_concepto['con_fechaPago']}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VERIFICACIÓN RELACIÓN MAESTRA-DETALLE

# COMMAND ----------

print("\n" + "="*60)
print("🔍 VERIFICANDO RELACIÓN MAESTRA-DETALLE")
print("="*60)

try:
    relacion = spark.sql(f"""
        SELECT 
            -- Tabla maestra
            COUNT(DISTINCT re.id) as reportes_maestra,
            COUNT(DISTINCT re.alumno_id) as alumnos_en_maestra,
            
            -- Tabla detalle  
            COUNT(DISTINCT rec.reporteEconomico_id) as reportes_en_detalle,
            COUNT(DISTINCT rec.id) as registros_detalle,
            
            -- Relación entre ambas
            COUNT(DISTINCT CASE WHEN rec.reporteEconomico_id IS NOT NULL THEN re.id END) as reportes_con_detalle,
            COUNT(DISTINCT CASE WHEN re.id IS NOT NULL THEN rec.reporteEconomico_id END) as detalles_con_maestra
        FROM {catalogo}.bronze.pfimapp_reporteeconomico re
        FULL OUTER JOIN {catalogo}.bronze.pfimapp_reporteecoconceptopago rec 
          ON re.id = rec.reporteEconomico_id
    """).collect()[0]
    
    print(f"📊 MAESTRA - Reportes únicos: {relacion['reportes_maestra']}")
    print(f"🎓 MAESTRA - Alumnos relacionados: {relacion['alumnos_en_maestra']}")
    print(f"💰 DETALLE - Reportes en detalle: {relacion['reportes_en_detalle']}")
    print(f"📈 DETALLE - Registros detalle: {relacion['registros_detalle']}")
    print(f"🔗 Reportes con detalle: {relacion['reportes_con_detalle']}")
    print(f"🔗 Detalles con maestra: {relacion['detalles_con_maestra']}")
    
    # Verificar si hay reportes sin detalle o detalles sin maestra
    problemas = spark.sql(f"""
        SELECT 
            COUNT(CASE WHEN rec.reporteEconomico_id IS NULL THEN 1 END) as detalles_sin_maestra,
            COUNT(CASE WHEN re.id IS NULL THEN 1 END) as reportes_sin_detalle
        FROM {catalogo}.bronze.pfimapp_reporteeconomico re
        FULL OUTER JOIN {catalogo}.bronze.pfimapp_reporteecoconceptopago rec 
          ON re.id = rec.reporteEconomico_id
    """).collect()[0]
    
    print(f"⚠️  Detalles sin maestra: {problemas['detalles_sin_maestra']}")
    print(f"⚠️  Reportes sin detalle: {problemas['reportes_sin_detalle']}")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIAGNÓSTICO Y RECOMENDACIONES

# COMMAND ----------

print("\n" + "="*60)
print("🎯 DIAGNÓSTICO Y RECOMENDACIONES")
print("="*60)

print("\n📋 PARA TRANSFORMACIONES USAR:")
print("pfimapp_reporteeconomico (MAESTRA):")
print("   - alumno_id (para relación con alumnos)")

print("\npfimapp_reporteecoconceptopago (DETALLE):")
print("   - reporteEconomico_id (para JOIN con maestra)")
print("   - conceptoPago_id (para JOIN con conceptos)")
print("   - periodo_id (para JOIN con períodos)")
print("   - monto ✅")
print("   - fechaPago ✅")

print("\n🔗 RELACIÓN MAESTRA-DETALLE:")
print("   pfimapp_alumno → pfimapp_reporteeconomico → pfimapp_reporteecoconceptopago")

print("\n🎯 SIGUIENTE PASO: En 6_Transform_Bronze_Silver_FIM usar:")
print("""
SELECT
    a.id as id_alumno,
    re.id as id_reporte,
    rec.conceptoPago_id,
    rec.periodo_id, 
    rec.monto,
    rec.fechaPago
FROM catalog_fim.bronze.pfimapp_alumno a
INNER JOIN catalog_fim.bronze.pfimapp_reporteeconomico re ON a.id = re.alumno_id
INNER JOIN catalog_fim.bronze.pfimapp_reporteecoconceptopago rec ON re.id = rec.reporteEconomico_id
""")

print("="*60)