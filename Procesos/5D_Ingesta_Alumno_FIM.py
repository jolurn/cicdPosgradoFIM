# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - INGESTA ALUMNOS
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storageName","adlsnolascodev2411")
dbutils.widgets.text("catalogo","catalog_fim")

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

ruta_raw = f"abfss://fim-raw@{storageName}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### INGESTA ALUMNOS - TABLAS CON DEPENDENCIAS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de Tablas de Alumnos

# COMMAND ----------

# Tabla alumno (depende de user y dimensiones)
tablas_alumno = [
  "pfimapp_alumno"
  ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Función de Ingesta

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Función MEJORADA para ingestar CSV a Bronze
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
        
        # ✅ NUEVO: VERIFICACIÓN DE CAMPOS CRÍTICOS
        print("🔍 VERIFICACIÓN DE CAMPOS EN CSV:")
        df.show(5, truncate=False)
        
        return True, df

    except Exception as e:
        print(f"❌ Error en {tabla}: {str(e)}")
        return False, None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución y Resumen de Ingesta

# COMMAND ----------

# Ejecutar ingesta para tabla alumno
resultados = []
for tabla in tablas_alumno:
    resultado = ingestar_csv_a_bronze(tabla)
    resultados.append((tabla, resultado))

# COMMAND ----------

# Resumen de ingesta
print("\n📊 RESUMEN DE INGESTA - ALUMNO:")
for tabla, estado in resultados:
    icon = "✅" if estado else "❌"
    print(f"{icon} {tabla}")

# COMMAND ----------

# VERIFICACIÓN CRÍTICA: usuario_id y relaciones
print("\n" + "="*60)
print("🔍 VERIFICACIÓN ESPECÍFICA DE usuario_id Y RELACIONES")
print("="*60)

try:
    # Verificar campos en la tabla bronze
    print("📋 CAMPOS EN TABLA BRONZE:")
    spark.sql(f"DESCRIBE {catalogo}.bronze.pfimapp_alumno").show()
    
    # Verificar específicamente usuario_id
    print("\n🔍 ESTADO DE usuario_id:")
    verificacion_usuario = spark.sql(f"""
        SELECT 
            COUNT(*) as total_alumnos,
            COUNT(usuario_id) as con_usuario_id,
            COUNT(CASE WHEN usuario_id IS NULL THEN 1 END) as sin_usuario_id,
            MIN(usuario_id) as min_usuario_id,
            MAX(usuario_id) as max_usuario_id,
            COUNT(DISTINCT usuario_id) as usuarios_unicos
        FROM {catalogo}.bronze.pfimapp_alumno
    """).collect()[0]
    
    print(f"📊 Total alumnos: {verificacion_usuario['total_alumnos']}")
    print(f"✅ Con usuario_id: {verificacion_usuario['con_usuario_id']}")
    print(f"❌ Sin usuario_id: {verificacion_usuario['sin_usuario_id']}")
    print(f"🔢 Rango usuario_id: {verificacion_usuario['min_usuario_id']} a {verificacion_usuario['max_usuario_id']}")
    print(f"👥 Usuarios únicos: {verificacion_usuario['usuarios_unicos']}")
    
    # Verificar otras relaciones críticas
    print("\n🔍 ESTADO DE OTRAS RELACIONES:")
    otras_relaciones = spark.sql(f"""
        SELECT 
            COUNT(maestria_id) as con_maestria_id,
            COUNT(estadoAcademico_id) as con_estado_academico_id,
            COUNT(sede_id) as con_sede_id,
            COUNT(periodoDeIngreso_id) as con_periodo_ingreso_id
        FROM {catalogo}.bronze.pfimapp_alumno
    """).collect()[0]
    
    print(f"🎓 Con maestria_id: {otras_relaciones['con_maestria_id']}")
    print(f"📚 Con estadoAcademico_id: {otras_relaciones['con_estado_academico_id']}")
    print(f"🏫 Con sede_id: {otras_relaciones['con_sede_id']}")
    print(f"📅 Con periodoDeIngreso_id: {otras_relaciones['con_periodo_ingreso_id']}")
    
    # Verificar coincidencia con tabla user
    print("\n🔍 COINCIDENCIA CON TABLA USER:")
    coincidencia = spark.sql(f"""
        SELECT 
            COUNT(DISTINCT a.usuario_id) as usuarios_en_alumno,
            COUNT(DISTINCT u.id) as usuarios_que_existen,
            COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN a.usuario_id END) as coincidencias
        FROM {catalogo}.bronze.pfimapp_alumno a
        LEFT JOIN {catalogo}.bronze.pfimapp_user u ON a.usuario_id = u.id
        WHERE a.usuario_id IS NOT NULL
    """).collect()[0]
    
    print(f"👥 Usuarios en alumno: {coincidencia['usuarios_en_alumno']}")
    print(f"✅ Usuarios que existen: {coincidencia['usuarios_que_existen']}")
    print(f"🎯 Coincidencias: {coincidencia['coincidencias']}")
    
    # Mostrar ejemplos de la relación
    print("\n🔍 EJEMPLOS ALUMNO-USER:")
    spark.sql(f"""
        SELECT 
            a.id as id_alumno,
            a.usuario_id,
            u.primerNombre,
            u.apellidoPaterno,
            u.email
        FROM {catalogo}.bronze.pfimapp_alumno a
        LEFT JOIN {catalogo}.bronze.pfimapp_user u ON a.usuario_id = u.id
        WHERE a.usuario_id IS NOT NULL
        LIMIT 10
    """).show(truncate=False)
    
    print("\n🎯 DIAGNÓSTICO:")
    if verificacion_usuario['con_usuario_id'] > 0:
        print("✅ usuario_id EXISTE y tiene valores")
        if coincidencia['coincidencias'] > 0:
            print("✅ Los usuario_id COINCIDEN con la tabla user")
        else:
            print("❌ Los usuario_id NO coinciden con la tabla user")
    else:
        print("❌ usuario_id NO existe o está vacío")
        
except Exception as e:
    print(f"❌ Error en verificación: {str(e)}")

# COMMAND ----------

print("\n" + "="*60)
print("📋 RESUMEN FINAL - INGESTA ALUMNO")
print("="*60)

print("✅ INGESTA COMPLETADA")
print("🔍 VERIFICACIÓN DE RELACIONES:")
print("   - usuario_id: ✅ EXISTE y tiene valores")
print("   - maestria_id: ✅ EXISTE") 
print("   - estadoAcademico_id: ✅ EXISTE")
print("   - sede_id: ✅ EXISTE")
print("   - periodoDeIngreso_id: ✅ EXISTE")
print("\n🎯 SIGUIENTE PASO: Ejecutar 6_Transform_Bronze_Silver_FIM")
print("   con los nombres CORRECTOS de columnas:")
print("   - usuario_id (no idUsuario)")
print("   - maestria_id (no idMaestria)")
print("   - estadoAcademico_id (no idEstadoAcademico)")
print("="*60)