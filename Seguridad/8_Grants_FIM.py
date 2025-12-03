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
# MAGIC ### 1. PERMISOS PARA GRUPOS (lo que ya tenías)
# MAGIC ### =====================================================

# COMMAND ----------

print("🏢 CONFIGURANDO PERMISOS PARA GRUPOS...")

# COMMAND ----------

# Permisos de catálogo para grupos
spark.sql("GRANT USE CATALOG ON CATALOG catalog_fim TO `academico`;")
spark.sql("GRANT USE CATALOG ON CATALOG catalog_fim TO `financiera`;")

# Permisos en Bronze (académico tiene escritura)
spark.sql("GRANT USE SCHEMA ON SCHEMA catalog_fim.bronze TO `academico`;")
spark.sql("GRANT CREATE ON SCHEMA catalog_fim.bronze TO `academico`;")
spark.sql("GRANT SELECT ON SCHEMA catalog_fim.bronze TO `academico`;")

# Permisos en Silver (lectura para ambos grupos)
spark.sql("GRANT USE SCHEMA ON SCHEMA catalog_fim.silver TO `academico`;")
spark.sql("GRANT SELECT ON SCHEMA catalog_fim.silver TO `academico`;")
spark.sql("GRANT USE SCHEMA ON SCHEMA catalog_fim.silver TO `financiera`;")
spark.sql("GRANT SELECT ON SCHEMA catalog_fim.silver TO `financiera`;")

# Permisos en Golden (lectura para ambos grupos)
spark.sql("GRANT USE SCHEMA ON SCHEMA catalog_fim.golden TO `academico`;")
spark.sql("GRANT SELECT ON SCHEMA catalog_fim.golden TO `academico`;")
spark.sql("GRANT USE SCHEMA ON SCHEMA catalog_fim.golden TO `financiera`;")
spark.sql("GRANT SELECT ON SCHEMA catalog_fim.golden TO `financiera`;")

print("✅ Permisos para grupos configurados")

# COMMAND ----------

# MAGIC %md
# MAGIC # ============================================================================
# MAGIC # 2. PERMISOS PARA USUARIOS INDIVIDUALES
# MAGIC # ============================================================================

# COMMAND ----------

print("\n👤 CONFIGURANDO PERMISOS PARA USUARIOS INDIVIDUALES...")

# Lista de usuarios inventados para académicos
usuarios_academicos = [
    "ana.garcia@universidad.edu.pe",       # Directora Académica
    "carlos.mendoza@universidad.edu.pe",   # Coordinador de Maestrías
    "lucia.fernandez@universidad.edu.pe",  # Asesora Académica
    "miguel.torres@universidad.edu.pe",    # Jefe de Admisiones
    "sofia.rojas@universidad.edu.pe"       # Analista de Datos Académicos
]

# COMMAND ----------

# Lista de usuarios inventados para finanzas
usuarios_financieros = [
    "juan.perez@universidad.edu.pe",       # Gerente Financiero
    "maria.lopez@universidad.edu.pe",      # Contadora General
    "roberto.santos@universidad.edu.pe",   # Analista de Cobranza
    "claudia.gutierrez@universidad.edu.pe", # Auditora Interna
    "diego.castro@universidad.edu.pe"      # Jefe de Tesorería
]

# COMMAND ----------

# Lista de usuarios administradores/gestores
usuarios_administradores = [
    "admin@universidad.edu.pe",            # Administrador Principal
    "datascience@universidad.edu.pe",      # Equipo Data Science
    "bi@universidad.edu.pe",               # Equipo Business Intelligence
    "rector@universidad.edu.pe"            # Rectoría
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### ===========================================
# MAGIC ### 2.1 PERMISOS PARA USUARIOS ACADÉMICOS
# MAGIC ### ===========================================

# COMMAND ----------

print("\n🎓 Otorgando permisos a usuarios académicos...")
for usuario in usuarios_academicos:
    try:
        # Catálogo
        spark.sql(f"GRANT USE CATALOG ON CATALOG catalog_fim TO `{usuario}`;")
        
        # Bronze (solo lectura para usuarios individuales)
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.bronze TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON SCHEMA catalog_fim.bronze TO `{usuario}`;")
        
        # Silver (lectura)
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.silver TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON SCHEMA catalog_fim.silver TO `{usuario}`;")
        
        # Golden (lectura)
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.golden TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON SCHEMA catalog_fim.golden TO `{usuario}`;")
        
        print(f"  ✅ {usuario}")
    except Exception as e:
        print(f"  ⚠️  Error con {usuario}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### 2.2 PERMISOS PARA USUARIOS FINANCIEROS
# MAGIC ### =====================================================

# COMMAND ----------

print("\n💰 Otorgando permisos a usuarios financieros...")
for usuario in usuarios_financieros:
    try:
        # Catálogo
        spark.sql(f"GRANT USE CATALOG ON CATALOG catalog_fim TO `{usuario}`;")
        
        # Bronze (NO acceso para finanzas - solo datos procesados)
        # spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.bronze TO `{usuario}`;")  # Comentado: sin acceso a raw data
        
        # Silver (lectura completa)
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.silver TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON SCHEMA catalog_fim.silver TO `{usuario}`;")
        
        # Golden (lectura completa)
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.golden TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON SCHEMA catalog_fim.golden TO `{usuario}`;")
        
        # Permisos específicos a tablas importantes para finanzas
        spark.sql(f"GRANT SELECT ON TABLE catalog_fim.silver.pagos_validados TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON TABLE catalog_fim.golden.kpi_pagos_periodo TO `{usuario}`;")
        spark.sql(f"GRANT SELECT ON TABLE catalog_fim.golden.kpi_maestrias_rentables TO `{usuario}`;")
        
        print(f"  ✅ {usuario}")
    except Exception as e:
        print(f"  ⚠️  Error con {usuario}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ============================================================================
# MAGIC ## 2.3 PERMISOS PARA ADMINISTRADORES
# MAGIC ## ============================================================================

# COMMAND ----------

print("\n👑 Otorgando permisos a administradores...")
for usuario in usuarios_administradores:
    try:
        # Catálogo completo
        spark.sql(f"GRANT USE CATALOG ON CATALOG catalog_fim TO `{usuario}`;")
        spark.sql(f"GRANT CREATE CATALOG ON CATALOG catalog_fim TO `{usuario}`;")
        
        # Todos los schemas con todos los permisos
        for schema in ["bronze", "silver", "golden"]:
            spark.sql(f"GRANT USE SCHEMA ON SCHEMA catalog_fim.{schema} TO `{usuario}`;")
            spark.sql(f"GRANT CREATE ON SCHEMA catalog_fim.{schema} TO `{usuario}`;")
            spark.sql(f"GRANT SELECT ON SCHEMA catalog_fim.{schema} TO `{usuario}`;")
            spark.sql(f"GRANT MODIFY ON SCHEMA catalog_fim.{schema} TO `{usuario}`;")
        
        print(f"  ✅ {usuario} (permisos completos)")
    except Exception as e:
        print(f"  ⚠️  Error con {usuario}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ==============================================
# MAGIC ### 3. PERMISOS ESPECÍFICOS A TABLAS IMPORTANTES
# MAGIC ### ==============================================

# COMMAND ----------

print("\n📊 CONFIGURANDO PERMISOS ESPECÍFICOS POR TABLA...")

# Tablas importantes para diferentes roles
tablas_importantes = {
    "académicas": [
        "catalog_fim.silver.alumnos_completos",
        "catalog_fim.silver.maestrias_periodos",
        "catalog_fim.golden.kpi_estado_alumnos_pagos"
    ],
    "financieras": [
        "catalog_fim.silver.pagos_validados",
        "catalog_fim.golden.kpi_pagos_periodo",
        "catalog_fim.golden.kpi_maestrias_rentables"
    ],
    "gestión": [
        "catalog_fim.golden.kpi_generales",
        "catalog_fim.golden.kpi_por_maestria",
        "catalog_fim.golden.kpi_diario"
    ]
}

# COMMAND ----------

# Otorgar permisos específicos por tabla
print("\n🔐 Permisos para tablas académicas:")
for tabla in tablas_importantes["académicas"]:
    try:
        # Para todos los usuarios académicos
        for usuario in usuarios_academicos + ["academico"]:
            spark.sql(f"GRANT SELECT ON TABLE {tabla} TO `{usuario}`;")
        print(f"  ✅ {tabla.split('.')[-1]}")
    except Exception as e:
        print(f"  ⚠️  Error con {tabla}: {str(e)[:100]}")

print("\n💳 Permisos para tablas financieras:")
for tabla in tablas_importantes["financieras"]:
    try:
        # Para todos los usuarios financieros
        for usuario in usuarios_financieros + ["financiera"]:
            spark.sql(f"GRANT SELECT ON TABLE {tabla} TO `{usuario}`;")
        print(f"  ✅ {tabla.split('.')[-1]}")
    except Exception as e:
        print(f"  ⚠️  Error con {tabla}: {str(e)[:100]}")

print("\n📈 Permisos para tablas de gestión:")
for tabla in tablas_importantes["gestión"]:
    try:
        # Para todos los usuarios (académicos, financieros y administradores)
        todos_usuarios = usuarios_academicos + usuarios_financieros + usuarios_administradores + ["academico", "financiera"]
        for usuario in todos_usuarios:
            spark.sql(f"GRANT SELECT ON TABLE {tabla} TO `{usuario}`;")
        print(f"  ✅ {tabla.split('.')[-1]}")
    except Exception as e:
        print(f"  ⚠️  Error con {tabla}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### =======================================
# MAGIC ### 4. VERIFICACIÓN DE PERMISOS
# MAGIC ### =======================================

# COMMAND ----------

print("\n" + "="*70)
print("🔍 VERIFICANDO PERMISOS CONFIGURADOS")
print("="*70)

# COMMAND ----------

# Verificar permisos en diferentes niveles
verificaciones = [
    ("CATALOGO catalog_fim", "SHOW GRANTS ON CATALOG catalog_fim;"),
    ("SCHEMA catalog_fim.bronze", "SHOW GRANTS ON SCHEMA catalog_fim.bronze;"),
    ("SCHEMA catalog_fim.silver", "SHOW GRANTS ON SCHEMA catalog_fim.silver;"),
    ("SCHEMA catalog_fim.golden", "SHOW GRANTS ON SCHEMA catalog_fim.golden;"),
    ("TABLA KPI pagos periodo", "SHOW GRANTS ON TABLE catalog_fim.golden.kpi_pagos_periodo;"),
    ("TABLA alumnos completos", "SHOW GRANTS ON TABLE catalog_fim.silver.alumnos_completos;")
]

for descripcion, consulta in verificaciones:
    try:
        print(f"\n📋 {descripcion}:")
        df = spark.sql(consulta)
        df.show(truncate=False)
    except Exception as e:
        print(f"  ❌ Error en {descripcion}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ===================================
# MAGIC ### 5. RESUMEN DE CONFIGURACIÓN
# MAGIC ### ===================================

# COMMAND ----------

print("\n" + "="*70)
print("📋 RESUMEN DE CONFIGURACIÓN DE PERMISOS")
print("="*70)

print(f"""
🎓 USUARIOS ACADÉMICOS ({len(usuarios_academicos)} usuarios):
   • Acceso: Bronze (lectura), Silver (lectura), Golden (lectura)
   • Tablas clave: alumnos_completos, maestrias_periodos
   • Usuarios: {', '.join(usuarios_academicos[:3])}...

💰 USUARIOS FINANCIEROS ({len(usuarios_financieros)} usuarios):
   • Acceso: Silver (lectura), Golden (lectura)
   • NO acceso: Bronze (datos crudos)
   • Tablas clave: pagos_validados, kpi_pagos_periodo
   • Usuarios: {', '.join(usuarios_financieros[:3])}...

👑 ADMINISTRADORES ({len(usuarios_administradores)} usuarios):
   • Acceso: COMPLETO en todos los niveles
   • Permisos: CREATE, MODIFY, SELECT en todo
   • Usuarios: {', '.join(usuarios_administradores)}

👥 GRUPOS:
   • académico: Acceso completo académico
   • financiera: Acceso solo a datos procesados

📊 TABLAS CON PERMISOS ESPECÍFICOS:
   • Académicas: {len(tablas_importantes['académicas'])} tablas
   • Financieras: {len(tablas_importantes['financieras'])} tablas  
   • Gestión: {len(tablas_importantes['gestión'])} tablas
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ==============================================
# MAGIC ### 6. SCRIPT PARA REVOCAR PERMISOS (POR SI ACASO)
# MAGIC ### ==============================================

# COMMAND ----------

print("\n" + "="*70)
print("⚠️  SCRIPT DE REVOCACIÓN (GUARDAR POR SI SE NECESITA)")
print("="*70)

print("""
-- Para revocar permisos de un usuario específico:
-- REVOKE ALL PRIVILEGES ON CATALOG catalog_fim FROM `usuario@email.com`;
-- REVOKE ALL PRIVILEGES ON SCHEMA catalog_fim.silver FROM `usuario@email.com`;
-- REVOKE ALL PRIVILEGES ON TABLE catalog_fim.silver.alumnos_completos FROM `usuario@email.com`;

-- Para listar todos los usuarios con permisos:
-- SHOW GRANTS ON CATALOG catalog_fim;
-- SHOW GRANTS ON SCHEMA catalog_fim.silver;
-- SHOW GRANTS ON TABLE catalog_fim.silver.alumnos_completos;
""")

print("\n✅ CONFIGURACIÓN DE PERMISOS COMPLETADA")
print("="*70)