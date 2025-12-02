# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### TRANSFORMACIÓN - BRONZE A SILVER
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text catalogo default ".";

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### TRANSFORMACIÓN DE DATOS - ENRIQUECIMIENTO SILVER
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Vista Consolidada de Alumnos

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE catalog_fim.silver.alumnos_completos
# MAGIC SELECT
# MAGIC     a.id as id_alumno,
# MAGIC     TRY_CAST(a.usuario_id AS INT) as id_usuario,
# MAGIC     u.primerNombre,
# MAGIC     COALESCE(u.segundoNombre, '') as segundoNombre,
# MAGIC     u.apellidoPaterno,
# MAGIC     u.apellidoMaterno,
# MAGIC     u.numeroDocumento as dni,
# MAGIC     LOWER(TRIM(u.correoUNI)) as email,
# MAGIC     u.telefono,
# MAGIC     u.direccion,
# MAGIC     -- SOLUCIÓN CON TRY_CAST
# MAGIC     COALESCE(TRY_CAST(u.fechaNacimiento AS DATE), DATE('1900-01-01')) as fechaNacimiento,
# MAGIC     ec.nombre as estado_civil,
# MAGIC     td.nombre as tipo_documento,
# MAGIC     TRY_CAST(a.maestria_id AS INT) as idMaestria,
# MAGIC     m.nombre as maestria,
# MAGIC     TRY_CAST(a.estadoAcademico_id AS INT) as idEstadoAcademico,
# MAGIC     ea.nombre as estado_academico,
# MAGIC     TRY_CAST(a.sede_id AS INT) as idSede,
# MAGIC     s.nombre as sede,
# MAGIC     a.codigoUniProgresivo,
# MAGIC     TRY_CAST(a.periodoDeIngreso_id AS INT) as idPeriodoIngreso
# MAGIC FROM catalog_fim.bronze.pfimapp_alumno a
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_user u ON TRY_CAST(a.usuario_id AS INT) = u.id
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_estadocivil ec ON u.estadoCivil_id = ec.id
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_tipodocumento td ON u.tipoDocumento_id = td.id
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_maestria m ON TRY_CAST(a.maestria_id AS INT) = m.id
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_estadoacademico ea ON TRY_CAST(a.estadoAcademico_id AS INT) = ea.id
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_sede s ON TRY_CAST(a.sede_id AS INT) = s.id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Validación y Enriquecimiento de Pagos (CON NOMBRES CORRECTOS)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE catalog_fim.silver.pagos_validados
# MAGIC SELECT
# MAGIC     rec.id,
# MAGIC     re.alumno_id as idAlumno,
# MAGIC     rec.conceptoPago_id as idConceptoPago,
# MAGIC     cp.nombre as concepto_pago,
# MAGIC     rec.periodo_id as idPeriodo,
# MAGIC     p.codigo as periodo_codigo,
# MAGIC     p.nombre as periodo_nombre,
# MAGIC     -- SOLUCIÓN MÁS SEGURA: TRY_CAST
# MAGIC     COALESCE(TRY_CAST(rec.monto AS DOUBLE), 0.0) as monto,
# MAGIC     rec.numeroRecibo as numeroBoleta,
# MAGIC     TRY_CAST(rec.fechaPago AS DATE) as fecha_pago,
# MAGIC     eb.nombre as estado_boleta,
# MAGIC     -- LÓGICA SIMPLIFICADA CON TRY_CAST
# MAGIC     CASE
# MAGIC         WHEN COALESCE(TRY_CAST(rec.monto AS DOUBLE), 0.0) > 0 
# MAGIC              AND TRY_CAST(rec.fechaPago AS DATE) IS NOT NULL THEN true
# MAGIC         ELSE false
# MAGIC     END as pago_valido,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(TRY_CAST(rec.monto AS DOUBLE), 0.0) <= 0 THEN 'MONTO_INVALIDO'
# MAGIC         WHEN TRY_CAST(rec.fechaPago AS DATE) IS NULL THEN 'FECHA_PAGO_NULA'
# MAGIC         WHEN rec.numeroRecibo IS NULL THEN 'BOLETA_NULA'
# MAGIC         ELSE 'VALIDA'
# MAGIC     END as motivo_validacion
# MAGIC FROM catalog_fim.bronze.pfimapp_reporteecoconceptopago rec
# MAGIC INNER JOIN catalog_fim.bronze.pfimapp_reporteeconomico re ON rec.reporteEconomico_id = re.id
# MAGIC INNER JOIN catalog_fim.bronze.pfimapp_conceptopago cp ON rec.conceptoPago_id = cp.id
# MAGIC INNER JOIN catalog_fim.bronze.pfimapp_periodo p ON rec.periodo_id = p.id
# MAGIC LEFT JOIN catalog_fim.bronze.pfimapp_estadoboletap eb ON rec.estadoBoletaPago_id = eb.id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Combinación Maestrías-Periodos

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE catalog_fim.silver.maestrias_periodos
# MAGIC SELECT
# MAGIC     m.id as id_maestria,
# MAGIC     m.codigo as codigo_maestria,
# MAGIC     m.nombre as nombre_maestria,
# MAGIC     p.id as id_periodo,
# MAGIC     p.codigo as codigo_periodo,
# MAGIC     p.nombre as nombre_periodo,
# MAGIC     CASE
# MAGIC         WHEN UPPER(p.nombre) LIKE '%2024%' THEN true
# MAGIC         ELSE false
# MAGIC     END as periodo_activo
# MAGIC FROM catalog_fim.bronze.pfimapp_maestria m
# MAGIC CROSS JOIN catalog_fim.bronze.pfimapp_periodo p;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. VERIFICACIÓN DE TRANSFORMACIONES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar conteos después de transformación
# MAGIC SELECT 
# MAGIC     'alumnos_completos' as tabla,
# MAGIC     COUNT(*) as total_registros
# MAGIC FROM catalog_fim.silver.alumnos_completos
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'pagos_validados' as tabla,
# MAGIC     COUNT(*) as total_registros
# MAGIC FROM catalog_fim.silver.pagos_validados
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'maestrias_periodos' as tabla,
# MAGIC     COUNT(*) as total_registros
# MAGIC FROM catalog_fim.silver.maestrias_periodos;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. MUESTRA DE DATOS TRANSFORMADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample de alumnos transformados
# MAGIC SELECT * FROM catalog_fim.silver.alumnos_completos LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample de pagos transformados
# MAGIC SELECT * FROM catalog_fim.silver.pagos_validados LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. REPORTE DE CALIDAD

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reporte de calidad de datos en silver
# MAGIC SELECT 
# MAGIC     'alumnos_completos' as tabla,
# MAGIC     COUNT(CASE WHEN telefono IS NULL THEN 1 END) as telefonos_null,
# MAGIC     COUNT(CASE WHEN direccion IS NULL THEN 1 END) as direcciones_null,
# MAGIC     COUNT(CASE WHEN fechaNacimiento IS NULL THEN 1 END) as fechas_nacimiento_null
# MAGIC FROM catalog_fim.silver.alumnos_completos
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'pagos_validados' as tabla,
# MAGIC     COUNT(CASE WHEN monto <= 0 THEN 1 END) as montos_invalidos,
# MAGIC     COUNT(CASE WHEN fecha_pago IS NULL THEN 1 END) as fechas_pago_null,
# MAGIC     COUNT(CASE WHEN pago_valido = false THEN 1 END) as pagos_invalidos
# MAGIC FROM catalog_fim.silver.pagos_validados;

# COMMAND ----------

print("✅ TRANSFORMACIÓN BRONZE TO SILVER COMPLETADA")