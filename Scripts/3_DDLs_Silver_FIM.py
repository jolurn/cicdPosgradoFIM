# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - CAPA SILVER FIM
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageLocation default ".";
# MAGIC create widget text catalogo default ".";

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalogo}.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalogo}

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CREACIÓN DE TABLAS SILVER - VISTAS ENRIQUECIDAS
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla: Vista Consolidada de Alumnos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.silver.alumnos_completos (
# MAGIC id_alumno INT,
# MAGIC id_usuario INT,
# MAGIC primerNombre STRING,
# MAGIC segundoNombre STRING,
# MAGIC apellidoPaterno STRING,
# MAGIC apellidoMaterno STRING,
# MAGIC dni STRING,
# MAGIC email STRING,
# MAGIC telefono STRING,
# MAGIC direccion STRING,
# MAGIC fechaNacimiento DATE,
# MAGIC estado_civil STRING,
# MAGIC tipo_documento STRING,
# MAGIC idMaestria INT,
# MAGIC maestria STRING,
# MAGIC idEstadoAcademico INT,
# MAGIC estado_academico STRING,
# MAGIC idSede INT,
# MAGIC sede STRING,
# MAGIC codigoUniProgresivo STRING,
# MAGIC idPeriodoIngreso INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/alumnos_completos';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla: Pagos Validados y Normalizados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.silver.pagos_validados (
# MAGIC id INT,
# MAGIC idAlumno INT,
# MAGIC idConceptoPago INT,
# MAGIC concepto_pago STRING,
# MAGIC idPeriodo INT,
# MAGIC periodo_codigo STRING,
# MAGIC periodo_nombre STRING,
# MAGIC monto DOUBLE,
# MAGIC numeroBoleta STRING,
# MAGIC fecha_pago DATE,
# MAGIC estado_boleta STRING,
# MAGIC pago_valido BOOLEAN,
# MAGIC motivo_validacion STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pagos_validados';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla: Maestrías y Periodos Asociados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.silver.maestrias_periodos (
# MAGIC id_maestria INT,
# MAGIC codigo_maestria STRING,
# MAGIC nombre_maestria STRING,
# MAGIC id_periodo INT,
# MAGIC codigo_periodo STRING,
# MAGIC nombre_periodo STRING,
# MAGIC periodo_activo BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/maestrias_periodos';