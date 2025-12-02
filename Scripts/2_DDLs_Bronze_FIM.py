# Databricks notebook source
# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CONFIGURACIÓN INICIAL - TABLAS BRONZE FIM
# MAGIC ### =====================================================

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageLocation default ".";
# MAGIC create widget text catalogo default ".";

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalogo};
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalogo}.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalogo}

# COMMAND ----------

# MAGIC %md
# MAGIC ### =====================================================
# MAGIC ### CREACIÓN DE TABLAS BRONZE
# MAGIC ### =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas de Parámetros y Maestras

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_estadocivil (
# MAGIC id INT,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_estadocivil';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_tipodocumento (
# MAGIC id INT,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_tipodocumento';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas Transaccionales y de Negocio

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_user (
# MAGIC id INT,
# MAGIC monoradidad STRING,
# MAGIC idTipoDocumento INT,
# MAGIC numeroDocumento STRING,
# MAGIC numeroLibretaMilitar STRING,
# MAGIC direccion STRING,
# MAGIC primerNombre STRING,
# MAGIC segundoNombre STRING,
# MAGIC apellidoPaterno STRING,
# MAGIC apellidoMaterno STRING,
# MAGIC idEstadoCivil INT,
# MAGIC actual STRING,
# MAGIC correoUtil STRING,
# MAGIC telefono STRING,
# MAGIC fechaNacimiento DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_user';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_alumno (
# MAGIC id INT,
# MAGIC idUsuario INT,
# MAGIC idMaestria INT,
# MAGIC idPeriodoIngreso INT,
# MAGIC codigoUniProgresivo STRING,
# MAGIC idPeriodoPEN INT,
# MAGIC idEstadoAcademico INT,
# MAGIC idSede INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_alumno';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_maestria (
# MAGIC id INT,
# MAGIC codigo STRING,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_maestria';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_estadoacademico (
# MAGIC id INT,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_estadoacademico';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_periodo (
# MAGIC id INT,
# MAGIC codigo STRING,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_periodo';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_sede (
# MAGIC id INT,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_sede';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_reporteeconomico (
# MAGIC id INT,
# MAGIC idAlumno INT,
# MAGIC idConceptoPago INT,
# MAGIC idPeriodo INT,
# MAGIC monto DOUBLE,
# MAGIC numeroBoleta STRING,
# MAGIC fechaPago DATE,
# MAGIC idEstadoBoletaP INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_reporteeconomico';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_reporteecoconceptopago (
# MAGIC id INT,
# MAGIC idReporteEconomico INT,
# MAGIC idConceptoPago INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_reporteecoconceptopago';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_estadoboletap (
# MAGIC id INT,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_estadoboletap';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_fim.bronze.pfimapp_conceptopago (
# MAGIC id INT,
# MAGIC nombre STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/pfimapp_conceptopago';