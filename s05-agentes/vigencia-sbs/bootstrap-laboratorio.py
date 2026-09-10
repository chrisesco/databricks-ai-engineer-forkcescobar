# Databricks notebook source
# MAGIC %md
# MAGIC # S05 · Bootstrap del laboratorio
# MAGIC
# MAGIC Este notebook es el único lugar donde se cargan fixtures pequeños para la clase. En producción
# MAGIC lo reemplaza Auto Loader + `ai_parse_document` sobre un Volume. El procesador principal nunca
# MAGIC contiene una lista fija de documentos: lee `documentos_pendientes`.

# COMMAND ----------

import re
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "", "Tu catálogo de S01–S02")
CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
SCHEMA = "s05_vigencia_sbs"
assert re.fullmatch(r"[a-z][a-z0-9_]{1,127}", CATALOGO)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixture controlado
# MAGIC
# MAGIC Se simula que tres snapshots llegaron desde la fuente SBS. `content_hash` representa el hash
# MAGIC del archivo capturado; en producción se calcula desde el PDF, no se escribe a mano.

# COMMAND ----------

documentos = [
    ("snap-272-v30", "272-2017", 30, "Resolución SBS N.° 272-2017", "vigente_historica", "SF, SS", "2025-05-12", "2025-05-12", "hash-demo-272-v30", "pendiente", "fixture_didactico", "https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/VerHistorial.aspx?NormaId=1708"),
    ("snap-272-v31", "272-2017", 31, "Resolución SBS N.° 272-2017", "vigente", "SF, SS", "2026-08-04", "2026-08-04", "hash-demo-272-v31", "pendiente", "fixture_didactico", "https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/VerHistorial.aspx?NormaId=1708"),
    ("snap-1959-v1", "1959-2026", 1, "Resolución SBS N.° 1959-2026", "vigente", "SF, SS", "2026-08-04", "2026-08-03", "hash-demo-1959-v1", "pendiente", "fuente_metadata", "https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/BusquedaPortal.aspx"),
]
schema_docs = "doc_id string, norma_id string, version int, titulo string, estado string, sistemas string, fecha_publicacion string, fecha_emision string, content_hash string, estado_proceso string, tipo_fuente string, fuente_url string"
df_docs = spark.createDataFrame(documentos, schema_docs)
df_docs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.documentos_pendientes")

articulos = [
    ("snap-272-v30", "272-2017", 30, "art-23", "Gestión integral de riesgos", "La empresa establece responsabilidades y reporta la gestión integral de riesgos al directorio.", "fixture_didactico"),
    ("snap-272-v30", "272-2017", 30, "art-24", "Comité de riesgos", "El comité revisa periódicamente los riesgos relevantes y deja constancia de sus acuerdos.", "fixture_didactico"),
    ("snap-272-v30", "272-2017", 30, "art-25", "Información", "La información de riesgos se conserva y se pone a disposición de los órganos de gobierno.", "fixture_didactico"),
    ("snap-272-v31", "272-2017", 31, "art-23", "Gestión integral de riesgos", "La empresa establece responsabilidades, reporta la gestión integral de riesgos y documenta los indicadores mínimos al directorio.", "fixture_didactico"),
    ("snap-272-v31", "272-2017", 31, "art-24", "Comité de riesgos", "El comité revisa mensualmente los riesgos relevantes, registra acuerdos y asigna responsables de seguimiento.", "fixture_didactico"),
    ("snap-272-v31", "272-2017", 31, "art-26", "Trazabilidad", "La empresa conserva evidencia de decisiones, responsables y fecha de revisión para cada riesgo material.", "fixture_didactico"),
    ("snap-1959-v1", "1959-2026", 1, "art-1", "Modificación puntual", "Modifica los artículos 1 y 2 del Reglamento de Gobierno Corporativo y de la Gestión Integral de Riesgos.", "fuente_metadata"),
]
schema_art = "doc_id string, norma_id string, version int, articulo string, titulo string, texto string, tipo_fuente string"
df_art = spark.createDataFrame(articulos, schema_art)
df_art.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.articulos_fuente")
print(f"✅ Bootstrap listo: {df_docs.count()} snapshots y {df_art.count()} artículos pendientes")
