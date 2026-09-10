# Databricks notebook source
# MAGIC %md
# MAGIC # S05 · Procesador dinámico de vigencia SBS
# MAGIC
# MAGIC Este notebook no contiene el corpus. Lee snapshots pendientes desde Delta, procesa cada
# MAGIC `doc_id`, calcula diferencias contra la versión anterior, indexa solo los cambios nuevos y
# MAGIC marca el trabajo como procesado. Puede ejecutarse solo o como tarea anidada de `For each`.

# COMMAND ----------

import json
import re
from datetime import datetime, timezone
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "", "Tu catálogo de S01–S02")
dbutils.widgets.text("doc_id", "", "Snapshot opcional; vacío procesa todos los pendientes")
dbutils.widgets.text("embedding_endpoint", "databricks-qwen3-embedding-0-6b", "Endpoint de embeddings")
CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
DOC_ID = dbutils.widgets.get("doc_id").strip()
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint").strip()
SCHEMA = "s05_vigencia_sbs"
assert re.fullmatch(r"[a-z][a-z0-9_]{1,127}", CATALOGO)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
endpoints_activos = [ep.name for ep in w.serving_endpoints.list()]
assert EMBEDDING_ENDPOINT in endpoints_activos, f"endpoint_missing: {EMBEDDING_ENDPOINT}"
print(f"✅ endpoint de embeddings disponible: {EMBEDDING_ENDPOINT}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Descubrir trabajo pendiente desde Delta

# COMMAND ----------

pendientes = spark.table(f"{CATALOGO}.{SCHEMA}.documentos_pendientes").filter("estado_proceso = 'pendiente'")
if DOC_ID:
    pendientes = pendientes.filter(F.col("doc_id") == DOC_ID)
pendientes = pendientes.orderBy("norma_id", "version")
ids = [r.doc_id for r in pendientes.select("doc_id").collect()]
print(f"📥 snapshots descubiertos: {ids}")
if not ids:
    print("✅ No hay trabajo pendiente; el workflow es idempotente.")
    dbutils.notebook.exit("NO_PENDING_WORK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Procesar solo los snapshots descubiertos

# COMMAND ----------

df_fuente = spark.table(f"{CATALOGO}.{SCHEMA}.articulos_fuente")
df_cambios_lote = None
procesados = []
for doc_id in ids:
    meta = pendientes.filter(F.col("doc_id") == doc_id).first()
    actual = df_fuente.filter(F.col("doc_id") == doc_id)
    anteriores = df_fuente.filter((F.col("norma_id") == meta.norma_id) & (F.col("version") < meta.version))
    version_anterior = anteriores.select(F.max("version")).first()[0]
    if version_anterior is None:
        cambios = actual.select(
            "norma_id", F.lit(None).cast("int").alias("version_anterior"), F.col("version").alias("version_nueva"),
            "articulo", F.lit(None).cast("string").alias("texto_anterior"), F.col("texto").alias("texto_nuevo")
        ).withColumn("tipo_cambio", F.lit("agregado"))
    else:
        old = anteriores.filter(F.col("version") == version_anterior).select("norma_id", "articulo", F.col("texto").alias("texto_anterior"))
        new = actual.select("norma_id", "articulo", F.col("texto").alias("texto_nuevo"))
        cambios = (old.join(new, ["norma_id", "articulo"], "full")
            .withColumn("version_anterior", F.lit(int(version_anterior)))
            .withColumn("version_nueva", F.lit(int(meta.version)))
            .withColumn("tipo_cambio", F.when(F.col("texto_anterior").isNull(), "agregado")
                        .when(F.col("texto_nuevo").isNull(), "eliminado")
                        .when(F.col("texto_anterior") != F.col("texto_nuevo"), "modificado")
                        .otherwise("sin_cambio")))
    cambios = (cambios.withColumn("doc_id", F.lit(doc_id))
        .withColumn("content_hash", F.lit(meta.content_hash))
        .withColumn("cambio_id", F.sha2(F.concat_ws("|", "doc_id", "norma_id", "articulo", "version_anterior", "version_nueva"), 256))
        .withColumn("texto_busqueda", F.concat_ws(" ", F.coalesce("articulo", F.lit("")), F.coalesce("texto_anterior", F.lit("")), F.coalesce("texto_nuevo", F.lit(""))))
        .filter("tipo_cambio != 'sin_cambio'"))
    df_cambios_lote = cambios if df_cambios_lote is None else df_cambios_lote.unionByName(cambios)
    procesados.append((doc_id, "procesado", datetime.now(timezone.utc)))

df_cambios_lote.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.cambios_normativos")
display(df_cambios_lote.orderBy("doc_id", "articulo"))
assert df_cambios_lote.count() >= 4, "diff_should_find_changes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Embeddings solo de cambios nuevos

# COMMAND ----------

df_embeddings = df_cambios_lote.withColumn("embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto_busqueda)"))
filas = df_embeddings.select("cambio_id", "embedding").collect()
assert filas and all(r.embedding is not None and len(r.embedding) == 1024 for r in filas), "embedding_dimension"
df_embeddings.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.cambios_embeddings")
print(f"✅ cambios indexados: {len(filas)} · dimensión: {len(filas[0].embedding)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Tools y agente supervisor lógico

# COMMAND ----------

ALLOWLIST = {"buscar_norma", "listar_versiones", "comparar_versiones", "corroborar"}
def buscar_norma(texto):
    t = texto.lower().replace("'", "''")
    return spark.sql(f"SELECT doc_id, norma_id, version, titulo, estado, fuente_url FROM {CATALOGO}.{SCHEMA}.documentos_pendientes WHERE lower(norma_id) LIKE '%{t}%' OR lower(titulo) LIKE '%{t}%' ORDER BY version DESC")
def listar_versiones(norma_id):
    safe = norma_id.replace("'", "''")
    return spark.sql(f"SELECT norma_id, version, estado, fecha_publicacion, content_hash, fuente_url FROM {CATALOGO}.{SCHEMA}.documentos_pendientes WHERE norma_id = '{safe}' ORDER BY version")
def comparar_versiones(norma_id, version_anterior, version_nueva):
    safe = norma_id.replace("'", "''")
    return spark.sql(f"SELECT * FROM {CATALOGO}.{SCHEMA}.cambios_normativos WHERE norma_id = '{safe}' AND version_anterior = {int(version_anterior)} AND version_nueva = {int(version_nueva)} ORDER BY articulo")
def corroborar(norma_id, articulo, version):
    n = norma_id.replace("'", "''"); a = articulo.replace("'", "''")
    return spark.sql(f"SELECT norma_id, version, articulo, texto, tipo_fuente FROM {CATALOGO}.{SCHEMA}.articulos_fuente WHERE norma_id = '{n}' AND articulo = '{a}' AND version = {int(version)}")

plan = {"tool": "comparar_versiones", "argumentos": {"norma_id": "272-2017", "version_anterior": 30, "version_nueva": 31}}
assert plan["tool"] in ALLOWLIST
resultado = comparar_versiones(**plan["argumentos"])
normas_lote = [r.norma_id for r in pendientes.select("norma_id").distinct().collect()]
if "272-2017" in normas_lote:
    assert resultado.count() >= 4, "agent_compare_tool"
print(f"✅ agente supervisor lógico: {plan['tool']} → {resultado.count()} cambios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Marcar snapshots procesados y dejar traza

# COMMAND ----------

for doc_id, estado, _ in procesados:
    safe_doc_id = doc_id.replace("'", "''")
    spark.sql(f"UPDATE {CATALOGO}.{SCHEMA}.documentos_pendientes SET estado_proceso = '{estado}' WHERE doc_id = '{safe_doc_id}'")
traza = [("comparar versiones 272-2017", plan["tool"], json.dumps(plan["argumentos"]), resultado.count(), datetime.now(timezone.utc))]
spark.createDataFrame(traza, "pregunta string, tool string, argumentos_json string, filas_resultado long, ejecutado_ts timestamp").write.mode("append").saveAsTable(f"{CATALOGO}.{SCHEMA}.trazas_agente")
restantes = spark.table(f"{CATALOGO}.{SCHEMA}.documentos_pendientes").filter("estado_proceso = 'pendiente'").count()
print(json.dumps({"procesados": len(procesados), "pendientes_restantes": restantes, "diff_rows": df_cambios_lote.count(), "embedding_dimension": 1024}, ensure_ascii=False, indent=2))
assert restantes == 0
print("🎓 S05 DINÁMICA LISTA · tabla de control → procesamiento incremental → diff → embeddings → agente")
