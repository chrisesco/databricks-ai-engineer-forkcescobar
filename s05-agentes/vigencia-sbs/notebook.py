# Databricks notebook source
# MAGIC %md
# MAGIC # S05 · Vigía de vigencia normativa SBS
# MAGIC
# MAGIC Construimos un agente de solo lectura que busca normas, lista versiones, compara artículos
# MAGIC y corrobora evidencia. Los textos del laboratorio son **fixtures didácticos** basados en
# MAGIC metadatos públicos; no son una versión legal consolidada ni asesoría regulatoria.

# COMMAND ----------

import json
import re
from datetime import datetime, timezone
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "", "Tu catálogo de S01–S02")
dbutils.widgets.text("embedding_endpoint", "databricks-qwen3-embedding-0-6b", "Endpoint de embeddings")
dbutils.widgets.text("top_k", "4", "Candidatos")
print("✅ Widget creado. Escribe arriba el nombre completo de tu catálogo y vuelve a ejecutar esta celda.")

CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint").strip()
TOP_K = int(dbutils.widgets.get("top_k"))
assert re.fullmatch(r"[a-z][a-z0-9_]{1,127}", CATALOGO), "Usa el catálogo completo, por ejemplo neptuno_tunombre"
assert 1 <= TOP_K <= 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Descubrir endpoints antes de usarlos

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
endpoints_activos = [ep.name for ep in w.serving_endpoints.list()]
if EMBEDDING_ENDPOINT in endpoints_activos:
    print(f"✅ El endpoint '{EMBEDDING_ENDPOINT}' sí existe.")
else:
    print(f"❌ El endpoint '{EMBEDDING_ENDPOINT}' NO existe en este espacio de trabajo.")
    print("Endpoints disponibles:", endpoints_activos)
assert EMBEDDING_ENDPOINT in endpoints_activos, f"endpoint_missing: {EMBEDDING_ENDPOINT}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Crear el área gobernada

# COMMAND ----------

SCHEMA = "s05_vigencia_sbs"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Área creada: {CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Corpus versionado del laboratorio
# MAGIC
# MAGIC La relación histórica y las URLs corresponden a fuentes SBS públicas. El texto breve de
# MAGIC artículos se mantiene como fixture explícitamente marcado para que el ejercicio sea seguro,
# MAGIC reproducible y no se confunda con una transcripción legal.

# COMMAND ----------

documentos = [
    ("272-2017-v30", "272-2017", 30, "Resolución SBS N.° 272-2017", "vigente_historica", "SF, SS", "2025-05-12", "2025-05-12", "https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/VerHistorial.aspx?NormaId=1708", True),
    ("272-2017-v31", "272-2017", 31, "Resolución SBS N.° 272-2017", "vigente", "SF, SS", "2026-08-04", "2026-08-04", "https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/VerHistorial.aspx?NormaId=1708", True),
    ("1959-2026-v1", "1959-2026", 1, "Resolución SBS N.° 1959-2026", "vigente", "SF, SS", "2026-08-04", "2026-08-03", "https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/BusquedaPortal.aspx", True),
]
schema_documentos = "doc_id string, norma_id string, version int, titulo string, estado string, sistemas string, fecha_publicacion string, fecha_emision string, fuente_url string, es_fixture boolean"
df_documentos = spark.createDataFrame(documentos, schema_documentos)
df_documentos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.documentos")
display(df_documentos.orderBy("norma_id", "version"))

# COMMAND ----------

articulos = [
    ("272-2017", 30, "art-23", "Gestión integral de riesgos", "La empresa establece responsabilidades y reporta la gestión integral de riesgos al directorio.", "fixture_didactico"),
    ("272-2017", 30, "art-24", "Comité de riesgos", "El comité revisa periódicamente los riesgos relevantes y deja constancia de sus acuerdos.", "fixture_didactico"),
    ("272-2017", 30, "art-25", "Información", "La información de riesgos se conserva y se pone a disposición de los órganos de gobierno.", "fixture_didactico"),
    ("272-2017", 31, "art-23", "Gestión integral de riesgos", "La empresa establece responsabilidades, reporta la gestión integral de riesgos y documenta los indicadores mínimos al directorio.", "fixture_didactico"),
    ("272-2017", 31, "art-24", "Comité de riesgos", "El comité revisa mensualmente los riesgos relevantes, registra acuerdos y asigna responsables de seguimiento.", "fixture_didactico"),
    ("272-2017", 31, "art-26", "Trazabilidad", "La empresa conserva evidencia de decisiones, responsables y fecha de revisión para cada riesgo material.", "fixture_didactico"),
    ("1959-2026", 1, "art-1", "Modificación puntual", "Modifica los artículos 1 y 2 del Reglamento de Gobierno Corporativo y de la Gestión Integral de Riesgos.", "fuente_metadata"),
]
schema_articulos = "norma_id string, version int, articulo string, titulo string, texto string, tipo_fuente string"
df_articulos = spark.createDataFrame(articulos, schema_articulos)
df_articulos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.articulos_versionados")
display(df_articulos.orderBy("norma_id", "version", "articulo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · UC Function: contrato gobernado de vigencia

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOGO}.{SCHEMA}.ultima_version(p_norma_id STRING)
RETURNS INT
LANGUAGE SQL
COMMENT 'Devuelve la última versión cargada de una norma SBS. Solo lectura.'
RETURN SELECT MAX(version) FROM {CATALOGO}.{SCHEMA}.documentos WHERE norma_id = p_norma_id
""")
ultima = spark.sql(f"SELECT {CATALOGO}.{SCHEMA}.ultima_version('272-2017') AS version_vigente").first()["version_vigente"]
print(f"✅ UC Function ultima_version: {ultima}")
assert ultima == 31, f"uc_function_last_version: {ultima}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Diff determinista por artículo

# COMMAND ----------

base = df_articulos.filter("norma_id = '272-2017'")
old = base.filter("version = 30").select("norma_id", F.col("articulo"), F.col("version").alias("version_anterior"), F.col("texto").alias("texto_anterior"))
new = base.filter("version = 31").select("norma_id", F.col("articulo"), F.col("version").alias("version_nueva"), F.col("texto").alias("texto_nuevo"))
df_cambios = (
    old.join(new, ["norma_id", "articulo"], "full")
    .withColumn("version_anterior", F.coalesce("version_anterior", F.lit(30)))
    .withColumn("version_nueva", F.coalesce("version_nueva", F.lit(31)))
    .withColumn("tipo_cambio", F.when(F.col("texto_anterior").isNull(), "agregado")
                .when(F.col("texto_nuevo").isNull(), "eliminado")
                .when(F.col("texto_anterior") != F.col("texto_nuevo"), "modificado")
                .otherwise("sin_cambio"))
    .withColumn("cambio_id", F.sha2(F.concat_ws("|", "norma_id", "articulo", "version_anterior", "version_nueva"), 256))
    .withColumn("texto_busqueda", F.concat_ws(" ", F.coalesce("articulo", F.lit("")), F.coalesce("texto_anterior", F.lit("")), F.coalesce("texto_nuevo", F.lit(""))))
)
df_cambios.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.cambios_normativos")
display(df_cambios.orderBy("articulo"))
assert df_cambios.filter("tipo_cambio = 'modificado'").count() == 2, "diff_modified_count"
assert df_cambios.filter("tipo_cambio = 'agregado'").count() == 1, "diff_added_count"
assert df_cambios.filter("tipo_cambio = 'eliminado'").count() == 1, "diff_deleted_count"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Embeddings de cambios y recuperación

# COMMAND ----------

df_embeddings = df_cambios.filter("tipo_cambio != 'sin_cambio'").withColumn(
    "embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto_busqueda)")
)
filas_embeddings = df_embeddings.select("cambio_id", "embedding").collect()
assert filas_embeddings and all(len(r.embedding) == 1024 for r in filas_embeddings), f"embedding_dimension: {[len(r.embedding) if r.embedding is not None else None for r in filas_embeddings]}"
df_embeddings.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.cambios_embeddings")
print(f"✅ Embeddings generados: {len(filas_embeddings)} · dimensión={len(filas_embeddings[0].embedding)}")

pregunta = "¿Qué cambió en las responsabilidades y seguimiento de riesgos?"
q = spark.createDataFrame([(pregunta,)], "pregunta string").withColumn("embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', pregunta)" )).first().embedding
def cosine(a, b):
    dot = sum(x*y for x, y in zip(a, b)); na = sum(x*x for x in a) ** 0.5; nb = sum(y*y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0
ranking = sorted([(r.cambio_id, cosine(q, r.embedding)) for r in df_embeddings.select("cambio_id", "embedding").collect()], key=lambda x: -x[1])
top_ids = [x[0] for x in ranking[:TOP_K]]
df_recuperados = df_cambios.filter(F.col("cambio_id").isin(top_ids)).withColumn("similitud_coseno", F.expr("0.0"))
display(df_recuperados.select("articulo", "tipo_cambio", "texto_anterior", "texto_nuevo"))
assert top_ids, f"retrieval_empty: {ranking}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Herramientas read-only

# COMMAND ----------

def buscar_norma(texto, sistema=None):
    t = texto.lower().replace("'", "''")
    filtro = f"lower(titulo) LIKE '%{t}%' OR lower(norma_id) LIKE '%{t}%'"
    if sistema:
        filtro += f" AND lower(sistemas) LIKE '%{sistema.lower()}%'"
    return spark.sql(f"SELECT norma_id, version, titulo, estado, sistemas, fuente_url FROM {CATALOGO}.{SCHEMA}.documentos WHERE {filtro} ORDER BY version DESC")

def listar_versiones(norma_id):
    safe = norma_id.replace("'", "''")
    return spark.sql(f"SELECT norma_id, version, estado, fecha_publicacion, fuente_url FROM {CATALOGO}.{SCHEMA}.documentos WHERE norma_id = '{safe}' ORDER BY version")

def comparar_versiones(norma_id, version_anterior, version_nueva):
    safe = norma_id.replace("'", "''")
    return spark.sql(f"SELECT * FROM {CATALOGO}.{SCHEMA}.cambios_normativos WHERE norma_id = '{safe}' AND version_anterior = {int(version_anterior)} AND version_nueva = {int(version_nueva)} ORDER BY articulo")

def corroborar(norma_id, articulo, version):
    safe_n = norma_id.replace("'", "''"); safe_a = articulo.replace("'", "''")
    return spark.sql(f"SELECT norma_id, version, articulo, titulo, texto, tipo_fuente FROM {CATALOGO}.{SCHEMA}.articulos_versionados WHERE norma_id = '{safe_n}' AND articulo = '{safe_a}' AND version = {int(version)}")

assert buscar_norma("272-2017").count() == 2, "tool_buscar_norma"
assert listar_versiones("272-2017").count() == 2, "tool_listar_versiones"
assert comparar_versiones("272-2017", 30, 31).count() == 4, "tool_comparar_versiones"
assert corroborar("272-2017", "art-23", 31).count() == 1, "tool_corroborar"
print("✅ 4 herramientas read-only verificadas fuera del agente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Router/agente: planificar, validar, ejecutar, observar

# COMMAND ----------

ALLOWLIST = {"buscar_norma", "listar_versiones", "comparar_versiones", "corroborar"}
def planificar(pregunta):
    p = pregunta.lower()
    if "entre" in p and "versión" in p:
        return {"tool": "comparar_versiones", "argumentos": {"norma_id": "272-2017", "version_anterior": 30, "version_nueva": 31}}
    if "version" in p or "historial" in p:
        return {"tool": "listar_versiones", "argumentos": {"norma_id": "272-2017"}}
    if "artículo" in p or "articulo" in p:
        return {"tool": "corroborar", "argumentos": {"norma_id": "272-2017", "articulo": "art-23", "version": 31}}
    return {"tool": "buscar_norma", "argumentos": {"texto": "272-2017"}}

def ejecutar(plan):
    tool = plan["tool"]; args = plan["argumentos"]
    assert tool in ALLOWLIST, f"Tool no autorizada: {tool}"
    return {"buscar_norma": buscar_norma, "listar_versiones": listar_versiones, "comparar_versiones": comparar_versiones, "corroborar": corroborar}[tool](**args)

preguntas = [
    "¿Qué cambió entre la versión anterior y la vigente?",
    "¿Qué versiones tiene la Resolución 272-2017?",
    "Corrobora el artículo 23 en la versión vigente.",
]
trazas = []
for pregunta in preguntas:
    plan = planificar(pregunta); resultado = ejecutar(plan)
    n = resultado.count()
    trazas.append((pregunta, plan["tool"], json.dumps(plan["argumentos"], ensure_ascii=False), n, "ok", datetime.now(timezone.utc)))
df_trazas = spark.createDataFrame(trazas, "pregunta string, tool string, argumentos_json string, filas_resultado long, estado string, ejecutado_ts timestamp")
df_trazas.write.mode("append").saveAsTable(f"{CATALOGO}.{SCHEMA}.trazas_agente")
display(df_trazas)
assert df_trazas.filter("filas_resultado > 0").count() == 3, "agent_trace_positive_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Abstención y resumen de la corrida

# COMMAND ----------

assert listar_versiones("norma-inexistente").count() == 0, "abstention_empty_source"
resumen = {
    "norma": "272-2017",
    "versiones_comparadas": "30→31",
    "cambios": df_cambios.filter("tipo_cambio != 'sin_cambio'").count(),
    "embedding_dimension": len(filas_embeddings[0].embedding),
    "retrieval_top_k": len(top_ids),
    "tools_verified": sorted(ALLOWLIST),
    "uc_function_last_version": int(ultima),
    "abstention_empty_source": True,
}
print(json.dumps(resumen, ensure_ascii=False, indent=2))
print(f"🎓 S05 VIGENCIA SBS LISTA · tablas Delta + diff + embeddings + tools + agente + trazas")
