# Databricks notebook source
# MAGIC %md
# MAGIC # S04 · Append — AI Functions y recuperación sobre documentos
# MAGIC
# MAGIC Este append muestra la ruta administrada que el notebook base deja como alternativa:
# MAGIC `ai_parse_document` → `ai_prep_search` → embeddings → recuperación semántica.
# MAGIC Requiere una ruta de documentos en un Unity Catalog Volume. Si se deja vacía, se puede
# MAGIC ejecutar igualmente la demostración de `ai_prep_search` sobre texto plano.

# COMMAND ----------

# MAGIC %md ### Antes de empezar · catálogo del curso
# MAGIC
# MAGIC Al importar o actualizar el notebook, Databricks no ejecuta el código automáticamente.
# MAGIC Esta debe ser la primera celda que corras. Escribe arriba el nombre completo del catálogo
# MAGIC de S01 y S02, por ejemplo: `neptuno_tunombre`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "", "Tu catálogo de S01–S02")
print("✅ Widget creado. Escribe arriba el nombre completo de tu catálogo y vuelve a ejecutar esta celda.")

# COMMAND ----------

import math

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

dbutils.widgets.text("schema", "rag", "Schema del laboratorio")
dbutils.widgets.text("embedding_endpoint", "databricks-qwen3-embedding-0-6b", "Endpoint de embeddings")
dbutils.widgets.text("ruta_documentos", "", "Ruta de documentos en Volume (opcional)")
dbutils.widgets.text("pregunta", "¿Cómo se devuelve mercadería que perdió frío?", "Pregunta de prueba")
dbutils.widgets.text("top_k", "5", "Resultados a mostrar")

CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
SCHEMA = dbutils.widgets.get("schema").strip().lower()
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint").strip()
RUTA_DOCUMENTOS = dbutils.widgets.get("ruta_documentos").strip()
PREGUNTA = dbutils.widgets.get("pregunta").strip()
TOP_K = int(dbutils.widgets.get("top_k"))

assert CATALOGO, "Escribe primero el catálogo de S01–S02."
assert RUTA_DOCUMENTOS == "" or RUTA_DOCUMENTOS.startswith(f"/Volumes/{CATALOGO}/"), (
    f"La ruta debe vivir bajo /Volumes/{CATALOGO}/..."
)
assert 1 <= TOP_K <= 10
NS = f"{CATALOGO}.{SCHEMA}"
print(f"✅ namespace: {NS}")

# COMMAND ----------

# MAGIC %md ### Checkpoint · descubre el endpoint antes de usar Qwen
# MAGIC
# MAGIC La lista evita asumir que todos los workspaces tienen el mismo nombre de endpoint.

# COMMAND ----------

w = WorkspaceClient()
endpoints_activos = [ep.name for ep in w.serving_endpoints.list()]
nombre_widget = EMBEDDING_ENDPOINT
if nombre_widget in endpoints_activos:
    print(f"✅ El endpoint '{nombre_widget}' sí existe.")
else:
    print(f"❌ El endpoint '{nombre_widget}' NO existe en este espacio de trabajo.")
    print("Endpoints disponibles:", endpoints_activos)
    raise ValueError("Actualiza el widget embedding_endpoint con un endpoint disponible.")

endpoints = {ep.name: ep for ep in w.serving_endpoints.list()}
estado = str(endpoints[EMBEDDING_ENDPOINT].state.ready)
assert "READY" in estado.upper(), f"El endpoint existe, pero su estado es {estado}."
print(f"✅ {EMBEDDING_ENDPOINT}: READY")

# COMMAND ----------

# MAGIC %md ## 1 · `ai_prep_search` sobre texto plano
# MAGIC
# MAGIC Esta prueba no necesita un PDF: prepara una cadena de texto en chunks listos para búsqueda.
# MAGIC En la ruta completa, la entrada será el resultado estructurado de `ai_parse_document`.

# COMMAND ----------

texto_demo = """
Política de devoluciones. Los productos refrigerados o congelados solo se aceptan dentro de
24 horas si existe evidencia de ruptura de cadena de frío. El cliente debe informar el pedido.
""".strip()
texto_demo_sql = texto_demo.replace("'", "''").replace("\n", " ")

try:
    prep_demo = spark.sql(
        "SELECT ai_prep_search("
        + "'"
        + texto_demo_sql
        + "'"
        + ", map('version', '2.0')) AS result"
    )
    display(prep_demo)
    print("✅ ai_prep_search respondió sobre texto plano.")
except Exception as exc:
    print("⚠️ ai_prep_search no está habilitado en este entorno:", str(exc)[:500])

# COMMAND ----------

# MAGIC %md ## 2 · `ai_parse_document` + `ai_prep_search` sobre un Volume
# MAGIC
# MAGIC Sube un PDF, DOCX o PPTX a un Volume y escribe su carpeta en `ruta_documentos`.
# MAGIC La celda conserva la ruta de origen, parsea la estructura y luego produce chunks semánticos.

# COMMAND ----------

if not RUTA_DOCUMENTOS:
    print("ℹ️ Para ejecutar ai_parse_document, completa el widget ruta_documentos con /Volumes/<catálogo>/... .")
    prepped_chunks = None
else:
    ruta_sql = RUTA_DOCUMENTOS.replace("'", "''")
    prepped_chunks = spark.sql(
        f"""
        WITH parsed_documents AS (
          SELECT path, ai_parse_document(content, map('version', '2.0')) AS parsed
          FROM read_files('{ruta_sql}', format => 'binaryFile')
        ),
        prepped_documents AS (
          SELECT path, ai_prep_search(parsed, map('version', '2.0')) AS result
          FROM parsed_documents
        )
        SELECT
          prepped_documents.path AS source_uri,
          chunk.value:chunk_id::STRING AS chunk_id,
          chunk.value:chunk_position::INT AS chunk_position,
          chunk.value:chunk_to_retrieve::STRING AS texto_retrieval,
          chunk.value:chunk_to_embed::STRING AS texto_embedding,
          chunk.value:metadata AS metadata
        FROM prepped_documents,
             LATERAL variant_explode(prepped_documents.result:document.contents) AS chunk
        """
    )
    assert prepped_chunks.count() > 0, "No se produjeron chunks; revisa la ruta y el formato del archivo."
    display(prepped_chunks.orderBy("source_uri", "chunk_position"))
    print(f"✅ ai_parse_document → ai_prep_search: {prepped_chunks.count()} chunks listos")

# COMMAND ----------

# MAGIC %md ## 3 · Embeddings y recuperación sobre los chunks administrados
# MAGIC
# MAGIC Reutilizamos el mismo endpoint Qwen validado en el notebook base. La consulta y cada chunk
# MAGIC pasan por el mismo modelo; después ordenamos por similitud coseno.

# COMMAND ----------

if prepped_chunks is None:
    print("ℹ️ Recuperación omitida: ejecuta primero la ruta con ruta_documentos.")
else:
    indexed = prepped_chunks.withColumn(
        "embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto_embedding)")
    )
    qvec = (
        spark.createDataFrame([(PREGUNTA,)], "texto string")
        .select(F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto)").alias("embedding"))
        .first()["embedding"]
    )

    def cosine(a, b):
        numerador = sum(x * y for x, y in zip(a, b))
        norma = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
        return numerador / norma if norma else 0.0

    ranking = [
        (r.chunk_id, r.source_uri, r.texto_retrieval, cosine(qvec, r.embedding))
        for r in indexed.select("chunk_id", "source_uri", "texto_retrieval", "embedding").collect()
    ]
    ranking.sort(key=lambda row: (-row[3], row[0]))
    display(
        spark.createDataFrame(
            ranking[:TOP_K],
            "chunk_id string, source_uri string, texto_retrieval string, dense_score double",
        )
    )
    print(f"✅ recuperación dense: top-{TOP_K} para «{PREGUNTA}»")

# COMMAND ----------

# MAGIC %md ## 4 · Qué se ejecutó y qué queda preparado
# MAGIC
# MAGIC - El notebook base usa corpus sintético inline para explicar el contrato paso a paso.
# MAGIC - Este append demuestra la ruta administrada para archivos reales cuando hay un Volume.
# MAGIC - La recuperación usa los chunks de `ai_prep_search` y el mismo endpoint de embeddings.
