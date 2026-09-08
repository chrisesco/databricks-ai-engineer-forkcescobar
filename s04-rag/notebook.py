# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 4 · RAG: recuperar, medir y responder con evidencia
# MAGIC **Databricks AI Engineer** — caso Neptuno
# MAGIC
# MAGIC En este laboratorio construiremos un RAG pequeño pero completo:
# MAGIC
# MAGIC 1. conservamos el texto crudo y normalizamos sin perder offsets;
# MAGIC 2. separamos span citable, texto de embedding y contexto de respuesta;
# MAGIC 3. comparamos BM25, dense y fusión RRF;
# MAGIC 4. generamos una respuesta con cita y una abstención;
# MAGIC 5. guardamos un gold set para no ajustar por intuición.
# MAGIC
# MAGIC ### Intuición rápida: relaciones en el espacio vectorial
# MAGIC
# MAGIC Ejemplo clásico: si restas el vector de **Hombre** a **Rey** y le sumas **Mujer**, el
# MAGIC resultado matemático estará muy cerca del vector de **Reina**:
# MAGIC
# MAGIC `Rey - Hombre + Mujer ≈ Reina`
# MAGIC
# MAGIC Es una analogía ilustrativa de embeddings de palabras: una relación puede aparecer como
# MAGIC una dirección en el espacio vectorial. No es una garantía exacta de todos los modelos
# MAGIC modernos ni sustituye la evaluación sobre nuestro corpus.

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

# MAGIC %md ## 0 · Configuración
# MAGIC
# MAGIC Cada alumno usa su catálogo `neptuno_<nombre>`. La validación docente puede usar otro schema
# MAGIC para no tocar las tablas de clase.

# COMMAND ----------

import hashlib
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

dbutils.widgets.text("schema", "rag", "Schema del laboratorio")
dbutils.widgets.text("modelo", "system.ai.gpt-5-6-luna", "Modelo generador")
dbutils.widgets.text("embedding_endpoint", "databricks-qwen3-embedding-0-6b", "Endpoint de embeddings")
dbutils.widgets.text("top_k", "4", "Candidatos por carril")

CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
SCHEMA = dbutils.widgets.get("schema").strip().lower()
MODELO = dbutils.widgets.get("modelo").strip()
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint").strip()
TOP_K = int(dbutils.widgets.get("top_k"))

assert re.fullmatch(r"neptuno_[a-z0-9_]+", CATALOGO or ""), "Usa neptuno_<nombre>."
assert re.fullmatch(r"rag(?:_[a-z0-9_]+)?", SCHEMA or ""), "Usa rag o rag_<sufijo>."
assert re.fullmatch(r"[a-z0-9_.-]+", EMBEDDING_ENDPOINT), "Endpoint inválido."
assert 1 <= TOP_K <= 10
assert CATALOGO in {r.catalog.lower() for r in spark.sql("SHOW CATALOGS").collect()}

NS = f"{CATALOGO}.{SCHEMA}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NS}")
print(f"✅ namespace: {NS}")

# COMMAND ----------

# MAGIC %md ### ⭐ Checkpoint · endpoint de embeddings real
# MAGIC
# MAGIC Para español usamos `databricks-qwen3-embedding-0-6b`: es multilingüe, acepta lotes y
# MAGIC devuelve hasta 1.024 dimensiones. La disponibilidad depende de región y workspace, por eso
# MAGIC validamos primero y fallamos con un mensaje accionable.
# MAGIC La celda anterior lista los endpoints visibles: úsala para confirmar el nombre real antes de
# MAGIC cambiar el widget de embeddings.

# COMMAND ----------

w = WorkspaceClient()
endpoints_activos = [ep.name for ep in w.serving_endpoints.list()]
nombre_widget = EMBEDDING_ENDPOINT
if nombre_widget in endpoints_activos:
    print(f"✅ El endpoint '{nombre_widget}' sí existe.")
else:
    print(f"❌ El endpoint '{nombre_widget}' NO existe en este espacio de trabajo.")
    print("Endpoints disponibles:", endpoints_activos)
endpoints = {ep.name: ep for ep in w.serving_endpoints.list()}
assert EMBEDDING_ENDPOINT in endpoints, (
    f"No existe {EMBEDDING_ENDPOINT}. Revisa la lista anterior y actualiza el widget."
)

estado = str(endpoints[EMBEDDING_ENDPOINT].state.ready)
assert "READY" in estado.upper(), f"El endpoint existe, pero su estado es {estado}."
print(f"✅ {EMBEDDING_ENDPOINT}: READY")

# COMMAND ----------

probe_textos = [
    "devolución de productos refrigerados",
    "política de cadena de frío",
    "pronóstico del clima",
]
probe = (
    spark.createDataFrame([(x,) for x in probe_textos], "texto string")
    .withColumn("embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto)"))
    .collect()
)

def cosine(a, b):
    numerador = sum(x * y for x, y in zip(a, b))
    norma = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return numerador / norma if norma else 0.0

dims = {len(r.embedding) for r in probe}
sim_relacionada = cosine(probe[0].embedding, probe[1].embedding)
sim_ajena = cosine(probe[0].embedding, probe[2].embedding)
assert dims == {1024}, f"Dimensión inesperada: {dims}"
assert sim_relacionada > sim_ajena, "El smoke test semántico no separó el par relacionado."
print(f"✅ 3 vectores · 1.024 dims · relacionada={sim_relacionada:.3f} · ajena={sim_ajena:.3f}")

# COMMAND ----------

# MAGIC %md ### ⭐ Experimento: una relación como dirección vectorial
# MAGIC
# MAGIC Ahora lo comprobamos con el endpoint real. Calculamos `Rey - Hombre + Mujer` y medimos
# MAGIC qué tan cerca queda del embedding de `Reina`. El porcentaje es una forma didáctica de
# MAGIC mostrar la similitud coseno; no es una probabilidad ni una garantía del modelo.

# COMMAND ----------

palabras_analogia = ["Rey", "Hombre", "Mujer", "Reina"]
df_analogia = spark.createDataFrame([(x,) for x in palabras_analogia], "palabra string")
filas_analogia = (
    df_analogia
    .withColumn("embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', palabra)"))
    .collect()
)
vectores_analogia = {fila.palabra: fila.embedding for fila in filas_analogia}
assert {len(vector) for vector in vectores_analogia.values()} == {1024}

tabla_vectores = spark.createDataFrame(
    [(palabra, vectores_analogia[palabra]) for palabra in palabras_analogia],
    "palabra string, vector array<double>",
)
display(tabla_vectores)
print("✅ Tabla de palabras y vectores: 4 filas · 1.024 dimensiones por vector")

vector_resultado = [
    rey - hombre + mujer
    for rey, hombre, mujer in zip(
        vectores_analogia["Rey"],
        vectores_analogia["Hombre"],
        vectores_analogia["Mujer"],
    )
]
display(
    spark.createDataFrame(
        [("Rey - Hombre + Mujer", vector_resultado)],
        "operacion string, vector_resultante array<double>",
    )
)

similitud_reina = cosine(vector_resultado, vectores_analogia["Reina"])
parecido_pct = max(0.0, min(1.0, similitud_reina)) * 100

display(
    spark.createDataFrame(
        [("Rey - Hombre + Mujer", "Reina", float(similitud_reina), float(parecido_pct))],
        "vector_resultante string, vector_referencia string, similitud_coseno double, parecido_porcentaje double",
    )
)
print(f"✅ Comparación contra Reina · parecido orientativo={parecido_pct:.2f}%")

# COMMAND ----------

# MAGIC %md ## 1 · Corpus crudo de Neptuno
# MAGIC
# MAGIC El primer documento incluye una palabra cortada por guion y salto de línea. Es deliberado:
# MAGIC nos permite comprobar que la normalización mejora búsqueda sin reescribir la evidencia original.

# COMMAND ----------

documentos = [
    (
        "politica_devoluciones",
        "Política de devoluciones",
        "2026-07-01",
        "Las devoluciones de alimentos no pere-\ncibles se aceptan hasta 30 días después de la entrega. "
        "Productos refrigerados o congelados solo se aceptan dentro de 24 horas si existe evidencia "
        "de ruptura de cadena de frío. El cliente debe informar el número de pedido.",
    ),
    (
        "contrato_expreso_veloz",
        "Contrato Expreso Veloz",
        "2026-06-15",
        "Expreso Veloz cubre Lima Metropolitana. El compromiso estándar es entregar en 24 horas hábiles. "
        "Si supera 48 horas, Neptuno puede solicitar una nota de crédito equivalente al 8 por ciento del flete.",
    ),
    (
        "contrato_paquetes_unidos",
        "Contrato Paquetes Unidos",
        "2026-06-15",
        "Paquetes Unidos cubre provincias. El plazo objetivo es de 3 a 5 días hábiles. Mercadería refrigerada "
        "requiere embalaje térmico provisto por Neptuno; el transportista registra temperatura en recepción y entrega.",
    ),
    (
        "ficha_bebidas",
        "Ficha de categoría Bebidas",
        "2026-05-20",
        "La categoría Bebidas incluye cafés, tés, cervezas y vinos importados. Los vinos requieren almacenamiento "
        "sin luz directa entre 12 y 18 grados Celsius. No deben colocarse junto a productos con olores intensos.",
    ),
    (
        "ficha_lacteos",
        "Ficha de categoría Lácteos",
        "2026-05-20",
        "Los productos lácteos se reciben entre 2 y 6 grados Celsius. Una lectura fuera de rango debe registrarse "
        "como incidencia y el lote queda en cuarentena hasta evaluación de calidad.",
    ),
    (
        "proveedor_exoticos",
        "Ficha del proveedor Exóticos Líquidos",
        "2026-04-10",
        "Exóticos Líquidos provee principalmente Bebidas desde Reino Unido. El pedido mínimo es de 20 cajas y "
        "el lead time habitual es 18 días. Contacto operativo: compras internacionales.",
    ),
]

df_docs = spark.createDataFrame(
    documentos,
    "documento_id string, titulo string, fecha string, contenido_raw string",
).withColumn("fecha", F.to_date("fecha"))

df_docs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{NS}.documentos")
assert spark.table(f"{NS}.documentos").count() == 6
display(spark.table(f"{NS}.documentos").select("documento_id", "titulo", "fecha"))

# COMMAND ----------

# MAGIC %md ## 2 · Normalización conservadora y offsets
# MAGIC
# MAGIC La regla elimina **solo** `guion + un salto de línea` entre letras. Conservamos `contenido_raw`
# MAGIC y un mapa `índice normalizado → índice raw`; así una cita puede volver al original.

# COMMAND ----------

CORTE_HIFEN = re.compile(
    r"(?<=[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{2})-[ \t]*\n[ \t]*(?=[a-záéíóúüñ])"
)

def deshifenar_con_mapa(texto_raw: str):
    salida, mapa = [], []
    cursor = 0
    for match in CORTE_HIFEN.finditer(texto_raw):
        for pos in range(cursor, match.start()):
            salida.append(texto_raw[pos])
            mapa.append(pos)
        cursor = match.end()
    for pos in range(cursor, len(texto_raw)):
        salida.append(texto_raw[pos])
        mapa.append(pos)
    return "".join(salida), mapa

texto_demo, mapa_demo = deshifenar_con_mapa(documentos[0][3])
assert "perecibles" in texto_demo
assert "pere-\ncibles" in documentos[0][3]
assert len(texto_demo) == len(mapa_demo)
print("✅ hifenación controlada: raw intacto, vista normalizada y mapa de offsets")

# COMMAND ----------

def oraciones_con_offsets(texto: str):
    for match in re.finditer(r"[^.!?]+(?:[.!?]+|$)", texto, flags=re.S):
        segmento = match.group(0)
        izquierda = len(segmento) - len(segmento.lstrip())
        derecha = len(segmento.rstrip())
        inicio = match.start() + izquierda
        fin = match.start() + derecha
        if fin > inicio and len(texto[inicio:fin]) > 20:
            yield inicio, fin, texto[inicio:fin]

filas_chunks = []
for documento_id, titulo, fecha, contenido_raw in documentos:
    normalizado, mapa = deshifenar_con_mapa(contenido_raw)
    spans = list(oraciones_con_offsets(normalizado))
    for chunk_n, (norm_start, norm_end, texto_citable) in enumerate(spans, start=1):
        raw_start = mapa[norm_start]
        raw_end = mapa[norm_end - 1] + 1
        raw_span = contenido_raw[raw_start:raw_end]
        reconstruido, _ = deshifenar_con_mapa(raw_span)
        assert reconstruido.strip() == texto_citable
        previo = spans[chunk_n - 2][2] if chunk_n > 1 else ""
        texto_embedding = (
            f"{titulo}. Contexto anterior: {previo} Fragmento: {texto_citable}"
            if previo
            else f"{titulo}. {texto_citable}"
        )
        chunk_id = hashlib.sha256(f"{documento_id}|{chunk_n}".encode()).hexdigest()
        filas_chunks.append(
            (
                chunk_id,
                documento_id,
                titulo,
                fecha,
                chunk_n,
                raw_start,
                raw_end,
                texto_citable,
                texto_embedding,
            )
        )

chunks = spark.createDataFrame(
    filas_chunks,
    "chunk_id string, documento_id string, titulo string, fecha string, chunk_n int, "
    "raw_start int, raw_end int, texto_citable string, texto_embedding string",
).withColumn("fecha", F.to_date("fecha"))

chunks.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{NS}.chunks")
spark.sql(f"ALTER TABLE {NS}.chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
n_chunks = spark.table(f"{NS}.chunks").count()
assert n_chunks == 17, f"Se esperaban 17 chunks; hay {n_chunks}."

detalle = spark.sql(f"DESCRIBE DETAIL {NS}.chunks").first().asDict()
assert str(detalle["properties"].get("delta.enableChangeDataFeed", "")).lower() == "true"
print(f"✅ {n_chunks} chunks · offsets reversibles · CDF=true")
display(spark.table(f"{NS}.chunks").select("documento_id", "chunk_n", "raw_start", "raw_end", "texto_citable"))

# COMMAND ----------

# MAGIC %md ## 3 · Embeddings persistidos
# MAGIC
# MAGIC Indexamos `texto_embedding`, que puede incorporar contexto anterior. La cita continúa siendo
# MAGIC `texto_citable`, sin overlap, y los offsets siguen apuntando al raw.

# COMMAND ----------

chunks_embeddings = (
    spark.table(f"{NS}.chunks")
    .withColumn("embedding", F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto_embedding)"))
)
chunks_embeddings.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{NS}.chunks_embeddings"
)
spark.sql(
    f"ALTER TABLE {NS}.chunks_embeddings SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

dimensiones = {
    r.dims
    for r in spark.table(f"{NS}.chunks_embeddings")
    .select(F.size("embedding").alias("dims"))
    .distinct()
    .collect()
}
assert dimensiones == {1024}
assert spark.table(f"{NS}.chunks_embeddings").count() == n_chunks
print(f"✅ {n_chunks} embeddings persistidos · dimensión 1.024")

# COMMAND ----------

# MAGIC %md ## 4 · Tres carriles comparables: BM25, dense y RRF
# MAGIC
# MAGIC Los tres devuelven el mismo contrato (`chunk_id`, documento, texto y score). RRF fusiona
# MAGIC posiciones; no reparte cupos fijos entre listas.

# COMMAND ----------

STOPWORDS = {
    "a", "al", "con", "cual", "como", "de", "del", "el", "en", "es", "la", "las", "lo",
    "los", "para", "por", "que", "se", "si", "su", "un", "una", "y",
}

def tokenizar(texto: str):
    limpio = unicodedata.normalize("NFKD", texto.lower())
    limpio = "".join(c for c in limpio if not unicodedata.combining(c))
    return [t for t in re.findall(r"[a-z0-9]+", limpio) if len(t) > 1 and t not in STOPWORDS]

chunk_rows = [r.asDict() for r in spark.table(f"{NS}.chunks_embeddings").collect()]
por_id = {r["chunk_id"]: r for r in chunk_rows}
tokens_docs = {r["chunk_id"]: tokenizar(r["texto_embedding"]) for r in chunk_rows}
N = len(tokens_docs)
avgdl = sum(len(ts) for ts in tokens_docs.values()) / N
df_termino = Counter(t for ts in tokens_docs.values() for t in set(ts))

def ranking_bm25(pregunta: str, k: int = TOP_K):
    k1, b = 1.5, 0.75
    consulta = tokenizar(pregunta)
    ranking = []
    for chunk_id, tokens in tokens_docs.items():
        frecuencias = Counter(tokens)
        score = 0.0
        for termino in consulta:
            if termino not in frecuencias:
                continue
            idf = math.log(1 + (N - df_termino[termino] + 0.5) / (df_termino[termino] + 0.5))
            tf = frecuencias[termino]
            score += idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * len(tokens) / avgdl))
        if score > 0:
            ranking.append((chunk_id, score))
    return sorted(ranking, key=lambda x: (-x[1], x[0]))[:k]

def embedding_consulta(pregunta: str):
    return (
        spark.createDataFrame([(pregunta,)], "texto string")
        .select(F.expr(f"ai_query('{EMBEDDING_ENDPOINT}', texto)").alias("embedding"))
        .first()["embedding"]
    )

def ranking_dense(pregunta: str, k: int = TOP_K):
    qvec = embedding_consulta(pregunta)
    ranking = [(r["chunk_id"], cosine(qvec, r["embedding"])) for r in chunk_rows]
    return sorted(ranking, key=lambda x: (-x[1], x[0]))[:k]

def ranking_rrf(lexico, dense, k: int = TOP_K, constante: int = 60):
    scores = defaultdict(float)
    for lista in (lexico, dense):
        for posicion, (chunk_id, _) in enumerate(lista, start=1):
            scores[chunk_id] += 1 / (constante + posicion)
    return sorted(scores.items(), key=lambda x: (-x[1], x[0]))[:k]

def recuperar(pregunta: str, k: int = TOP_K):
    lexico = ranking_bm25(pregunta, k)
    dense = ranking_dense(pregunta, k)
    hibrido = ranking_rrf(lexico, dense, k)
    return {"bm25": lexico, "dense": dense, "rrf": hibrido}

pregunta_demo = "¿En cuánto tiempo puedo retornar un congelado si se rompió la cadena de frío?"
carriles_demo = recuperar(pregunta_demo)
for nombre, ranking in carriles_demo.items():
    primero = por_id[ranking[0][0]]
    print(f"{nombre:>5} → {primero['titulo']} · score={ranking[0][1]:.4f}")

# COMMAND ----------

# MAGIC %md ### ⭐ Demo · inspeccionar lo recuperado antes de generar

# COMMAND ----------

filas_demo = []
for carril, ranking in carriles_demo.items():
    for posicion, (chunk_id, score) in enumerate(ranking, start=1):
        r = por_id[chunk_id]
        filas_demo.append((carril, posicion, score, r["documento_id"], r["titulo"], r["texto_citable"]))

display(
    spark.createDataFrame(
        filas_demo,
        "carril string, posicion int, score double, documento_id string, titulo string, texto_citable string",
    ).orderBy("carril", "posicion")
)

# COMMAND ----------

# MAGIC %md ## 5 · Gold set: retrieval y abstención
# MAGIC
# MAGIC Cuatro preguntas positivas y una negativa. La negativa no exige que el buscador devuelva cero
# MAGIC vecinos — siempre habrá un vecino geométrico—; exige que el gate reconozca evidencia insuficiente.

# COMMAND ----------

gold = [
    ("devolucion_frio", "¿Cuánto tiempo tengo para retornar un congelado si se rompió la cadena de frío?", "politica_devoluciones", "24 horas"),
    ("compensacion_flete", "¿Qué compensación corresponde si Expreso Veloz demora más de dos días?", "contrato_expreso_veloz", "8 por ciento"),
    ("lacteo_fuera_rango", "¿Qué ocurre con un lote lácteo recibido a 8 grados?", "ficha_lacteos", "cuarentena"),
    ("lead_time", "¿Cuál es el tiempo de abastecimiento de Exóticos Líquidos?", "proveedor_exoticos", "18 días"),
    ("sin_evidencia", "¿Cuál es la contraseña del WiFi del almacén?", None, None),
]

evaluacion = []
cache_carriles = {}
for caso, pregunta, documento_esperado, evidencia_esperada in gold:
    carriles = recuperar(pregunta)
    cache_carriles[caso] = carriles
    bm25_top = por_id[carriles["bm25"][0][0]]["documento_id"] if carriles["bm25"] else None
    dense_top = por_id[carriles["dense"][0][0]]["documento_id"]
    rrf_top = por_id[carriles["rrf"][0][0]]["documento_id"]
    lex_score = carriles["bm25"][0][1] if carriles["bm25"] else 0.0
    dense_score = carriles["dense"][0][1]
    evidencia_suficiente = lex_score > 0 or dense_score >= 0.65
    docs_rrf = [por_id[chunk_id]["documento_id"] for chunk_id, _ in carriles["rrf"]]
    hit_top3 = documento_esperado in docs_rrf if documento_esperado else None
    abstiene = not evidencia_suficiente
    evaluacion.append(
        (
            caso,
            pregunta,
            documento_esperado,
            evidencia_esperada,
            bm25_top,
            dense_top,
            rrf_top,
            float(lex_score),
            float(dense_score),
            hit_top3,
            abstiene,
        )
    )

df_eval = spark.createDataFrame(
    evaluacion,
    "caso string, pregunta string, documento_esperado string, evidencia_esperada string, "
    "bm25_top1 string, dense_top1 string, rrf_top1 string, bm25_score double, dense_score double, "
    "rrf_hit_top3 boolean, abstiene boolean",
)
display(df_eval)

positivos = df_eval.filter("documento_esperado IS NOT NULL")
assert positivos.filter("NOT rrf_hit_top3").count() == 0, "RRF perdió evidencia del gold set en top-3."
assert positivos.filter("rrf_top1 = documento_esperado").count() >= 3, "RRF debe acertar al menos 3/4 en top-1."
assert df_eval.filter("caso = 'sin_evidencia' AND abstiene").count() == 1, "El gate no abstuvo el caso negativo."

df_eval.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{NS}.evaluacion_retrieval")
print("✅ gold set: 4 positivos con evidencia en top-3 + 1 negativo con abstención")

# COMMAND ----------

# MAGIC %md ## 6 · Reranking listwise sobre el shortlist
# MAGIC
# MAGIC El reranker **no reemplaza** el primer retrieval: relee juntos los candidatos y devuelve un
# MAGIC nuevo orden. Medimos también si produjo una salida válida; un ranking con empates o IDs inventados
# MAGIC no sirve para un gate de confianza.

# COMMAND ----------

candidatos = cache_carriles["devolucion_frio"]["rrf"]
lineas = []
for chunk_id, _ in candidatos:
    r = por_id[chunk_id]
    lineas.append(f"ID={chunk_id}\nTÍTULO={r['titulo']}\nTEXTO={r['texto_citable']}")

prompt_rerank = f"""
Ordena TODOS los candidatos del más al menos relevante para responder la pregunta.
Pregunta: {gold[0][1]}

{chr(10).join(lineas)}

Devuelve únicamente un arreglo JSON de IDs, sin markdown ni explicación.
No agregues, repitas ni elimines IDs.
""".strip()

salida_rerank = (
    spark.createDataFrame([(prompt_rerank,)], "prompt string")
    .select(F.expr(f"ai_query('{MODELO}', prompt)").alias("respuesta"))
    .first()["respuesta"]
)

match = re.search(r"\[[\s\S]*\]", salida_rerank)
ids_rerank = json.loads(match.group(0)) if match else []
ids_candidatos = [chunk_id for chunk_id, _ in candidatos]
rerank_valido = (
    len(ids_rerank) == len(ids_candidatos)
    and len(set(ids_rerank)) == len(ids_rerank)
    and set(ids_rerank) == set(ids_candidatos)
)
assert rerank_valido, f"El reranker no devolvió una permutación válida: {salida_rerank}"
print("✅ reranker listwise: permutación válida del shortlist")
for posicion, chunk_id in enumerate(ids_rerank, start=1):
    print(f"{posicion}. {por_id[chunk_id]['titulo']} — {por_id[chunk_id]['texto_citable']}")

# COMMAND ----------

# MAGIC %md ## 7 · Generación grounded con cita
# MAGIC
# MAGIC Para la respuesta final agregamos los tres primeros spans citables. El modelo no ve el texto
# MAGIC usado para embeddings ni puede inventar una fuente fuera del bloque de evidencias.

# COMMAND ----------

top_contexto = ids_rerank[:3]
bloques = []
for chunk_id in top_contexto:
    r = por_id[chunk_id]
    bloques.append(f"[{r['titulo']}] {r['texto_citable']}")

prompt_grounded = f"""
Responde en español usando EXCLUSIVAMENTE estas evidencias:

{chr(10).join(bloques)}

Pregunta: {gold[0][1]}

Reglas:
- Si la evidencia no alcanza, responde exactamente: No hay evidencia suficiente.
- Cita cada afirmación factual con el título entre corchetes.
- Máximo 90 palabras.
""".strip()

respuesta = (
    spark.createDataFrame([(prompt_grounded,)], "prompt string")
    .select(F.expr(f"ai_query('{MODELO}', prompt)").alias("respuesta"))
    .first()["respuesta"]
)
print(respuesta)
assert "24" in respuesta
assert "[Política de devoluciones]" in respuesta
assert "No hay evidencia suficiente" not in respuesta
print("✅ respuesta positiva: dato esperado + cita válida")

# COMMAND ----------

# MAGIC %md ### ⭐ Caso negativo · abstener antes de llamar al LLM

# COMMAND ----------

negativo = df_eval.filter("caso = 'sin_evidencia'").first().asDict()
respuesta_negativa = (
    "No hay evidencia suficiente."
    if negativo["abstiene"]
    else "ERROR: el gate permitió generar sin evidencia."
)
print(respuesta_negativa)
assert respuesta_negativa == "No hay evidencia suficiente."

# COMMAND ----------

# MAGIC %md ## 8 · Ruta administrada con Databricks AI Search
# MAGIC
# MAGIC Este workspace no necesita un índice para demostrar embeddings. Para escalar el mismo contrato:
# MAGIC
# MAGIC 1. crea un endpoint **standard** de AI Search;
# MAGIC 2. crea un índice **Delta Sync** sobre `<catalogo>.<schema>.chunks_embeddings`;
# MAGIC 3. usa `chunk_id` como PK y la columna `embedding` existente (1.024 dims), o deja que el índice
# MAGIC    llame al endpoint administrado;
# MAGIC 4. compara `ANN`, `FULL_TEXT` y `HYBRID` con el mismo gold set;
# MAGIC 5. conserva filtros de vigencia, región y permisos antes de generar.
# MAGIC
# MAGIC > CDF ya quedó habilitado en las tablas fuente del laboratorio.

# COMMAND ----------

resumen = {
    "namespace": NS,
    "documentos": spark.table(f"{NS}.documentos").count(),
    "chunks": spark.table(f"{NS}.chunks").count(),
    "embedding_endpoint": EMBEDDING_ENDPOINT,
    "embedding_dims": next(iter(dimensiones)),
    "gold_positivos_top3": positivos.filter("rrf_hit_top3").count(),
    "gold_negativos_abstenidos": df_eval.filter("caso = 'sin_evidencia' AND abstiene").count(),
    "rerank_valido": rerank_valido,
    "respuesta_citada": "[Política de devoluciones]" in respuesta,
}
display(spark.createDataFrame([(k, str(v)) for k, v in resumen.items()], "control string, valor string"))
assert resumen == {
    "namespace": NS,
    "documentos": 6,
    "chunks": 17,
    "embedding_endpoint": EMBEDDING_ENDPOINT,
    "embedding_dims": 1024,
    "gold_positivos_top3": 4,
    "gold_negativos_abstenidos": 1,
    "rerank_valido": True,
    "respuesta_citada": True,
}
print("🎓 S04 LISTA: corpus + offsets + BM25/dense/RRF + rerank + cita + abstención + evaluación persistida")
