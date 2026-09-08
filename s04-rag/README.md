# Sesión 04 · RAG — dale a la IA el contexto de tu negocio

**lunes 7 de septiembre · 3 h**

## Qué vas a lograr

Construir un RAG pequeño y verificable sobre seis documentos de Neptuno: conservar el raw,
crear chunks citables, calcular embeddings, comparar BM25/dense/RRF, rerankear y responder con
cita o abstención.

## Material

- `slides-S04.html`: deck de la sesión. Teclas: `←/→`, `F`, `T`, `M` para estrellas y `A` para respuestas.
- `notebook.py`: notebook Databricks Source listo para importar.
- `notebook-append.py`: ruta opcional con `ai_parse_document` → `ai_prep_search` → embeddings → recuperación sobre archivos de un Volume.
- `slides-S04-append.html`: complemento sobre las 10 familias de recuperación, híbrido vs. reranking, facetas, grafos y nombres equivalentes en Databricks, Azure y AWS.
- Video recomendado: [¿Qué son los EMBEDDINGS? — Grandes Modelos de Lenguaje (10:09)](https://www.youtube.com/watch?v=h4GNDHC-s50).

## Antes de ejecutar

1. Importa `notebook.py` en tu workspace.
2. Conecta el notebook a serverless compute.
3. Escribe tu catálogo `neptuno_<nombre>` en el widget `catalogo`.
4. Conserva `schema=rag`.
5. El endpoint previsto para el corpus en español es `databricks-qwen3-embedding-0-6b`.

El notebook valida la disponibilidad antes de calcular nada. Si el endpoint no aparece, revisa
la región y los modelos habilitados en tu workspace; no sustituyas el nombre al azar.

## Qué crea

- `<catalogo>.rag.documentos`: seis fuentes con texto crudo.
- `<catalogo>.rag.chunks`: 17 spans citables, `raw_start/raw_end` y CDF habilitado.
- `<catalogo>.rag.chunks_embeddings`: embeddings de 1.024 dimensiones.
- `<catalogo>.rag.evaluacion_retrieval`: cuatro preguntas positivas y una negativa.

## Criterio de terminado

- cuatro casos positivos recuperados por RRF en top-3;
- el caso sin evidencia se abstiene;
- el reranker devuelve una permutación válida del shortlist;
- la respuesta contiene el dato esperado y una cita verificable.

> Este conjunto de cinco casos es un smoke test didáctico. Un benchmark productivo necesita más
> preguntas por estrato, negativos difíciles, revisión humana, latencia y costo.
