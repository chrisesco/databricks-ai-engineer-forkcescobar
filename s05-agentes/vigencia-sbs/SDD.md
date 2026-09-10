# SDD · Vigía de vigencia normativa SBS

**Curso:** Databricks AI Engineer · **Sesión:** S05 · **Contexto:** Perú  
**Estado:** iteración dinámica ejecutable · **Fecha:** 2026-09-08

## Iteración 2 — procesamiento dinámico y agentes configurables

La versión ejecutable separa el contenido didáctico inicial del procesamiento real:

- `bootstrap-laboratorio.py` crea una carga sintética explícita para el laboratorio.
- `notebook-dinamico.py` no contiene el corpus: lee `documentos_pendientes`, procesa únicamente
  snapshots con estado `pendiente`, calcula el diff contra la versión anterior, genera embeddings
  para los cambios y deja trazabilidad.
- En producción, `documentos_pendientes` debe alimentarse con Auto Loader desde snapshots
  inmutables en un Volume. La detección de una nueva versión se basa en `norma_id`, `version` y
  `content_hash`; el job no debe volver a procesar una fila marcada como `procesado`.
- `workflow-config.json` define el patrón Lakeflow Jobs: una tarea SQL descubre el control-plane y
  una tarea `For each` ejecuta una instancia por snapshot. Así, el conjunto de documentos puede
  crecer sin editar el notebook.

### Dónde viven los subagentes y el supervisor

`agent-config.yaml` es el contrato versionado de roles, instrucciones, herramientas y permisos:

- `historial`: identifica versiones y vigencia.
- `comparador`: produce cambios por artículo y severidad.
- `corroborador`: devuelve evidencia oficial y metadatos de fuente.
- `supervisor`: enruta la pregunta y exige evidencia antes de responder.

La configuración operativa se materializa en Databricks desde **Agents → Create Agent → Supervisor
Agent**, conectando agentes especializados, endpoints, UC Functions, Genie o MCP según corresponda.
El notebook conserva un supervisor lógico mínimo para validar el flujo sin depender de IDs de
recursos externos; la creación de los recursos administrados requiere permisos de workspace y
`EXECUTE` sobre las funciones/endpoints involucrados.

## 1. Objetivo

Construir un asistente que ayude a revisar cambios en normativa SBS: localizar normas,
listar versiones, comparar artículos entre versiones, explicar el impacto documental y
corroborar respuestas contra fuentes oficiales. El asistente informa y cita; no reemplaza
asesoría legal, regulatoria o financiera.

## 2. Pregunta de negocio

> ¿Qué cambió entre la versión anterior y la vigente de una norma SBS y qué debería revisar
> una persona responsable de cumplimiento?

Caso principal: **Reglamento de Gobierno Corporativo y de la Gestión Integral de Riesgos,
Resolución SBS N.° 272-2017**, cuyo historial oficial muestra múltiples normas modificatorias
y una versión vigente reportada por el buscador SBS.

## 3. Alcance de la primera versión

Incluye:

- Ingesta de un corpus pequeño de documentos y artículos versionados.
- Metadatos de norma, versión, estado, fechas, sistema y URL oficial.
- Diff estructurado por artículo: agregado, eliminado, modificado o sin cambio.
- Embeddings de cambios para recuperar diferencias por significado.
- Herramientas de solo lectura para buscar, comparar y corroborar.
- Router/agente auditable con allowlist, validación y trazas.
- Respuesta con evidencia, versión, artículo y advertencia de alcance.

Fuera de alcance en esta iteración: interpretación jurídica definitiva, extracción universal
de cualquier PDF, crawling agresivo, acciones de escritura en sistemas externos, asesoría a
una entidad concreta y automatización de cumplimiento.

## 4. Arquitectura full stack Databricks

```text
Fuente oficial SBS / fixture didáctico
        ↓
Volume o landing + Delta raw
        ↓
ai_parse_document / normalización / metadatos
        ↓
Delta documentos → Delta artículos_versionados
        ↓
Diff determinista por norma + artículo
        ↓
Embeddings en endpoint de Model Serving
        ↓
Delta cambios_embeddings + recuperación semántica
        ↓
UC Functions / herramientas de solo lectura
        ↓
Router/agente → respuesta citada + tabla de trazas
```

Componentes Databricks:

- **Unity Catalog:** catálogo del alumno, schemas y gobierno de tablas/funciones.
- **Delta Lake:** tablas `documentos`, `articulos_versionados`, `cambios_normativos`,
  `trazas_agente`.
- **Volumes:** destino previsto para PDFs y anexos reales.
- **AI Functions:** `ai_parse_document` como ruta opcional para PDF y `ai_query` para
  embeddings y resumen controlado.
- **Model Serving:** endpoint configurable; por defecto `databricks-qwen3-embedding-0-6b`.
- **UC Functions:** contrato de herramientas; en la primera ejecución se demuestran como
  funciones Python/SQL read-only y se deja el contrato listo para registrarlas.
- **Agent loop:** planificar → validar → ejecutar tool → observar → redactar.
- **Observabilidad:** cada pregunta, tool, argumentos, evidencia y resultado se persiste.

## 5. Modelo de datos

| Tabla | Grano | Campos clave |
|---|---|---|
| `documentos` | una versión de norma | `norma_id`, `version`, `estado`, fechas, URL |
| `articulos_versionados` | un artículo de una versión | `norma_id`, `version`, `articulo`, `texto` |
| `cambios_normativos` | diferencia entre dos versiones | `norma_id`, `version_anterior`, `version_nueva`, `tipo_cambio`, textos |
| `cambios_embeddings` | un cambio con vector | `cambio_id`, `texto_busqueda`, `embedding` |
| `trazas_agente` | una ejecución del agente | pregunta, plan, tool, evidencia, resultado, timestamp |

## 6. Contratos de herramientas

- `buscar_norma(texto, sistema=None)`: devuelve normas coincidentes con fuente y estado.
- `listar_versiones(norma_id)`: devuelve versiones y normas modificatorias conocidas.
- `comparar_versiones(norma_id, version_anterior, version_nueva)`: devuelve diff por artículo.
- `buscar_cambios(pregunta, top_k)`: recupera cambios relevantes por embedding.
- `corroborar(norma_id, articulo, version)`: confirma si el artículo está presente en la
  versión solicitada y devuelve la evidencia exacta.

Reglas: solo lectura, argumentos validados, sin SQL arbitrario generado por el modelo,
límite de pasos y abstención cuando no hay fuente o versión verificable.

## 7. Criterios de aceptación

1. El notebook crea tablas Delta en el catálogo indicado.
2. El diff detecta al menos un artículo modificado, uno agregado y uno eliminado.
3. El embedding devuelve dimensión estable y se puede recuperar el cambio esperado.
4. Las herramientas devuelven resultados reproducibles fuera del loop del agente.
5. El agente responde con norma, versiones, artículo y evidencia.
6. Se registra una traza por pregunta, tool y resultado.
7. El notebook demuestra abstención ante una norma o versión inexistente.
8. Los fixtures didácticos están marcados y no se presentan como texto legal consolidado.

## 8. Riesgos y mitigaciones

- **PDF escaneado o layout complejo:** conservar binario, registrar fallo de parsing y usar
  revisión humana; no inventar texto.
- **Versión vigente ambigua:** consultar historial oficial y mostrar fecha/estado; abstenerse
  si hay conflicto.
- **Cambio semántico sutil:** combinar diff literal, recuperación semántica y corroboración.
- **Alucinación regulatoria:** exigir citas y responder “no encontrado” cuando no hay evidencia.
- **Datos sensibles:** usar fuentes públicas y no incorporar expedientes de clientes.

## 9. Próxima iteración

Reemplazar los fixtures por PDFs descargados desde URLs SBS permitidas, conservar hashes,
extraer artículos con `ai_parse_document`, medir precisión del diff sobre un gold set y
registrar las herramientas como UC Functions con permisos de mínimo privilegio.

## Fuentes de contexto

- [Buscador oficial SBS](https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/BusquedaPortal.aspx)
- [Historial de la Resolución SBS 272-2017](https://www.sbs.gob.pe/app/pp/INT_CN/Paginas/Busqueda/VerHistorial.aspx?NormaId=1708)
- [Normativa SBS](https://www.sbs.gob.pe/normativa-y-estandares/normativa/normativa-sbs)
