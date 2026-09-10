# Subagentes y Supervisor Agent

## Qué se configura dónde

### Pipeline dinámica

Se configura en `workflow-config.json` como Lakeflow Job:

1. `discover_pending` consulta la tabla de control.
2. `process_each_snapshot` usa `For each` con `{{tasks.discover_pending.output.rows}}`.
3. El notebook anidado recibe `{{input.doc_id}}`.

La lista de documentos no está en el código. Se modifica la tabla `documentos_pendientes`.

### Subagentes

Se definen como especialistas con instrucciones, herramientas y permisos propios. En esta
primera iteración quedan declarados en `agent-config.yaml`:

- `historial`: versiones y vigencia.
- `comparador`: diferencias por artículo.
- `corroborador`: fuente, hash y evidencia.

Para producción, cada subagente puede ser un endpoint de agente/Knowledge Assistant, una
Databricks App o un custom agent desplegado en Model Serving. Sus accesos se conceden por
separado; un subagente no hereda permisos por estar dentro del supervisor.

### Supervisor Agent

En Databricks se configura en **Agents → Create Agent → Supervisor Agent**. Allí se agregan
subagentes y tools, se escriben instrucciones, se prueban preguntas y se administran permisos.

La alternativa programática usa `WorkspaceClient().supervisor_agents` para crear el supervisor
y `create_tool` para registrar subagentes. La alternativa custom es una Databricks App con un
router propio que trata cada subagente como una tool.

## Decisión para S05

El notebook ejecutado enseña primero un supervisor lógico transparente, con allowlist y tools
read-only. El Supervisor Agent real se provisiona después de publicar los tres endpoints
especialistas y conceder `CAN_QUERY`; esto evita crear recursos externos no reproducibles desde
el notebook.

## Permisos mínimos

- Unity Catalog Function: `EXECUTE`.
- Tabla: `USE CATALOG`, `USE SCHEMA`, `SELECT`.
- AI Search: `SELECT` sobre el índice.
- Volume: `READ VOLUME`.
- Endpoint de agente: `CAN_QUERY`.
- Databricks App llamada por otra app: `CAN_USE`.
