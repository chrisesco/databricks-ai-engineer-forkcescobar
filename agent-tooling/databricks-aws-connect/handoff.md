# Handoff para el agente de los estudiantes

Tu tarea es ayudar al estudiante a completar y verificar su propia conexión desde un LLM de
terminal (Codex, Claude Code u otro agente con shell) hacia Databricks on AWS.

Lee primero `SKILL.md` en esta misma carpeta y aplica sus invariantes. La arquitectura esperada es:

```text
agente de terminal
        |
        | ejecuta Databricks CLI con --profile
        v
perfil local del estudiante (~/.databrickscfg / sesión OAuth)
        |
        | HTTPS, autenticación unificada
        v
workspace Databricks on AWS del estudiante
```

## Qué debes solicitar

Solicita solo datos no secretos:

1. Host del workspace, con forma `https://dbc-....cloud.databricks.com`.
2. Nombre de perfil local elegido, recomendado: `curso-databricks`.
3. ID de warehouse únicamente si el profesor asignó uno.

Nunca solicites que el estudiante pegue un PAT, contraseña, cookie, código OAuth ni el contenido
de `~/.databrickscfg`. Si comparte accidentalmente un secreto, no lo reproduzcas: indícale que lo
revoque/rote y que elimine el mensaje o archivo donde apareció.

## Secuencia de asistencia

1. Confirma `databricks version`. Si el comando no existe, detente e informa que falta instalar el
   Databricks CLI moderno; no improvises una instalación global sin autorización.
2. Revisa perfiles con `databricks auth profiles`. Esa salida contiene nombre, host y validez, no
   tokens. No uses la opción `--sensitive` en ningún comando.
3. Si hace falta login, entrega este comando con el host y perfil del estudiante:

   ```bash
   databricks auth login --host "<host>" --profile "<perfil>"
   ```

   El estudiante completa el navegador. No le pidas que narre ni copie el flujo de autenticación.
4. Desde la raíz de esta carpeta ejecuta:

   ```bash
   ./scripts/verify-connection.sh "<perfil>"
   ```

   Con warehouse asignado:

   ```bash
   DATABRICKS_WAREHOUSE_ID="<id>" ./scripts/verify-connection.sh "<perfil>"
   ```

5. Si falla, clasifica sin exponer secretos:

   - Perfil inexistente/no válido: repetir `databricks auth login` con el perfil correcto.
   - Host inesperado: crear/corregir un perfil para el workspace del curso; no editar el perfil de
     otro proyecto.
   - `401`: sesión expirada o inválida; renovar login.
   - `403`: conexión lograda, pero faltan permisos; pedir al profesor acceso al workspace o al
     recurso indicado.
   - Warehouse no visible: verificar el ID y que el estudiante tenga `CAN USE`; no arrancarlo.
   - Error de red/DNS/TLS: revisar conectividad, VPN o proxy antes de cambiar credenciales.

## Criterio de terminado

La conexión queda verificada cuando el script confirma autenticación y listado de warehouses. Si
se proporcionó un warehouse, también debe confirmar que ese ID es visible. Reporta al estudiante:

- perfil usado;
- host del workspace;
- checks aprobados;
- siguiente bloqueo concreto, si existe.

No reportes el usuario autenticado, tokens, cabeceras HTTP ni contenido de archivos de credenciales.
No ejecutes notebooks, SQL, creación de recursos ni arranque de compute como parte de este handoff.
