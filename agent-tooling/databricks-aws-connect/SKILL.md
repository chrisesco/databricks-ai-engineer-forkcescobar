---
name: databricks-aws-connect
description: Conecta y verifica un agente de terminal como Codex o Claude Code con un workspace de Databricks on AWS usando un perfil local del Databricks CLI, sin guardar ni versionar credenciales en el repositorio.
---

# Conectar un agente de terminal a Databricks on AWS

Usa el Databricks CLI y su autenticación unificada. El repositorio contiene instrucciones y un
verificador de solo lectura; la credencial pertenece al usuario y vive fuera del repositorio.

## Invariantes de seguridad

- Nunca pidas, leas, imprimas, copies ni commits tokens, contraseñas o el contenido de
  `~/.databrickscfg`.
- Nunca uses `databricks auth describe --sensitive`.
- No crees `.env`, `.databrickscfg`, `*.pat`, `secrets.json` ni `terraform.tfvars` dentro del repo.
- Usa un perfil propio del estudiante. No reutilices ni exportes perfiles del profesor.
- Antes de hacer una mutación, explica el efecto y solicita confirmación. La verificación inicial
  debe ser de solo lectura y no debe iniciar compute.

## Flujo

1. Comprueba que existe el CLI moderno:

   ```bash
   databricks version
   ```

2. Obtén del estudiante únicamente estas coordenadas no secretas:

   - URL HTTPS de su workspace de Databricks on AWS.
   - Nombre local que desea para el perfil, por ejemplo `curso-databricks`.
   - Opcional: ID del SQL warehouse asignado por el profesor.

3. Si el perfil aún no existe o no es válido, indica al estudiante que ejecute personalmente:

   ```bash
   databricks auth login --host "https://dbc-XXXXXXXX.cloud.databricks.com" --profile "curso-databricks"
   ```

   Este comando abre el login OAuth y guarda la sesión en la configuración local del usuario.
   El agente no necesita ver la credencial.

4. Verifica desde esta carpeta:

   ```bash
   ./scripts/verify-connection.sh curso-databricks
   ```

   Para comprobar además que un warehouse asignado es visible, sin arrancarlo:

   ```bash
   DATABRICKS_WAREHOUSE_ID="<warehouse-id>" \
     ./scripts/verify-connection.sh curso-databricks
   ```

5. Interpreta el resultado:

   - `OK autenticación`: el perfil puede llamar la API del workspace.
   - `OK warehouses`: el usuario puede listar SQL warehouses.
   - `OK warehouse asignado`: el ID indicado es visible para ese usuario.
   - Un `401`/`403` requiere renovar login o permisos; no requiere compartir el token.
   - Un warehouse `STOPPED` sigue siendo una conexión válida. No lo inicies durante este chequeo.

6. Entrega un resumen que incluya perfil, host y verificaciones aprobadas, pero ninguna identidad,
   token o salida sensible. Para asistir a un estudiante, lee primero [handoff.md](handoff.md).

## Uso posterior

Pasa el perfil explícitamente para evitar conectar al workspace equivocado:

```bash
databricks current-user me -p "curso-databricks"
databricks warehouses list -p "curso-databricks"
```

En código que soporte Databricks Unified Authentication, usa
`DATABRICKS_CONFIG_PROFILE=curso-databricks` solo en la sesión de terminal. No escribas ese valor
en archivos del repositorio si el nombre del perfil identifica a una persona o entorno privado.
