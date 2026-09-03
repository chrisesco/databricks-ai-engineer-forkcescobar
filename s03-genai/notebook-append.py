# Databricks notebook source
# MAGIC %md
# MAGIC # Laboratorio separado · Parámetros con DeepSeek
# MAGIC **Databricks AI Engineer — S03**
# MAGIC
# MAGIC Este notebook es independiente del laboratorio principal de S03. Compara `temperature`,
# MAGIC `top_p` y `max_tokens` usando la API OpenAI-compatible de DeepSeek.
# MAGIC
# MAGIC ## Configuración de la key — solo la primera vez
# MAGIC
# MAGIC La key **no se escribe en ninguna celda**. Debe guardarse en un Secret Scope de Databricks.
# MAGIC Un administrador puede crear el scope desde la CLI:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope deepseek
# MAGIC databricks secrets put-secret deepseek DEEPSEEK_API_KEY
# MAGIC ```
# MAGIC
# MAGIC El segundo comando solicitará el valor de la key sin guardarlo en el repositorio. Si el scope
# MAGIC ya existe, ejecuta únicamente `put-secret`. También se puede configurar desde **Workspace
# MAGIC Settings → Secrets** si tu workspace ofrece esa interfaz.
# MAGIC
# MAGIC Requisitos: permiso para leer el scope `deepseek` y conectividad de salida HTTPS hacia
# MAGIC `https://api.deepseek.com`. No compartas el notebook con un runtime que tenga la key impresa.

# COMMAND ----------

# MAGIC %md ## 0 · Cliente seguro
# MAGIC
# MAGIC Ejecuta primero esta celda de instalación. Databricks puede solicitar reiniciar el intérprete;
# MAGIC si lo hace, vuelve a ejecutar desde la celda de importación. No usamos `restartPython()` dentro
# MAGIC del flujo porque interrumpe la ejecución de las celdas siguientes.

# COMMAND ----------

%pip install -q openai

# COMMAND ----------

from openai import OpenAI

SCOPE = "deepseek"
KEY_NAME = "DEEPSEEK_API_KEY"
api_key = dbutils.secrets.get(scope=SCOPE, key=KEY_NAME).strip()
assert api_key and len(api_key) >= 20, "El secreto DeepSeek está vacío o incompleto."

client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
MODELO = "deepseek-chat"
print("✅ Cliente listo. La key se obtuvo desde Secret Scope y no se imprime.")

# COMMAND ----------

# MAGIC %md ## 1 · Misma tarea, tres configuraciones
# MAGIC
# MAGIC Pedimos cinco nombres para que la diversidad sea observable. La temperatura no garantiza
# MAGIC respuestas diferentes: modifica el muestreo. `top_p` limita la masa de probabilidad considerada
# MAGIC y `max_tokens` limita la longitud máxima de salida.

# COMMAND ----------

pregunta = """Propón exactamente 5 nombres distintos para una aplicación que ayuda a organizar tareas personales.
Devuelve una lista numerada. Cada nombre debe ser diferente de los demás, no uses Nimbus y añade una breve explicación de cada propuesta."""

configuraciones = [
    {"nombre": "estable", "temperature": 0.0, "top_p": 0.9, "max_tokens": 240},
    {"nombre": "balanceada", "temperature": 0.5, "top_p": 0.9, "max_tokens": 240},
    {"nombre": "creativa", "temperature": 0.9, "top_p": 0.95, "max_tokens": 240},
]

resultados = []
for cfg in configuraciones:
    try:
        r = client.chat.completions.create(
            model=MODELO,
            messages=[{"role": "user", "content": pregunta}],
            temperature=cfg["temperature"],
            top_p=cfg["top_p"],
            max_tokens=cfg["max_tokens"],
        )
        resultados.append({
            **cfg,
            "respuesta": r.choices[0].message.content,
            "finish_reason": r.choices[0].finish_reason,
            "prompt_tokens": r.usage.prompt_tokens,
            "completion_tokens": r.usage.completion_tokens,
            "error": None,
        })
    except Exception as exc:
        resultados.append({**cfg, "respuesta": None, "finish_reason": None,
                           "prompt_tokens": None, "completion_tokens": None,
                           "error": f"{type(exc).__name__}: {exc}"})

for x in resultados:
    print(f"\n--- {x['nombre']} · temp={x['temperature']} · top_p={x['top_p']} · max_tokens={x['max_tokens']}")
    print(x["respuesta"])
    print("tokens:", x["prompt_tokens"], "→", x["completion_tokens"], "| finish:", x["finish_reason"])
    if x["error"]:
        print("ERROR:", x["error"])

assert any(x["respuesta"] for x in resultados), "Ninguna configuración devolvió respuesta. Revisa el error mostrado."

# COMMAND ----------

# MAGIC %md ## 2 · Lectura del resultado
# MAGIC
# MAGIC - `finish_reason = stop`: el modelo terminó naturalmente.
# MAGIC - `finish_reason = length`: alcanzó el límite `max_tokens` y pudo quedar truncado.
# MAGIC - La temperatura puede cambiar el estilo, pero no garantiza diversidad.
# MAGIC - Para tareas creativas se puede tolerar más variación; para extracción o clasificación conviene
# MAGIC   medir estabilidad y usar instrucciones y formatos explícitos.
# MAGIC
# MAGIC **Seguridad:** antes de compartir el notebook, confirma que no contiene el valor de la key y que
# MAGIC cada usuario tiene su propio secreto o permiso controlado sobre el scope.
