# Sesión 03 · Fundamentos de IA Generativa
**miércoles 2 de septiembre · 3 h**

## Qué vas a lograr
Entender los LLMs y hacer tus primeras llamadas productivas sobre los datos que ya cargaste.

## Temas
- Qué es la IA generativa: tokens, ventana de contexto, grounding
- Cómo elegir un modelo por tarea, costo y latencia
- Foundation Model APIs y AI Playground; prompt engineering
- Llamar modelos desde SQL con `ai_query`
- Genie Spaces: preguntarle a tus datos en lenguaje natural

## Material

[![Abrir parámetros DeepSeek en Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/manuelarguelles/databricks-ai-engineer-alumnos/blob/main/s03-genai/S03-parametros-deepseek-colab.ipynb)

- `notebook.py`: laboratorio principal de S03.
- `slides-S03.html`: slides de la sesión.
- `S03-append.html`: apéndice arquitectónico sobre el framework de 9 capas y Foundry vs. Databricks.
- `S03-parametros-deepseek-colab.ipynb`: laboratorio opcional para comparar parámetros desde Google Colab. Configura `DEEPSEEK_API_KEY` en Colab Secrets; nunca pegues la key en una celda.
- `notebook-append.py`: laboratorio opcional para ejecutar la comparación desde Databricks. Requiere un Secret Scope `deepseek` y conectividad hacia `api.deepseek.com`; si el workspace bloquea esa salida, usa Colab.

## Tu entregable
Se publica al comenzar la sesión, junto con el notebook.

---
📅 *El material de esta sesión se sube unos días antes de la clase.*
Mientras tanto, asegurate de tener cerrado el entregable de la sesión anterior:
cada sesión se apoya en la que viene antes.
