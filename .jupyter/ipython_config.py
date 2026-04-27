import os

openai_model = os.getenv("JUPYTER_AI_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))

c.AiMagics.default_language_model = f"openai-chat:{openai_model}"
