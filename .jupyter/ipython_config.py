import os
import sys

sys.path.insert(0, "/workspace")

openai_model = os.getenv("JUPYTER_AI_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))

c.AiMagics.default_language_model = f"openai-chat:{openai_model}"
c.InteractiveShellApp.exec_lines = [
    "from localbricks.startup import ensure_notebook_spark",
    "ensure_notebook_spark()",
]
