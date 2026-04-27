import os

openai_model = os.getenv("JUPYTER_AI_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
openai_embeddings_model = os.getenv("JUPYTER_AI_EMBEDDINGS_MODEL", "text-embedding-3-small")

c.AiExtension.default_language_model = f"openai-chat:{openai_model}"
c.AiExtension.default_completions_model = f"openai-chat:{openai_model}"
c.AiExtension.default_embeddings_model = f"openai:{openai_embeddings_model}"
c.AiExtension.allowed_providers = ["openai", "openai-chat"]

if os.getenv("OPENAI_API_KEY"):
    c.AiExtension.default_api_keys = {"OPENAI_API_KEY": os.environ["OPENAI_API_KEY"]}
