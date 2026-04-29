import sys

sys.path.insert(0, "/workspace/.jupyter")

c.ServerApp.jpserver_extensions = {
    "jupyter_ai": False,
    "jupyter_ai_disabled_routes": True,
}
