from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from tornado import web


class DisabledAiChatsHandler(APIHandler):
    @web.authenticated
    def get(self):
        self.finish({"chats": []})


class DisabledAiCompletionsHandler(APIHandler):
    @web.authenticated
    def get(self):
        self.finish({})


def _jupyter_server_extension_points():
    return [{"module": "jupyter_ai_disabled_routes"}]


def load_jupyter_server_extension(server_app):
    base_url = server_app.web_app.settings["base_url"]
    handlers = [
        (url_path_join(base_url, "api/ai/chats"), DisabledAiChatsHandler),
        (url_path_join(base_url, "api/ai/completions"), DisabledAiCompletionsHandler),
        (url_path_join(base_url, "api/ai/completion/inline"), DisabledAiCompletionsHandler),
    ]
    server_app.web_app.add_handlers(".*$", handlers)
