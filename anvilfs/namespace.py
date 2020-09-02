from .base import BaseAnVILFolder
from .workspace import Workspace

class Namespace(BaseAnVILFolder):
    def __init__(self, namespace_name):
        super().__init__(namespace_name)

    def fetch_workspace(self, workspace_name):
        ws = Workspace(self, workspace_name)
        self[ws.name] = ws
        return ws

    def __str__(self):
        out = "<Namespace {}>".format(self.name)
        for ws in self.workspaces:
            out += "\n{}".format(str(ws))