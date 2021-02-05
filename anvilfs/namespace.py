from .basefolder import BaseAnVILFolder
from .workspace import Workspace


class Namespace(BaseAnVILFolder):
    def __init__(self, namespace_name, workspaces=[]):
        super().__init__(namespace_name)
        self.workspace_names = workspaces

    def lazy_init(self):
        for ws in self.workspace_names:
            self.fetch_workspace(ws)

    def fetch_workspace(self, workspace_name):
        ws = Workspace(self.name, workspace_name)
        self[ws.name] = ws
        return ws

    def __str__(self):
        out = "<Namespace {}>".format(self.name)
        # namespace can only have workspace folders, so keys are workspaces
        for ws in self.keys():
            out += "\n - {}".format(str(ws))
        return out
