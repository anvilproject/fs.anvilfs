import firecloud.api as fapi

from .base import BaseAnVILFolder
from .bucket import WorkspaceBucket

class Workspace(BaseAnVILFolder):
    def __init__(self, namespace_reference,  workspace_name):
        self.namespace = namespace_reference
        resp = self.fetch_api_info(workspace_name)
        super().__init__(workspace_name, resp["workspace"]["lastModified"])
        baf = BaseAnVILFolder("Other Data/") 
        self[baf] = None
        baf[WorkspaceBucket(resp["workspace"]["bucketName"])] = None
        self.bucket_name = resp["workspace"]["bucketName"]

    def fetch_api_info(self, workspace_name):
        fields = "workspace.attributes,workspace.bucketName,workspace.lastModified"
        return fapi.get_workspace(namespace=self.namespace.name, workspace=workspace_name, fields=fields).json()
