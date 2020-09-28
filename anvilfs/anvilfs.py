from .base import BaseAnVILFolder
from .namespace import Namespace
from .workspace import Workspace

from fs.base import FS
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected

import datetime
from google.auth import credentials
import json
import firecloud.api as fapi
from google.auth.transport.requests import AuthorizedSession

class WorkloadIdentityCredentials(credentials.Scoped, credentials.Credentials):
    def __init__(self, scopes):
        super(WorkloadIdentityCredentials, self).__init__()
        print(f"Init with scopes={scopes}")
        self._scopes = scopes

    def with_scopes(self, scopes):
        return WorkloadIdentityCredentials(scopes=scopes)

    @property
    def requires_scopes(self):
        return False

    def refresh(self, request):
        print(f"Refresh with scopes={self._scopes}")
        url = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token'
        if self._scopes:
            url += '?scopes=' + ','.join(self._scopes)
        response = request(url=url, method="GET", headers={
                           'Metadata-Flavor': 'Google'})
        if response.status == 200:
            response_json = json.loads(response.data)
        else:
            raise RuntimeError('bad status from metadata server')
        self.token = response_json['access_token']
        self.expiry = datetime.datetime.utcnow(
        ) + datetime.timedelta(seconds=response_json['expires_in'])


class AnVILFS(FS):
    def __init__(self, namespace, workspace):
        super(AnVILFS, self).__init__()
        # hax
        scopes = ['https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/cloud-platform']
        credentials = WorkloadIdentityCredentials(scopes=scopes)
        fapi.__setattr__("__SESSION", AuthorizedSession(credentials))
        fapi.fcconfig.set_root_url("https://firecloud-orchestration.dsde-dev.broadinstitute.org/api/")
#        fapi.list_workspaces()
        # /hax
        self.namespace = Namespace(namespace)
        self.workspace = self.namespace.fetch_workspace(workspace)
        self.rootobj = self.workspace  # leaving the option to make namespace root

    def getinfo(self, path, namespaces=None):
        return self.rootobj.get_object_from_path(path).getinfo()

    # Get a list of resource names (str) in a directory.
    def listdir(self, path):
        if path == "/" or path == "":
            return self.rootobj.keys()
        try:
            maybe_dir = self.rootobj.get_object_from_path(path)
        except KeyError as ke:
            raise ResourceNotFound("Resource {} not found".format(path))
        if isinstance(maybe_dir, BaseAnVILFolder):
            return maybe_dir.keys()
        else:
            raise DirectoryExpected("{} is not a directory".format(path))

    def scandir(self, path):
        if path[-1] != "/":
            path = path + "/"
        result = []
        l = self.listdir(path)
        for o in l:
            result.append(self.getinfo(path+o))
        return result

    def makedir():  # Make a directory.
        raise Exception("makedir not implemented")

    def openbin(self, path, mode="r", buffering=-1, **options):
        obj = self.rootobj.get_object_from_path(path)
        try:
            return obj.get_bytes_handler()
        except AttributeError as e:
            raise FileExpected(
                "Error: requested object is not a file:\n  {}".format(path))

    def remove():  # Remove a file.
        raise Exception("remove not implemented")

    def removedir():  # Remove a directory.
        raise Exception("removedir not implemented")

    def setinfo():  # Set resource information.
        raise Exception("setinfo not implemented")
    # for network systems, scandir needed otherwise default calls a combination of listdir and getinfo for each file.
