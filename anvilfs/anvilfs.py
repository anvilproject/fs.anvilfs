from .clientrepository import ClientRepository
from .basefolder import BaseAnVILFolder
from .google import DRSAnVILFile
from .namespace import Namespace
from .workloadidentitycredentials import WorkloadIdentityCredentials

from google.auth.transport.requests import AuthorizedSession

from fs.base import FS
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected


class AnVILFS(FS, ClientRepository):
    DEFAULT_API_URL = "https://api.firecloud.org/api/"

    def __init__(self, namespace, workspace, api_url=None,
                 on_anvil=False, drs_url=None):
        super(AnVILFS, self).__init__()
        ClientRepository.base_project = namespace
        if not api_url:
            api_url = self.DEFAULT_API_URL
        # the API endpoint where DRS URI resolution requests are sent
        if drs_url:
            DRSAnVILFile.api_url = drs_url
        if on_anvil:
            scopes = ['https://www.googleapis.com/auth/userinfo.email',
                      'https://www.googleapis.com/auth/userinfo.profile',
                      'https://www.googleapis.com/auth/cloud-platform']
            credentials = WorkloadIdentityCredentials(scopes=scopes)
            self.fapi.__setattr__("__SESSION", AuthorizedSession(credentials))
            self.fapi.fcconfig.set_root_url(api_url)
        self.namespace = Namespace(namespace, [workspace])
        self.workspace = self.namespace[workspace+"/"]
        self.rootobj = self.workspace  # edit to make namespace root

    def getinfo(self, path, namespaces=None):
        return self.rootobj.get_object_from_path(path).getinfo()

    # Get a list of resource names (str) in a directory.
    def listdir(self, path):
        if path == "/" or path == "":
            return self.rootobj.keys()
        try:
            maybe_dir = self.rootobj.get_object_from_path(path)
        except KeyError:
            raise ResourceNotFound("Resource {} not found".format(path))
        if isinstance(maybe_dir, BaseAnVILFolder):
            return maybe_dir.keys()
        else:
            raise DirectoryExpected(f"{path}")

    def scandir(self, path):
        if path[-1] != "/":
            path = path + "/"
        result = []
        dirlist = self.listdir(path)
        for o in dirlist:
            result.append(self.getinfo(path+o))
        return result

    def makedir():  # Make a directory.
        raise Exception("makedir not implemented")

    def openbin(self, path, mode="r", buffering=-1, **options):
        obj = self.rootobj.get_object_from_path(path)
        try:
            return obj.get_bytes_handler()
        except AttributeError:
            raise FileExpected(
                "Error: requested object is not a file:\n  {}".format(path))

    def remove():  # Remove a file.
        raise Exception("remove not implemented")

    def removedir():  # Remove a directory.
        raise Exception("removedir not implemented")

    def setinfo():  # Set resource information.
        raise Exception("setinfo not implemented")
