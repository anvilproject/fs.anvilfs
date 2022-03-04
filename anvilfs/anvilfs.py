from .clientrepository import ClientRepository
from .basefolder import BaseAnVILFolder
from .google import DRSAnVILFile
from .namespace import Namespace
from .workloadidentitycredentials import WorkloadIdentityCredentials
from .workspacebucket import WorkspaceBucket, WorkspaceBucketSubFolder

from google.auth.transport.requests import AuthorizedSession

from fs.base import FS
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected


class AnVILFS(FS, ClientRepository):
    DEFAULT_API_URL = "https://api.firecloud.org/api/"

    def __init__(self, namespace, workspace, api_url=None,
                 on_anvil=False, drs_url=None):
        super(AnVILFS, self).__init__()
        # ensure there is a firecloud api session to mine for info
        self.fapi._set_session()
        fapi_project = self.fapi.__getattribute__("__SESSION").credentials.quota_project_id
        if fapi_project:
            ClientRepository.base_project = fapi_project
        # deal with custom API URL
        if not api_url:
            api_url = self.DEFAULT_API_URL
        else:
            self.fapi.fcconfig.set_root_url(api_url)
        # the API endpoint where DRS URI resolution requests are sent
        if drs_url:
            DRSAnVILFile.api_url = drs_url
        if on_anvil:
            scopes = ['https://www.googleapis.com/auth/userinfo.email',
                      'https://www.googleapis.com/auth/userinfo.profile',
                      'https://www.googleapis.com/auth/cloud-platform']
            credentials = WorkloadIdentityCredentials(scopes=scopes)
            self.fapi.__setattr__("__SESSION", AuthorizedSession(credentials))
        
        self.namespace = Namespace(namespace, [workspace])
        self.workspace = self.namespace[workspace+"/"]
        # if OWNER or PROJECT_OWNER
        if self.workspace.access_level.lower() in ("owner", "project_owner"):
            ClientRepository.workspace_project = self.workspace.google_project
        # set workspace to root dir
        self.rootobj = self.workspace

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

    def isdir(self, path):
        if path == "" or path[-1] != "/":
            path += "/"
        try:
            return self.getinfo(path).is_dir
        except ResourceNotFound:
            return False

    def scandir(self, path, **kwargs):
        if path[-1] != "/":
            path = path + "/"
        result = []
        dirlist = self.listdir(path)
        for o in dirlist:
            result.append(self.getinfo(path+o))
        return result

    def upload(self, path, file, **kwargs):
        cleaved = path.rsplit('/', 1)
        root_dir = cleaved[0] + "/"
        fname = cleaved[1]
        root_obj = self.rootobj.get_object_from_path(root_dir)
        root_obj.upload(fname, file)

    def makedir(self, path, **kwargs):  # Make a directory.
        raise Exception("makedir not implemented")

    def makedirs(self, path, **kwargs):  # Make directories.
        # google bucket 'directories' are just components of a filename,
        #   e.g. you cannot have an empty directory. so, making
        #   directories is entirely local to the plugin until file upload.
        #   here we just go through the objects to ensure that the last
        #   existent directory is a google bucket location and make the
        #   subdirs, but upload must 'create' the subdirs itself
        # strip leading and lagging slashes
        if path and path[0] == "/":
            path = path[1:]
        if path and path[-1] == "/":
            path = path[:-1]
        root_obj = self.rootobj
        dirs = path.split("/")
        if "" in dirs:
            raise Exception("Empty directory name in path " + 
                f"{path} not supported")
        for d in dirs:
            try:
                root_obj = root_obj[d + "/"]
            except KeyError:
                t = type(root_obj)
                if  t != WorkspaceBucket and t != WorkspaceBucketSubFolder:
                    raise Exception("Only workspace bucket files are writable")
                prev_bucketpath = root_obj.bucketpath
                bucketname = root_obj.bucket_name
                new_dir = WorkspaceBucketSubFolder(d, prev_bucketpath + d + "/",
                    bucketname)
                root_obj[d + "/"] = new_dir
                root_obj = root_obj[d + "/"]
        return
                
        

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
