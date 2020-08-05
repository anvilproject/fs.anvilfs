"""
AnVILFS: AnVIL filesystem for Python

Visualization of AnVIL file abstraction, mirroring anvil.terra.bio layout:
? = possibly more types in that level
root: Workspace
  - Tables (entities)
    - BigQuery_table
    - cohort
    - participant
    - ?
  - Reference Data ()
  - Other Data (user bucket files & k/v pairs)
    - Workspace data (K/V pairs) (TODO: how get?)
    - Files (bucket files)
    - ?
  - ?


"""

# std
import threading
from io import BytesIO
#firecloud
import firecloud.api as fapi
# pyfilesystem2
from fs.base import FS
from fs.enums import ResourceType
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected
from fs.info import Info
# google
from google.cloud import storage



"""
Namespace == Google project name
"""
class BaseAnVILResource:
    def __init__(self):
        self.folders = {}

    def get_info(self):
        _result = {
            "basic": {
                "name": self.name,
                "is_dir": self.is_dir
            },
            "details": {}
        }
        _details = _result["details"]
        if self.is_dir:
            _details["type"] = ResourceType.directory
        else:
            _details["type"] = ResourceType.file
            _details["size"] = self.size
        return Info(_result)

    def make_folder_info(self, name):
        return Info({
            "basic": {
                "name": name,
                "is_dir": True
            },
            "details": {
                "type": ResourceType.directory
            }})

class Namespace(BaseAnVILResource):
    def __init__(self, namespace_name):
        super().__init__()
        self.name = namespace_name
        self.is_dir = True

    def fetch_workspace(self, workspace_name):
        self.folders[workspace_name] = Workspace(self, workspace_name)

    def __str__(self):
        out = "<Namespace {}>".format(self.name)
        for ws in self.workspaces:
            out += "\n{}".format(str(ws))

class Workspace(BaseAnVILResource):
    def __init__(self, namespace_reference,  workspace_name):
        super().__init__()
        self.namespace = namespace_reference
        self.name = workspace_name
        self.is_dir = True
        self.fetch_api_info()

    def __str__(self):
        out = "  <Workspace {}>".format(self.name)
        #for objects in entities ...

    def fetch_api_info(self):
        fields = "workspace.attributes,workspace.bucketName,workspace.lastModified"
        resp = fapi.get_workspace(namespace=self.namespace.name, workspace=self.name, fields=fields).json()
        self.attributes = resp["workspace"]["attributes"]
        self.bucket_name = resp["workspace"]["bucketName"]
        self.folders["Other Data/"] = WorkspaceBucket(resp["workspace"]["bucketName"])
        self.lastModified = resp["workspace"]["lastModified"]

    def get_object_from_path(self, path):
        print("gofp: {}".format(path))
        if path == "/":
            return self.folders
        idx = path.find("/", 1)
        print("gofpi: {}".format(idx))
        # if the next slash is the last slash, its a directory
        if idx == len(path) - 1 or idx < 0:
            return self.folders[path[1:]]
        print("gofp2: {}".format(path))
        first_subfolder = path[1:idx+1]
        remainder = path[idx:]
        print("{} - {} -".format(first_subfolder, remainder))
        return self.folders[first_subfolder].get_object_from_path(remainder)

    def get_info_from_path(self, path):
        obj = self.get_object_from_path(path)
        if isinstance(obj, dict) or obj.is_dir:
            return self.make_folder_info(path.split("/")[-2] + "/")
        return obj.get_info()
    
    def get_dirlist_from_path(self,path):
        pass


# google stores bucket files as a/b/c.extension;
#   so file dictionary stores { "a/b/":["c.extension", ...]}
class BucketFile(BaseAnVILResource):#, BytesIO):
    def __init__(self, blob):
        # super().__init__()
        self.name = blob.name
        self.size = blob.size
        self.blob_handle = blob
        self.is_dir = False

    def get_bytes_handler(self):
        print("get_bytes_handler getting called for {}".format(self.name))
        buffer = BytesIO()
        self.blob_handle.download_to_file(buffer)
        buffer.seek(0)
        return buffer


class WorkspaceBucket(BaseAnVILResource):
    def __init__(self, bucket_name):
        super().__init__()
        self.name = bucket_name
        self.is_dir = True
        google_bucket = storage.Client().get_bucket(bucket_name)
        blobs = google_bucket.list_blobs()
        #NOTE can generate signed urls from blobs with 'blob.generate_signed_url'
        #NOTE blobs are never just folders
        for blob in blobs:
            self.insert_file(blob)

    def __getitem__(self, item):
        return self.folders[item]

    def keys(self):
        return self.folders.keys()

    def insert_file(self, bucket_blob):
        bucket_file = bucket_blob.name
        file_size = bucket_blob.size
        print("inserting: {}".format(bucket_file))
        idx = bucket_file.find('/')
        # if there is no '/' its a file
        if idx < 0:
            self.folders[bucket_file] = BucketFile(bucket_blob)
        else:
            current_level = self.folders
            split = bucket_file.split("/")
            levels = split[:-1]
            file_name = split[-1]
            for level in levels:
                level = level + "/"
                if level not in current_level:
                    current_level[level] = {}
                current_level = current_level[level]
            current_level[file_name] = BucketFile(bucket_blob)

    def get_object_from_path(self, path):
        if path == "/":
            return self.folders
        levels = path.split("/")[1:]
        current_obj = self.folders
        for level in levels[:-1]:
            current_obj = current_obj[level + "/"]
        if levels[-1]:
            return current_obj[levels[-1]]
        else:
            return current_obj

    def get_info_from_path(self, path):
        obj = self.get_object_from_path(path)
        if isinstance(obj, dict) or obj.is_dir:
            return self.make_folder_info(path.split("/")[-2] + "/")
        return obj.get_info()


class Table(BaseAnVILResource):
    pass

class Cohort(BaseAnVILResource):
    pass


# READ ONLY filesystem?
class AnVILFS(FS):

    def leading_slash(f):
        def wrapped(self, path, *args, **kwargs):
            if "path" in kwargs:
                if kwargs["path"][0] != "/":
                    kwargs["path"] = "/" + kwargs["path"]
            elif isinstance(path, str):
                if path[0] != "/":
                    path = "/" + path
            return f(self, path, *args, **kwargs)
        return wrapped

    # input = name strings
    def __init__(self, namespace, workspace):
        super(AnVILFS, self).__init__()
        self._lock = threading.RLock()
        self.namespace = Namespace(namespace)
        self.namespace.fetch_workspace(workspace)
        self.workspace = self.namespace.folders[workspace]

    # this is all relative to the workspace, might need to be extended to namespace?
    @leading_slash
    def getinfo(self, path, namespaces=None):
        print("afs: getinfo({})".format(path))
        return self.workspace.get_info_from_path(path)

    @leading_slash
    def listdir(self, path):# Get a list of resources in a directory.
        if path[-1] != "/":
            path = path + "/"
        print("afs: listdir({})".format(path))
        try:
            maybe_dir = self.workspace.get_object_from_path(path)
            print("md: {}".format(maybe_dir))
        except KeyError as ke:
            raise ResourceNotFound("Resource {} not found".format(path))
        if isinstance(maybe_dir, dict) or maybe_dir.is_dir:
            print("returning dir keys {}".format(maybe_dir.keys()))
            return list(maybe_dir.keys())
        else:
            raise DirectoryExpected("{} is not a directory".format(path))

    @leading_slash
    def scandir(self, path):
        if path[-1] != "/":
            path = path + "/"
        print("afs: scandir({})".format(path))
        result = []
        l = self.listdir(path)
        print("listdir list: {}".format(l))
        for o in l:
            print(path+o)
            result.append(self.getinfo(path+o))
        return result

    def makedir():# Make a directory.
        raise Exception("makedir not implemented")
    
    @leading_slash
    def openbin(self, path, mode="r", buffering=-1, **options):
        print("afs: openbin({})".format(path))
        obj = self.workspace.get_object_from_path(path)
        try:
            return obj.get_bytes_handler()
        except AttributeError as e:
            raise FileExpected("Error: requested object is not a file:\n  {}".format(path))

    # @leading_slash
    # def download(self, path, *args, **kwargs):
    #     print("download being called on {}".format(path))
    #     obj = self.workspace.get_object_from_path(path)
    #     return obj.download()

    def remove():# Remove a file.
        raise Exception("remove not implemented")
    def removedir():# Remove a directory.
        raise Exception("removedir not implemented")
    def setinfo():# Set resource information.
        raise Exception("setinfo not implemented")
    # for network systems, scandir needed otherwise default calls a combination of listdir and getinfo for each file.