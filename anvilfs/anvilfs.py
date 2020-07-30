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

import firecloud.api as fapi
import threading

from fs.base import FS
from fs.enums import ResourceType
from fs.info import Info
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
        self.folders["Other Data/"] = WorkspaceBucket(resp["workspace"]["bucketName"])
        self.lastModified = resp["workspace"]["lastModified"]

    def get_info_from_path(self, path):
        assert(path[0] == "/")
        idx = path.find("/", 1)
        # if the next slash is the last slash, its a directory
        if idx == len(path) - 1:
            return self.folders[path[1:]].get_info()
        first_subfolder = path[1:idx+1]
        remainder = path[idx:]
        return self.folders[first_subfolder].get_info_from_path(remainder)


# google stores bucket files as a/b/c.extension;
#   so file dictionary stores { "a/b/":["c.extension", ...]}
class BucketFile(BaseAnVILResource):
    def __init__(self, name, size):
        self.name = name
        self.size = size
        self.is_dir = False

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
            self._insert_file(blob.name, blob.size)

    def _insert_file(self, bucket_file, file_size):
        print("inserting {}".format(bucket_file))
        idx = bucket_file.find('/')
        # if there is no '/' its a file
        if idx < 0:
            self.folders[bucket_file] = BucketFile(bucket_file, file_size)
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
            current_level[file_name] = BucketFile(file_name, file_size)

    def _make_folder_info(self, name):
        return {
            "basic": {
                "name": name,
                "is_dir": self.is_dir
            },
            "details": {
                "type": ResourceType.directory
            }
        }
        _details = _result["details"]
        if self.is_dir:
            _details["type"] = ResourceType.directory

    def get_info_from_path(self, path):
        assert(path[0] == "/")
        levels = path.split("/")
        if path[-1] == "/": # if final destination is a folder
            return self._make_folder_info(levels[-2] + "/")
        # if we're here, its a file with no terminal '/'
        current_obj = self.folders
        for level in levels[:-1]:
            current_obj = current_obj[level]
        return current_obj.get_info()


class Table(BaseAnVILResource):
    pass

class Cohort(BaseAnVILResource):
    pass


# READ ONLY filesystem?
class AnVILFS(FS):
    # input = name strings
    def __init__(self, namespace, workspace):
        super(AnVILFS, self).__init__()
        #self._lock = threading.RLock()
        self.namespace = Namespace(namespace)
        self.namespace.fetch_workspace(workspace)
        self.workspace = self.namespace.folders[workspace]

    def getinfo(self, path, namespaces=None):
        return self.workspace.get_info_from_path(path)

# required functions #TODO
    def listdir():# Get a list of resources in a directory.
        pass
    def makedir():# Make a directory.
        pass
    def openbin():# Open a binary file.
        pass
    def remove():# Remove a file.
        pass
    def removedir():# Remove a directory.
        pass
    def setinfo():# Set resource information.
        pass
    # for network systems, scandir needed otherwise default calls a combination of listdir and getinfo for each file.