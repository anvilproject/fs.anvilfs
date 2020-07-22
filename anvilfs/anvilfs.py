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

import logging
import threading
import firecloud.api as fapi

from fs.info import Info
from fs.base import FS
from google.cloud import storage


"""
Namespace == Google project name
"""
class Namespace():
    def __init__(self, namespace_name):
        self.name = namespace_name
        self.workspaces = {}

    def fetch_workspace(self, workspace_name):
        self.workspaces[workspace_name] = Workspace(self, workspace_name)

    def __str__(self):
        out = "<Namespace {}>".format(self.name)
        for ws in self.workspaces:
            out += "\n{}".format(str(ws))

class Workspace():
    def __init__(self, namespace_reference,  workspace_name):
        self.namespace = namespace_object
        self.name = workspace_name
        self.fetch_info()
        self.bucket = BucketData(self)
        # mimicking anvil folder structure:
        self.folders = {
            "Other Data": {},
        }


    def __str__(self):
        out = "  <Workspace {}>".format(self.name)
        #for objects in entities ...

    def fetch_info(self):
        fields = "workspace.attributes,workspace.bucketName,workspace.lastModified"
        resp = fapi.get_workspace(name=self.name, fields=fields).json()
        fapi._check_response_code(resp, 200)
        self.attributes = resp["workspace"]["attributes"]
        self.bucket = BucketData(self, resp["workspace"]["bucketName"])
        self.lastModified = resp["workspace"]["lastModified"]


#base anvil object
class Entity():
    pass


# google stores bucket files as a/b/c.extension;
#   so file dictionary stores { "a/b/":["c.extension", ...]}
class BucketFile():
    def __init__(self, name, size):
        self.name = name
        self.size = size

class BucketFolder():
    def __init__(self):
        self.files = {}
        self.folders = {}

    def add_file(bucketfile_obj):
        self.files[bucketfile_obj.name] = bucketfile_obj
    
    def add_folder(bucketfolder_obj):
        self.folders[bucketfolder_obj.name] = bucketfolder_obj


class RootBucketFolder(BucketFolder):
    # Path must end in '/' if seeking a directory to avoid ambiguity
    #    a <- file named a in root
    #    a/ <- folder named a in root
    def path_to_object(self, path):
        # base case
        if path == "/":
            return self
        # sanitize input
        if path[0] == "/":
            path = path[1:]
        # check if folder or file
        if path[-1] == "/":
            objtype = BucketFolder
        else:
            objtype = BucketFile
        # initialize 'current directory'
        current_dir = self
        for i, step in enumerate(steps, 1):
            # if its the last step, it's the name of the object we're after
            if i == len(step):
                if objtype == BucketFile:
                    return current_dir.files[step]
                else:
                    return current_dir.folders[step]
            # if it isnt the last step, it's necessarily a folder
            current_dir = current_dir.folders[step]


class BucketData():
    def __init__(self, workspace_reference, bucketname):
        self.client = storage.Client()
        self.workspace = workspace_reference
        self.name = bucketname
        self.fetch_info()
        self.root = RootBucketFolder()

    def _insert_file(self, bucketfile):
        bfname = bucketfile.name
        idx = bfname.rfind('/')
        if idx < 0:
            self.files["/"].append(bucketfile)
        else:
            dirstr = bfname[:idx]
            if dirstr not in self.files.keys():
                self.files[dirstr] = []
            self.files[dirstr].append(bucketfile)

    def fetch_info(self):
        bucket = self.client.get_bucket(self.name)
        blobs = bucket.list_blobs()
        # can generate signed urls from blobs with 'blob.generate_signed_url'
        for blob in blobs:
            self._insert_file(BucketFile(blob.name, blob.size)

class Table(Entity):
    pass

class Cohort(Entity):
    pass


# READ ONLY filesystem?
class AnVILFS(FS):
    def __init__(self, namespace, workspace):
        super(AnVILFS, self).__init__()
        #self._lock = threading.RLock()
        self.namespace = Namespace(namespace)
        self.namespace.fetch_workspace(workspace)
"""
REQUIRED FUNCTIONS
    #TODO:
    listdir() Get a list of resources in a directory.
    makedir() Make a directory.
    openbin() Open a binary file.
    remove() Remove a file.
    removedir() Remove a directory.
    setinfo() Set resource information.
    # for network systems, scandir needed otherwise default calls a combination of listdir and getinfo for each file.
"""
    def getinfo(self, path, namespaces=None):
        obj = 
        rawInfo = {
            "basic": {
                name
            }
        }