from io import BytesIO
from os import SEEK_END, SEEK_SET

import re

import gs_chunked_io as gscio

from .basefile import BaseAnVILFile
from .basefolder import BaseAnVILFolder


class OtherDataFolder(BaseAnVILFolder):
    def __init__(self, attributes, bucket_name):
        super().__init__("Other Data")
        self.bucket_name = bucket_name
        self.attributes = attributes

    def lazy_init(self):
        # clone it to delete from one while iterating over other
        workspacedata = dict(self.attributes)
        blocklist_prefixes = [
            "referenceData_",
            "description",
            "tag:"
        ]
        for datum in self.attributes:
            for blocked in blocklist_prefixes:
                if datum.startswith(blocked):
                    del workspacedata[datum]
        if workspacedata:
            wsdf = WorkspaceDataFolder(workspacedata)
            self[wsdf.name] = wsdf
        _wsb = WorkspaceBucket(self.bucket_name)
        self[_wsb.name] = _wsb


class WorkspaceDataFolder(BaseAnVILFolder):
    def __init__(self, workspacedata):
        super().__init__("Workspace Data")
        self.workspacedata = workspacedata

    def lazy_init(self):
        files = {}
        for k in self.workspacedata:
            val = self.workspacedata[k]
            filetype = self.is_linkable_file(val)
            if filetype is not None:
                if filetype not in files:
                    files[filetype] = []
                files[filetype].append(val)
        linked_files = []
        for method in files:
            try:
                fresh_files = method.factory(files[method])
                linked_files.extend(fresh_files)
            except Exception as e:
                print("ERROR: SKIPPING FILE due to error:")
                print(e)
                continue
        linked_files.append(
            WorkspaceData("WorkspaceData.tsv", self.workspacedata))
        for f in linked_files:
            self[f.name] = f


class WorkspaceBucketSubFolder(BaseAnVILFolder):
    def __init__(self, name, bucketpath, bucket_name):
        self.bucket_name = bucket_name
        self.files = []
        self.bucketpath = bucketpath
        super().__init__(name)

    def lazy_init(self):
        pass

    def upload(self, fname, read_file):
        try:
            self["google_bucket"]
        except KeyError:
            self.google_bucket = self.gc_storage_client.bucket(
                self.bucket_name)
        with gscio.Writer(self.bucketpath + fname, self.google_bucket) as gsw:
            data = read_file.read(gsw.chunk_size)
            while data:
                gsw.write(data)
                data = read_file.read(gsw.chunk_size)


class WorkspaceBucket(BaseAnVILFolder):
    def __init__(self, bucket_name):
        super().__init__("Files")
        self.bucket_name = bucket_name
        self.bucketpath = ""

    def lazy_init(self):
        self.google_bucket = self.gc_storage_client.get_bucket(
            self.bucket_name)
        blobs = self.google_bucket.list_blobs()
        self.initialized = True
        for blob in blobs:
            self.insert_file(blob)

    def insert_file(self, bucket_blob):
        # name relative to the path from workspace bucket
        path = bucket_blob.name
        # handle subfolders like base folders -- dunno why google doesn't
        # e.g., list has a/b/ but not a/ 
        if path[-1] == "/":
            return
            #raise Exception(f"Files should be set, not folders: {path}")
        s = path.split("/")
        # march to terminal folder, creating along the way
        current = self
        for i in range(len(s)-1):
            subname = s[i]+'/'
            if subname not in current:
                dir_path = '/'.join(s[:i+1]) + '/'
                current[subname] = WorkspaceBucketSubFolder(subname, dir_path, self.bucket_name)
            current = current[subname]
        current[s[-1]] = WorkspaceBucketFile(bucket_blob)

    def upload(self, fname, read_file):
        try:
            self["google_bucket"]
        except KeyError:
            self.google_bucket = self.gc_storage_client.bucket(
                self.bucket_name)
        with gscio.Writer(fname, self.google_bucket) as gsw:
            data = read_file.read(gsw.chunk_size)
            while data:
                gsw.write(data)
                data = read_file.read(gsw.chunk_size)


class WorkspaceBucketFile(BaseAnVILFile):
    def __init__(self, blob):
        self.name = blob.name.split("/")[-1]
        self.size = blob.size
        self.last_modified = blob.updated
        self.blob_handle = blob
        self.is_dir = False

    def get_bytes_handler(self):
        return gscio.Reader(self.blob_handle)


class WorkspaceData(BaseAnVILFile):
    def __init__(self, name, data_dict):
        self.name = name
        self.buffer = self._dict_to_buffer(data_dict)
        self.last_modified = None

    def _dict_to_buffer(self, d):
        # only keys that match the below regex are valid
        keys = [k for k in d.keys() if bool(
            re.match("^[A-Za-z0-9_-]*$", k))]
        data = ""
        for k in keys:
            data += f"{k}\t{d[k]}\n"
        buffer = BytesIO(data.encode('utf-8'))
        position = buffer.tell()
        buffer.seek(0, SEEK_END)
        self.size = buffer.tell()
        buffer.seek(position, SEEK_SET)
        return buffer

    def get_bytes_handler(self):
        return self.buffer
