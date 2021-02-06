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
            print(workspacedata)
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
                print(f"appending {val} to linkable files")
                files[filetype].append(val)
        linked_files = []
        for method in files:
            try:
                fresh_files = method.factory(files[method])
                linked_files.extend(fresh_files)
            except Exception as e:
                print(f"ERROR: SKIPPING FILE due to error:")
                print(e)
                continue
        linked_files.append(
            WorkspaceData("WorkspaceData.tsv", self.workspacedata))
        for f in linked_files:
            self[f.name] = f


class WorkspaceBucketSubFolder(BaseAnVILFolder):
    def __init__(self, name, remaining, initializing_blob):
        super().__init__(name)
        self.remaining = remaining
        self.initializing_blob = initializing_blob

    def lazy_init(self):
        if len(self.remaining) == 1:
            self[self.remaining[0]] = WorkspaceBucketFile(
                self.initializing_blob)
            del self.initializing_blob
        else:
            subname = self.remaining[0] + '/'
            self[subname] = WorkspaceBucketSubFolder(
                subname, self.remaining[1:], self.initializing_blob)


class WorkspaceBucket(BaseAnVILFolder):
    def __init__(self, bucket_name):
        super().__init__("Files")
        self.bucket_name = bucket_name

    def lazy_init(self):
        google_bucket = self.gc_storage_client.get_bucket(self.bucket_name)
        blobs = google_bucket.list_blobs()
        self.initialized = True
        for blob in blobs:
            self.insert_file(blob)

    def insert_file(self, bucket_blob):
        # name relative to the path from workspace bucket
        path = bucket_blob.name
        if path[-1] == "/":
            raise Exception("Files should be set, not folders")
        s = path.split("/")
        if len(s) == 1:
            _wsbf = WorkspaceBucketFile(bucket_blob)
            self[_wsbf.name] = _wsbf
        else:
            subname = s[0]+'/'
            self[subname] = WorkspaceBucketSubFolder(
                subname, s[1:], bucket_blob)


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
