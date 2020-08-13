from io import BytesIO

from google.cloud import storage

from .base import BaseAnVILFolder, BaseAnVILFile

class WorkspaceBucket(BaseAnVILFolder):
    def __init__(self, bucket_name):
        super().__init__("Files")
        self.bucket_name = bucket_name
        google_bucket = storage.Client().get_bucket(bucket_name)
        blobs = google_bucket.list_blobs()
        for blob in blobs:
            self.insert_file(blob)

    def insert_file(self, bucket_blob):
        # name relative to the path from workspace bucket
        path = bucket_blob.name
        if path[-1] == "/":
            raise Exception("Files should be set, not folders")
        s = path.split("/")
        if len(s) == 1:
            self[WorkspaceBucketFile(bucket_blob)] = None
        # get to underlying folder
        base = self
        for sub in s[:-1]:
            try:
                base = base[sub+"/"]
            except KeyError:
                baf = BaseAnVILFolder(sub+"/") 
                base[baf] = {}
                base = baf
        # now to insert the final object
        base[WorkspaceBucketFile(bucket_blob)] = None


class WorkspaceBucketFile(BaseAnVILFile):
    def __init__(self, blob):
        self.name = blob.name.split("/")[-1]
        self.size = blob.size
        self.last_modified = blob.updated
        self.blob_handle = blob
        self.is_dir = False
    
    def get_bytes_handler(self):
        buffer = BytesIO()
        self.blob_handle.download_to_file(buffer)
        buffer.seek(0)
        return buffer