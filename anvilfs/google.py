from io import BytesIO

from .base import BaseAnVILFile

class GoogleAnVILFile(BaseAnVILFile):
    def __init__(self, url, client):
        self.client = client
        _split = url[len("gs://"):].split("/")
        self.name = _split[-1]
        blobs = self.client.list_blobs(_split[0], prefix="/".join(_split[1:])) 
        #buck = self.client.get_bucket(_split[0])
        self.blob = None
        self.size = None
        self.last_modified = None
        for b in blobs:
            if b.name == self.name:
                self.blob = b
                self.size = b.size
                self.last_modified = b.updated
                break
        if not self.blob and self.size and self.last_modified:
            raise Exception(f"blob '{self.name}' not found...")
        #self.blob = buck.get_blob("/".join(_split[1:]))
        

    def get_bytes_handler(self):
        buff = BytesIO()
        self.blob.download_to_file(buff)
        buff.seek(0)
        return buff