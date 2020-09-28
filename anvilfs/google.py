import urllib.request
import json

from io import BytesIO

from .base import BaseAnVILFile

class GoogleAnVILFile(BaseAnVILFile):
    def __init__(self, url, client):
        self.client = client
        _split = url[len("gs://"):].split("/")
        self.name = _split[-1]
        filepath = "/".join(_split[1:])
        blobs = self.client.list_blobs(_split[0], prefix=filepath) 
        #buck = self.client.get_bucket(_split[0])
        self.blob = None
        self.size = None
        self.last_modified = None
        for b in blobs:
            if b.name == filepath:
                self.blob = b
                self.size = b.size
                self.last_modified = b.updated
                break
        if not self.blob and not self.size and not self.last_modified:
            raise Exception(f"blob '{self.name}' not found...")
        #self.blob = buck.get_blob("/".join(_split[1:]))
        

    def get_bytes_handler(self):
        buff = BytesIO()
        self.blob.download_to_file(buff)
        buff.seek(0)
        return buff
    
    @classmethod
    def drsmaker(self, drsurl, client):
        api_prefix = "https://dataguids.org/ga4gh/dos/v1/dataobjects/"
        apistring = api_prefix + drsurl[len("drs://"):]
        _r = urllib.request.urlopen(apistring)
        objs = []
        for _url in json.loads(_r.read().decode('utf-8'))["data_object"]["urls"]:
            gurl = _url["url"]
            objs.append(GoogleAnVILFile(gurl, client))
        return objs

