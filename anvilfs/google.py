import requests
import json
from io import BytesIO

from google.auth import credentials

from .base import BaseAnVILFile

class GoogleAnVILFile(BaseAnVILFile):
    def __init__(self, url):
        _split = url[len("gs://"):].split("/")
        self.name = _split[-1]
        filepath = "/".join(_split[1:])
        blobs = self.gc_storage_client.list_blobs(_split[0], prefix=filepath) 
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

class DRSAnVILFile(GoogleAnVILFile):

    def __init__(self, drsurl):
        sesh = self.fapi.__getattribute__("__SESSION")
        if not sesh or not sesh.credentials.valid:
            self.fapi._set_session()
        token = self.fapi.__getattribute__("__SESSION").credentials.token
        #api_prefix = "https://dataguids.org/ga4gh/dos/v1/dataobjects/" <- old news
        api_url = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
        #apistring = api_prefix + drsurl[len("drs://"):]
        response = requests.post(
            api_url,
            data = {
                "url": drsurl
            },
            headers = {
                "Authorization": f"Bearer {token}"
            }
        )
        result = json.loads(response.text)
        gurl = result["gsUri"]
        super().__init__(gurl)

