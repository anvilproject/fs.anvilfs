import requests
import json
from io import BytesIO
import concurrent.futures

from google.auth import credentials
from google.cloud import storage

import gs_chunked_io as gscio

from .basefile import BaseAnVILFile
from .clientrepository import ClientRepository


class GoogleAnVILFile(BaseAnVILFile):
    def __init__(self, input):
        if type(input) == str and input.startswith("gs://"):
            #normal
            url = input
            _split = url[len("gs://"):].split("/")
            self.name = _split[-1]
            filepath = "/".join(_split[1:])
            self.blob = self.info_to_blob(_split[0], filepath)
            self.blob.reload()
            self.size = self.blob.size
            self.last_modified = self.blob.updated
            # blobs = self.gc_storage_client.list_blobs(_bucket, prefix=filepath)
            # #buck = self.client.get_bucket(_split[0])
            # self.blob = None
            # self.size = None
            # self.last_modified = None
            # for b in blobs:
            #     if b.name == filepath:
            #         self.blob = b
            #         self.size = b.size
            #         self.last_modified = b.updated
            #         break
            # if not self.blob and not self.size and not self.last_modified:
            #     raise Exception(f"blob '{self.name}' not found...")
        elif type(input) == dict:
            self.name = input["name"]
            self.blob = self.info_to_blob(input["bucket"], input["path"])
            self.size = input["size"]
            self.last_modified = input["last_modified"]
        #self.blob = buck.get_blob("/".join(_split[1:]))
    
    @classmethod
    def factory(cls, gslist):
        results = []
        for item in gslist:
            results.append(GoogleAnVILFile(item))
        return results

    def info_to_blob(self, source_bucket, path):
        # requires project, bucket_name, prefix
        kb = 1024
        mb = 1024*kb
        chunk_size = 200*mb
        uproj = self.gc_storage_client.project
        bucket = self.gc_storage_client.bucket(source_bucket, user_project=uproj)
        return storage.blob.Blob(path, bucket)#, chunk_size = chunk_size)

    def get_bytes_handler(self):
        #buff = BytesIO()
        #self.blob.download_to_file(buff)
        #buff.seek(0)
        return gscio.Reader(self.blob)


class DRSAnVILFile(GoogleAnVILFile):
    api_url = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"

    def __init__(self, input):
        token = ClientRepository().get_fapi_token()
        #api_prefix = "https://dataguids.org/ga4gh/dos/v1/dataobjects/" <- old news
        api_url = self.api_url
        #apistring = api_prefix + drsurl[len("drs://"):]
        response = requests.post(
            api_url,
            data = {
                "url": input
            },
            headers = {
                "Authorization": f"Bearer {token}"
            }
        )
        result = json.loads(response.text)
        gurl = result["gsUri"]
        super().__init__(gurl)

    @classmethod
    def factory(cls, drslist):
        # subfunction for threads
        def _load_url(drsuri, timeout):
            token = ClientRepository().get_fapi_token()
            url = cls.api_url
            r = requests.post(
                    url,
                    data = {
                    "url":drsuri
                    },
                    headers = {
                        "Authorization": f"Bearer {token}"
                    }
                )
            return json.loads(r.text)
        # thread pool maker
        def _pooler(inlist, maxworks=50):
            timeout = 60#seconds
            good_data = []
            bad_uris = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=maxworks) as executor:
                # Start the load operations and mark each future with its URL
                future_to_url = {executor.submit(_load_url, url, timeout): url for url in inlist}
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        if "gsUri" not in data:
                            print(f"DRS resolution error - received:\n\t{data}")
                            bad_uris.append(url)
                        else:
                            good_data.append(data)
                    except Exception as exc:
                        print('%r generated an exception: %s' % (url, exc))
                        raise exc
                        bad_uris.append(url)
                    else:
                        pass
            return good_data, bad_uris

        # first pass
        file_objects = []
        good, bad = _pooler(drslist)
        # retry
        good_retries, bad_retries = _pooler(bad)
        if bad_retries:
            print(f"Unable to resolve the following URIs:\n{bad_retries}")
        total_goods = good + good_retries
        gs_info = [
            {
                "gsUri": x["gsUri"],
                "bucket": x["bucket"],
                "path": x["name"],
                "size": x["size"],
                "name": x["fileName"],
                "last_modified": x["timeUpdated"]
            } 
            for x in total_goods]
        # make google bucket objects
        for item in gs_info:
            file_objects.append(GoogleAnVILFile(item))
        return file_objects


class LazyDRSAnVILFile(DRSAnVILFile):
    def __init__(self, uri, name, size=None, last_modified=None):
        print(f"lazy init {name}")
        self.uri = uri
        self.name = name
        if not size:
            self.size = 1
        else:
            self.size = size
        if not last_modified:
            self.last_modified = ""
        else:
            self.last_modified = last_modified
    
    def get_bytes_handler(self):
        super().__init__(self.uri)
        return super().get_bytes_handler()