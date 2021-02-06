import requests
import json
import concurrent.futures

from google.cloud import storage

import gs_chunked_io as gscio

from .basefile import BaseAnVILFile
from .clientrepository import ClientRepository

local_cr = ClientRepository()


class GoogleAnVILFile(BaseAnVILFile):
    def __init__(self, info):
        kb = 1024
        mb = 1024*kb
        self.chunk_size = 200*mb

        if type(info) == str and info.startswith("gs://"):
            self.blob = self.uri_to_blob(info)
            self.blob.reload()
            self.size = self.blob.size
            self.last_modified = self.blob.updated
            self.name = info.split("/")[-1]
        elif type(info) == dict:
            self.name = info["name"]
            if "blob" in info:
                self.blob = info["blob"]
            else:
                self.blob = self.info_to_blob(info["bucket"], info["path"])
            self.size = info["size"]
            self.last_modified = info["last_modified"]
        else:
            raise Exception(f"Bad GoogleAnVILFile init content:\n\t{info}")

    @classmethod
    def factory(cls, gs_uri_list):
        results = []
        MAX_BATCH_SIZE = 1000  # break into sub batches
        gs_uri_2d_array = []
        sublist = []
        LAZY_THRESHOLD = 1000  # if exceeded, fudge metadata for UX
        # create list of blob sublists, sublist <= 1000 elements
        if len(gs_uri_list) > LAZY_THRESHOLD:
            return [
                LazyGoogleAnVILFile(item) for item in gs_uri_list
            ]
        for gs_uri in gs_uri_list:
            # if max length reached, add it to 2d array
            sublist.append(cls.uri_to_blob(gs_uri))
            if len(sublist) == MAX_BATCH_SIZE:
                gs_uri_2d_array.append(sublist)
                sublist = []
        # add final under-sized batch to list if it exists
        if sublist:
            gs_uri_2d_array.append(sublist)
        # perform batch operations
        for batch in gs_uri_2d_array:
            with local_cr.gc_storage_client.batch():
                for item in batch:
                    item.reload()
            # sub list has been refreshed, create obj from metadata
            for item in batch:
                results.append(GoogleAnVILFile({
                    "name": item.name.split("/")[-1],
                    "last_modified": item.updated,
                    "size": item.size,
                    "blob": item
                }))
        return results

    @classmethod
    def uri_to_blob(cls, uri):
        split = uri.split("/")
        source_bucket = split[2]
        path = "/".join(split[3:])
        uproj = local_cr.gc_storage_client.project
        bucket = local_cr.gc_storage_client.bucket(
            source_bucket, user_project=uproj)
        return storage.blob.Blob(path, bucket)

    def info_to_blob(self, source_bucket, path):
        # requires project, bucket_name, prefix
        uproj = self.gc_storage_client.project
        bucket = self.gc_storage_client.bucket(
            source_bucket, user_project=uproj)
        return storage.blob.Blob(path, bucket)

    def get_bytes_handler(self):
        return gscio.Reader(self.blob)


# for use with very large lists
class LazyGoogleAnVILFile(GoogleAnVILFile):
    def __init__(self, uri, size=None, last_modified=None):
        self.uri = uri
        self.name = uri.split("/")[-1]
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


class DRSAnVILFile(GoogleAnVILFile):
    api_url = ("https://us-central1-broad-dsde-prod.cloudfunctions.net/"
               "martha_v3")

    def __init__(self, input):
        token = ClientRepository().get_fapi_token()
        api_url = self.api_url
        response = requests.post(
            api_url,
            data={
                "url": input
            },
            headers={
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
                    data={
                        "url": drsuri
                    },
                    headers={
                        "Authorization": f"Bearer {token}"
                    }
                )
            return json.loads(r.text)

        # thread pool maker
        def _pooler(inlist, maxworks=50):
            timeout = 60  # seconds
            good_data = []
            bad_uris = []
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=maxworks) as executor:
                # Start the load operations and mark each future with its URL
                future_to_url = {executor.submit(
                    _load_url, url, timeout): url for url in inlist}
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        if "gsUri" not in data:
                            print(f"DRS resolution error- received:\n\t{data}")
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
