from google.cloud import storage
from google.api_core.exceptions import NotFound
import gs_chunked_io as gscio

from .basefile import BaseAnVILFile


class GoogleAnVILFile(BaseAnVILFile):
    def __init__(self, info, creds=None):
        print(f"initializing {info}")
        kb = 1024
        mb = 1024*kb
        self.chunk_size = 200*mb

        self.gs_client = None

        if creds:
            self.gs_client = storage.client.Client(
                project=self.base_project, credentials=creds)
        else:
            self.gs_client = self.gc_storage_client
        if type(info) == str and info.startswith("gs://"):
            self.blob = self.uri_to_blob(info, self.gs_client)
            self.blob.reload(client=self.gs_client)
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
            batch_client = cls.get_default_gcs_client()
            good_items = []
            try:
                with batch_client.batch():
                    for item in batch:
                        item.reload()
                        good_items.append(item)
            except NotFound:
                print("AnVILFS Error: dead links found in batch")
            # sub list has been refreshed, create obj from metadata
            for item in good_items:
                try:
                    results.append(GoogleAnVILFile({
                        "name": item.name.split("/")[-1],
                        "last_modified": item.updated,
                        "size": item.size,
                        "blob": item
                    }))
                except KeyError:
                    # failed batch items raise KeyErrors, so they're skipped
                    continue
        return results

    @classmethod
    def uri_to_blob(cls, uri, client=None):
        if not client:
            client = cls.get_default_gcs_client()
        split = uri.split("/")
        source_bucket = split[2]
        path = "/".join(split[3:])
        uproj = client.project
        bucket = client.bucket(
            source_bucket, user_project=uproj)
        return storage.blob.Blob(path, bucket)

    def info_to_blob(self, source_bucket, path):
        # requires project, bucket_name, prefix
        uproj = self.gs_client.project
        bucket = self.gs_client.bucket(
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
