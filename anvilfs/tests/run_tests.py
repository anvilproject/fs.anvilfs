#!/usr/bin/env python3

# setup
if "anvil" not in locals():
    from ..anvilfs import AnVILFS
    from google.cloud import storage
    anvil = AnVILFS("fccredits-silicon-purple-4148", "ws_lcs")
    bucket_name = anvil.workspace.bucket_name
    blobs = storage.Client().get_bucket(bucket_name).list_blobs()
    files = []
    folders = ["/Other Data/", "/"]
    for blob in blobs:
        files.append("/Other Data/"+blob.name)
        split = blob.name.split("/")
        for i in range(len(split[:-1])):
            segment = "/Other Data/"+"/".join(split[i:-1])+"/"
            if segment not in folders:
                folders.append(segment)
    print(files)
    print(folders)


from .integration import run_all

run_all(anvil, files, folders)