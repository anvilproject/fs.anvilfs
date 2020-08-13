from .testconfig import config
from .unit import run_all as run_all_unit_tests
# from .integration import run_all as run_all_integration_tests


# setup
if "anvil" not in locals():
    from ..anvilfs import AnVILFS
    from google.cloud import storage
    anvil = AnVILFS(config["namespace_name"], 
                    config["workspace_name"])
    bucket_name = anvil.workspace.bucket_name
    blobs = storage.Client().get_bucket(bucket_name).list_blobs()
    files = []
    folders = []
    bucketfolder_prefixes = ["Other Data", "Files"]
    for blob in blobs:
        split = blob.name.split("/")
        whole_split = bucketfolder_prefixes + split
        files.append("/".join(whole_split))
        for i in range(0,len(whole_split[:-1])):
            segment = whole_split[0:-1*(i+1)]
            reformed = "/".join(segment) + "/"
            if reformed not in folders:
                folders.append(reformed)
            else:
                break
    print("Contents of AnVIL workspace '{}'".format(config["workspace_name"]))
    print("    discovered files: \n{}".format(files))
    print("    discovered folders: \n{}\n".format(folders))

if __name__ == "__main__":
    unit_results, unit_failures = run_all_unit_tests(anvil, files, folders)
    # integration_results, integration_failures =  run_all_integration_tests(anvil, files, folders)