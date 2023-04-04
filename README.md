### PyFilesystem2 AnVIL plugin

A plugin for representing [AnVIL](https://anvil.terra.bio/) resources in Python environments as a file system; particularly, in an AnVIL-launched [Galaxy](https://galaxyproject.org) instances.

Galaxy installation requirements:
- `file_sources_conf.yml` in the Galaxy config directory with an AnVIL entry:
```
- doc: <whatever you would like to call it>
  id: <root folder name>
  namespace: <your google billing project == Terra namespace >
  type: anvil
  workspace: <name of the Terra workspace you wish to browse>
  api_url: <OPTIONAL - if not specified default production url will be used>
  on_anvil: <OPTIONAL - (true/false) must be true to manage AnVIL workload identity scopes>
  drs_url: <OPTIONAL - if not specified  default production url will be used>
  writable: <OPTIONAL - (true/false) if not specified plugin will be read-only>
```

**NOTES**: 
- `doc` and `id` can be any string you choose but `type` *MUST* be `anvil`
- for off-AnVIL use, Galaxy and Data Fetch tools require the following environment variables:
  - `GOOGLE_APPLICATION_CREDENTIALS="<path to credentials json>"`
  - `TERRA_NOTEBOOK_GOOGLE_ACCESS_TOKEN="$(gcloud auth print-access-token)"`


Dependencies:
- [FISS -- (Fi)reCloud (S)ervice (S)elector python library](https://github.com/broadinstitute/fiss)
- [Google Cloud Python client library](https://cloud.google.com/python/docs/reference/storage/latest)
- Google Cloud SDK
  - authenticated / configured with AnVIL project
- [terra-notebook-utils](https://github.com/DataBiosphere/terra-notebook-utils)
- [gs-chunked-io](https://github.com/xbrianh/gs-chunked-io)
- [getm](https://github.com/DataBiosphere/getm)
