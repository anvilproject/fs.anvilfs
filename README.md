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
  on_anvil: <OPTIONAL - boolean set to false, in place to manage workload identity scopes>
  drs_url: <OPTIONAL - if not specified  default production url will be used>
```
**NOTE**: `doc` and `id` can be any string you choose but `type` *MUST* be `anvil`
- `lib/galaxy/files/sources/anvil.py`
  - see included `anvil.py`



Dependency requirements:
- fiss python library
- google cloud python library
- google cloud sdk
  - configured with AnVIL project