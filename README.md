### PyFilesystem2 AnVIL plugin

A plugin for describing [AnVIL](https://anvil.terra.bio/) resources such that AnVIL-launched [Galaxy](https://galaxyproject.org) instances can browse and download data.

Installation requirements:
- `file_sources_conf.yml` in the Galaxy config directory with an AnVIL entry:
```
- doc: <whatever you would like to call it>
  id: AnVIL
  namespace: <your google billing project == Terra namespace >
  type: anvil
  workspace: <name of the Terra workspace you wish to browse>
  api_url: <OPTIONAL - if not specified default production url will be used>
  on_anvil: <OPTIONAL - boolean set to false, in place to manage workload identity scopes>
```

Dependency requirements:
- fiss python library
- google cloud python library
- google cloud sdk
  - configured with AnVIL project