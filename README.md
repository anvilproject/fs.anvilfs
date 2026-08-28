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

Releasing:
- Pushing a version tag such as `0.2.7` builds the sdist and wheel and
  publishes them to [PyPI](https://pypi.org/project/fs.anvilfs/) via the
  `Release` GitHub Actions workflow. Nothing else publishes; there is no
  manual upload step.
- The tag must equal `__version__` in `anvilfs/__about__.py` or the
  workflow fails before building, so bump the version and merge it first,
  then tag the merge commit:
  ```
  git checkout dev && git pull
  git tag 0.2.7 && git push origin 0.2.7
  ```
- Only bare `N.N.N` tags trigger a release.
- Uploads use PyPI
  [trusted publishing](https://docs.pypi.org/trusted-publishers/), so no
  API token is stored in the repository.

Known upstream incompatibilities:

`anvilfs/__init__.py` carries two compatibility shims. Both exist only
because an unmaintained dependency fails at import time on a runtime
Galaxy actually ships, and without them `import anvilfs` raises, the
`anvil` file source never registers, and Galaxy will not start at all.

- **FISS on Python 3.12+.** `firecloud.fiss` calls
  `configparser.SafeConfigParser()` when imported, and Python 3.12
  removed that name. `terra_notebook_utils.workspace` imports `fiss` at
  module scope, so `anvilfs/drs.py` cannot avoid it. Tracked upstream in
  [fiss#194](https://github.com/broadinstitute/fiss/issues/194), open
  since May 2025; firecloud 0.16.39 still makes the call. The shim
  restores the name, which has been a plain alias of `ConfigParser`
  since Python 3.2.
- **getm on urllib3 2.x.** `getm.http` builds a module-level
  `Retry(method_whitelist=[...])`. urllib3 renamed that argument to
  `allowed_methods` in 1.26 and removed it in 2.0. getm 0.0.5, the
  latest release, still uses the old name, and pinning `urllib3<2` is
  not possible inside Galaxy. The shim translates the keyword.

**These are temporary.** Each shim is a stopgap for a bug that belongs
upstream, not a feature of this package, and each mutates a module that
the whole host process shares. Delete the corresponding block as soon as
a fixed release of that dependency exists and the floor in `setup.py`
can be raised past it; the guards make each one inert in the meantime,
so a stale shim fails quietly rather than loudly.
