from ..base import BaseAnVILResource, BaseAnVILFile, BaseAnVILFolder
from ..bucket import WorkspaceBucket, WorkspaceBucketFile
from ..namespace import Namespace
from ..workspace import Workspace

from .testutils import BaseTest, FakeBlob


from fs.enums import ResourceType


# base anvil resource # -------------------------
class TestBaseANVILResource(BaseTest):
    def test_base_getinfo():
        DESC = "getinfo() is abstract"
        bar = BaseAnVILResource()
        bar.name = "test"
        try:
            bar.getinfo()
        except NotImplementedError as e:
            return BaseTest.success(DESC)
        return BaseTest.failure(DESC)

    def test_base_hash():
        DESC = "hash() hashes"
        bar = BaseAnVILResource()
        bar.name = "hasher"
        if bar.__hash__() == hash(("hasher", "BaseAnVILResource")):
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)


# base anvil file # -------------------------
class TestBaseAnVILFile(BaseTest):
    def test_bafile_getinfo():
        DESC = "getinfo()"
        name = "a"
        size = "1"
        last_mod = "lastmodstring"
        r_dict = {"basic": {
                    "name": name,
                    "is_dir": False,
                },
                "details": {
                    "type": ResourceType.file,
                    "size": size,
                    "modified": last_mod
                }
            }
        baf = BaseAnVILFile(name, size, last_mod)
        if baf.getinfo().raw == r_dict:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

    def test_bafile_get_bh():
        DESC = "open_bin() is abstract"
        baf = BaseAnVILFile("a", 1)
        try:
            baf.get_bytes_handler()
        except NotImplementedError as e:
            return BaseTest.success(DESC)
        return BaseTest.failure(DESC)

    def test_bafile_eq():
        DESC = "__eq__()"
        name = "a"
        size = 999
        last_mod = "yes"
        baf1 = BaseAnVILFile(name, size, last_mod)
        baf2 = BaseAnVILFile(name, size, last_mod)
        if baf1 == baf2:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

# base anvil folder # -------------------------
class TestBaseAnVILFolder(BaseTest):
    def test_bafolder_getinfo():
        DESC = "getinfo()"
        # folders end with '/' to differentiate between same-named files
        name = "a/"
        last_mod = "lastmodstring"
        r_dict = {
            "basic": {
                "name": name,
                "is_dir": True,
            },
            "details": {
                "type": ResourceType.directory,
                "modified": last_mod
            }
        }
        baf = BaseAnVILFolder(name, last_mod)
        if baf.getinfo().raw == r_dict:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

    def test_bafolder_hash():
        DESC = "__hash__()"
        name = "a/"
        last_mod = "yes"
        baf = BaseAnVILFolder(name, last_mod)
        if hash((name, "BaseAnVILFolder", last_mod)) == baf.__hash__():
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

    def test_bafolder_get():
        DESC = "__getitem__()"
        baf = BaseAnVILFolder("A","B")
        bafile = BaseAnVILFile("X", 1)
        baf.filesystem[bafile] = None
        if baf["X"] == bafile:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

    def test_bafolder_set():
        DESC = "__setitem__()"
        baf = BaseAnVILFolder("A","B")
        bafile = BaseAnVILFile("X", 1)
        baf[bafile] = None
        if bafile in baf.filesystem:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

    def test_bafolder_gofp():
        DESC = "get_object_from_path()"
        baf = BaseAnVILFolder("Place","Holder")
        baf = BaseAnVILFolder("root")
        bafA = BaseAnVILFolder("A")
        bafB = BaseAnVILFolder("B")
        bafC = BaseAnVILFolder("C")
        bafD = BaseAnVILFile("D", 1)
        baf[bafA] = None
        bafA[bafB] = None
        bafB[bafC] = None
        bafC[bafD] = None
        fi = baf.get_object_from_path("A/B/C/D")
        fo = baf.get_object_from_path("A/B/C")
        if fi == bafD and fo == bafC:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

# Namespace # -------------------------
class TestNamespace(BaseTest):
    def test_ns_init(ns_name, *args):
        DESC = "__init__()"
        ns = Namespace(ns_name)
        if ns.name != ns_name + "/":
            return BaseTest.failure(DESC + ": name")
        if ns.__hash__() != hash((ns_name+"/", "Namespace", None)):
            return BaseTest.failure(DESC + ": hash")
        return BaseTest.success(DESC)

    def test_ns_fetch_workspace(ns_name, ws_name):
        DESC = "fetch_workspace()"
        ns = Namespace(ns_name)
        ws = ns.fetch_workspace(ws_name)
        if ws.name[:-1] != ws_name:
            return BaseTest.failure(DESC + ": ws name")
        if ns[ws_name] != ws:
            return BaseTest.failure(DESC + ": ws obj equivalence")
        return BaseTest.success(DESC)

# Workspace # --------------------------
class TestWorkspace(BaseTest):
    def test_ws_init(ns_name, ws_name):
        DESC = "__init__()"
        ns = Namespace(ns_name)
        ws = Workspace(ns, ws_name)
        if not isinstance(ws["Other Data"]["Files"], WorkspaceBucket):
            return BaseTest.failure(DESC + ": WorkspaceBucket not present")
        else:
            return BaseTest.success(DESC)

    def test_ws_fetchinfo(ns_name, ws_name):
        DESC = "fetch_api_info()"
        ns = Namespace(ns_name)
        ws = Workspace(ns, ws_name)
        info = ws.fetch_api_info(ws_name)
        if "workspace" not in info:
            return BaseTest.failure(DESC + ": fetched info incomplete")
        else:
            return BaseTest.success(DESC)

# WorkspaceBucketFile # ------------------
class TestWorkspaceBucketFile(BaseTest):
    def test_wsbucketfile_init():
        DESC = "__init__()"
        wsbf1 = WorkspaceBucketFile(FakeBlob())
        wsbf2 = WorkspaceBucketFile(FakeBlob("Steve", 666, "Never"))
        if not (
            wsbf1.name == FakeBlob.DEFAULT_NAME.split("/")[-1] and
            wsbf1.size == FakeBlob.DEFAULT_SIZE and
            wsbf1.last_modified == FakeBlob.DEFAULT_UPDATED
        ):
            return BaseTest.failure(DESC + ": Default object mismatch")
        elif not (
            wsbf2.name == "Steve" and
            wsbf2.size == 666 and
            wsbf2.last_modified == "Never"
        ):
            return BaseTest.failure(DESC + ": specified object mismatch")
        else:
            return BaseTest.success(DESC)

# WorkspaceBucket # ----------------------
class TestWorkspaceBucket(BaseTest):
    def test_bucket_init(bucket_name):
        DESC = "__init__()"
        wsb = WorkspaceBucket(bucket_name)
        if wsb.name != "Files/":
            return BaseTest.failure(DESC + ": name mismatch")
        else:
            return BaseTest.success(DESC)

    def test_bucket_insert(bucket_name):
        DESC = "insert_file()"
        wsb = WorkspaceBucket(bucket_name)

        wsb.insert_file(FakeBlob())
        wsbf = WorkspaceBucketFile(FakeBlob())
        fake_insert = wsb["afile/"]["in/"]["the/"]["bucket.nfo"]
        if fake_insert != wsbf:
            return BaseTest.failure(DESC + ": object mismatch")
        else:
            return BaseTest.success(DESC)

def run_all(anvil, files, folders):
    # define required arguments
    ns_name = anvil.namespace.name[:-1]
    ws_name = anvil.workspace.name[:-1]
    bucket_name = anvil.workspace.bucket_name
    print("=][=  [UNIT TESTING]  =][=")
    # BaseAnVILResource
    objs_n_args = [
        (TestBaseANVILResource, []),
        (TestBaseAnVILFile, []), 
        (TestBaseAnVILFolder, []),
        (TestNamespace, [ns_name, ws_name]),
        (TestWorkspace, [ns_name, ws_name]),
        (TestWorkspaceBucket, [bucket_name]),
        (TestWorkspaceBucketFile, [])
    ]
    results = [0, 0]
    failures = []
    for o in objs_n_args:
        _s, _f = o[0].run_tests(*o[1])
        if _f:
            failures.append(o[0].__name__ + " - {} failure(s)".format(_f))
        results[0] += _s
        results[1] += _f

    print(" GRAND TOTAL: {} FAILURES in {} TESTS\n".format(results[1], results[0] + results[1]))
    if failures:
        print("  failing modules:")
        for f in failures:
            print("\t"+f)
    return (results, failures)