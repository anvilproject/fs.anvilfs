from ..base import BaseAnVILResource, BaseAnVILFile, BaseAnVILFolder
from ..bucket import WorkspaceBucket, WorkspaceBucketFile
from ..namespace import Namespace
from ..reference import ReferenceDataFolder, ReferenceDataFile
from ..workspace import Workspace, WorkspaceData

from .testutils import BaseTest, FakeBlob

from fs.enums import ResourceType
from google.cloud import storage

# global test storage client
client = storage.Client()

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
        baf.children[bafile.name] = bafile
        if baf["X"] == bafile:
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC)

    def test_bafolder_set():
        DESC = "__setitem__()"
        baf = BaseAnVILFolder("A","B")
        bafile = BaseAnVILFile("X", 1)
        baf[bafile.name] = bafile
        if bafile.name in baf.children:
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
        baf[bafA.name] = bafA
        bafA[bafB.name] = bafB
        bafB[bafC.name] = bafC
        bafC[bafD.name] = bafD
        fi = baf.get_object_from_path("A/B/C/D")
        fo = baf.get_object_from_path("A/B/C/")
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
        ws_folder_name = ws_name + "/"
        if ws.name[:-1] != ws_name:
            return BaseTest.failure(DESC + ": ws name")
        if ns[ws_folder_name] != ws:
            return BaseTest.failure(DESC + ": ws obj equivalence")
        return BaseTest.success(DESC)

# Workspace # --------------------------
class TestWorkspace(BaseTest):
    def test_ws_init(ns_name, ws_name):
        DESC = "__init__()"
        ns = Namespace(ns_name)
        ws = Workspace(ns, ws_name)
        if not isinstance(ws["Other Data/"]["Files/"], WorkspaceBucket):
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

class TestWorkspaceData(BaseTest):
    def test__dict_to_buffer():
        DESC = "_dict_to_buffer()"
        data1 = {"a":"A", "b":"B"}
        data2 = {"b":"B", "a":"A"}
        bytesrep = b"a\tA\nb\tB\n"
        wsd = WorkspaceData("one", data1)
        buff1val = wsd.buffer.getvalue()
        wsd._dict_to_buffer(data2)
        buff2val = wsd.buffer.getvalue()
        if (buff1val == bytesrep and buff1val == buff2val):
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC + ": identical values not equal after conversion")

    def test_data_init():
        DESC = "__init__()"
        data = {"space":"cowboy"}
        datab = b"space\tcowboy\n"
        wsd = WorkspaceData("maurice", data)
        if (wsd.name == "maurice" and 
            wsd.buffer.getvalue() == datab):
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC + ": initialization failure")


class TestReferenceDataFile(BaseTest):
    def test_init():
        DESC = "__init__()"
        fake_blob = FakeBlob(name="a", size=2, updated="Never")
        rdf = ReferenceDataFile(fake_blob)
        if (rdf.blob_handle == fake_blob and
            rdf.name == "a" and
            rdf.size == 2 and
            rdf.last_modified == "Never" and
            not rdf.is_dir ):
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC+ ": initialized object does not equal input")

    def test_make_rdfs():
        DESC = "make_rdfs()"
        # public reference gs bucket file:
        url = "gs://gcp-public-data--broad-references/hg38/v0/1000G_omni2.5.hg38.vcf.gz"
        bucket_name = "gcp-public-data--broad-references"
        bucket_path = "hg38/v0/1000G_omni2.5.hg38.vcf.gz"
        file_name = "1000G_omni2.5.hg38.vcf.gz"
        blob_size = 53238342
        blob_updated = "2019-12-06 23:55:16.264000+00:00"
        blob_name = bucket_path
        # get blob
        bucket = client.list_blobs(bucket_name, prefix=bucket_path)
        correct_blob = None
        for b in bucket:
            if b.name == bucket_path:
                correct_blob = {url: b}
                break
        r = ReferenceDataFile.make_rdfs(correct_blob)
        rdf = r[0]
        if (rdf == ReferenceDataFile(correct_blob[url]) and
            rdf.name == file_name and
            rdf.size == blob_size and
            str(rdf.last_modified) == str(blob_updated) and
            rdf.blob_handle == correct_blob[url] and
            not rdf.is_dir):
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC + ": factory-constructed ReferenceDataFile != manually created")

class TestReferenceDataFolder(BaseTest):
    def test_init():
        url = "gs://gcp-public-data--broad-references/hg38/v0/1000G_omni2.5.hg38.vcf.gz"
        bucket_name = "gcp-public-data--broad-references"
        bucket_path = "hg38/v0/1000G_omni2.5.hg38.vcf.gz"
        file_name = "1000G_omni2.5.hg38.vcf.gz"
        file_size = 53238342
        # get blob
        bucket = client.list_blobs(bucket_name, prefix=bucket_path)
        correct_blob = None
        for b in bucket:
            if b.name == bucket_path:
                correct_blob = {url: b}
                break
        DESC = "__init__()"
        refs = {
            "source": {
                "reftype": correct_blob
                }}
        rdf = ReferenceDataFolder("test", refs)
        if (rdf.name == "test/" and
            isinstance(rdf["source/"]["reftype/"][file_name], ReferenceDataFile) and
            rdf["source/"]["reftype/"][file_name].name == file_name and
            rdf["source/"]["reftype/"][file_name].size == file_size):
            return BaseTest.success(DESC)
        else:
            return BaseTest.failure(DESC, "created ReferenceDataFile initialized in ReferenceDataFolder != remote source")


def run_all(anvil, files, folders):
    # define required arguments
    ns_name = anvil.namespace.name[:-1]
    ws_name = anvil.workspace.name[:-1]
    bucket_name = anvil.workspace.bucket_name
    print("=][=  [UNIT TESTING]  =][=")
    # format: a list of tuples
    #   [ (TestClassName, [list, of, arguments]), ... ]
    objs_n_args = [
        (TestBaseANVILResource, []),
        (TestBaseAnVILFile, []), 
        (TestBaseAnVILFolder, []),
        (TestNamespace, [ns_name, ws_name]),
        (TestWorkspace, [ns_name, ws_name]),
        (TestWorkspaceBucket, [bucket_name]),
        (TestWorkspaceBucketFile, []),
        (TestWorkspaceData, []),
        (TestReferenceDataFile, []),
        (TestReferenceDataFolder, [])
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