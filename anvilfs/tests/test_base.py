import pytest
import requests

from anvilfs.baseresource import BaseAnVILResource, ClientRepository
from anvilfs.basefolder import BaseAnVILFolder
from anvilfs.basefile import BaseAnVILFile

from fs.enums import ResourceType
from fs.info import Info


class TestClientRepository:
    br = ClientRepository()

    def test_base_default_project(self):
        assert self.br.base_project is None

    def test_ref_defaults(self):
        rs = self.br._refs
        assert (
            rs["fapi"].__name__ == "firecloud.api" and
            rs["gc_storage_client"] is None and
            rs["gc_bigquery_client"] is None
        )

    def test_ref_inits(self):
        ris = self.br._ref_inits
        gcs_string = "<class 'google.cloud.storage.client.Client'>"
        gcbq_string = "<class 'google.cloud.bigquery.client.Client'>"
        assert (
            str(ris["gc_storage_client"]) == gcs_string and
            str(ris["gc_bigquery_client"]) == gcbq_string
        )

    def test_fapi_token(self):
        token = self.br.get_fapi_token()
        rs = ("https://www.googleapis.com/oauth2/v1/"
              f"tokeninfo?access_token={token}")
        r = requests.get(rs)
        assert r.status_code == 200


class TestBaseResource:

    bar = BaseAnVILResource()

    def test_info_abstract(self):
        with pytest.raises(NotImplementedError):
            self.bar.getinfo()

    def test_hash_abstract(self):
        with pytest.raises(KeyError):
            self.bar.__hash__()

    def test_eq_abstract(self):
        with pytest.raises(NotImplementedError):
            self.bar.__eq__(self.bar)

    def test_str_abstract(self):
        with pytest.raises(KeyError):
            self.bar.__str__()

    def test_ne_abstract(self):
        with pytest.raises(NotImplementedError):
            self.bar.__ne__(self.bar)


class TestBaseAnVILFolder:

    def test_init(self):
        name = "Steve the folder"
        last_modified = "Never"
        baf = BaseAnVILFolder(name, last_modified)
        assert (
            baf.name == name + "/" and
            baf.last_modified == last_modified and
            baf.children == {}
        )

    def test_lazy_init_abstract(self):
        baf = BaseAnVILFolder("Francesca Folder")
        with pytest.raises(NotImplementedError):
            baf.lazy_init()

    def test_hash(self):
        name = "Bort"
        last_modified = "Half past never"
        hashed = BaseAnVILFolder(name, last_modified).__hash__()
        assert hashed == hash((name + "/", "BaseAnVILFolder", last_modified))

    def test_eq(self):
        name1 = "Bort"
        lm1 = "Never"
        name2 = "Bort"
        lm2 = "Never"
        baf1 = BaseAnVILFolder(name1, lm1)
        baf2 = BaseAnVILFolder(name2, lm2)
        assert baf1 == baf2

    def test_ne(self):
        name1 = "Bort"
        lm1 = "Never"
        name2 = "Bart"
        lm2 = "Never"
        baf1 = BaseAnVILFolder(name1, lm1)
        baf2 = BaseAnVILFolder(name2, lm2)
        assert baf1 != baf2

    def test_keys(self):
        # requires initialized object
        baf = BaseAnVILFolder("Qwerty")
        baf.initialized = True  # circumvent abstract init
        baf["A"] = "1"
        baf["B"] = "2"
        assert baf.keys() == ["A", "B"]

    def test_setitem(self):
        baf = BaseAnVILFolder("Qwerty")
        baf["A"] = "1"
        assert baf.children["A"] == "1"

    def test_getitem(self):
        # requires initialized object
        baf = BaseAnVILFolder("Qwerty")
        baf.initialized = True  # circumvent abstract init
        baf.children["A"] = "1"
        assert baf["A"] == "1"

    def test_get_object_from_path(self):
        # requires initialized object
        name = "Qwerty"
        subname = "Asdf"
        baf = BaseAnVILFolder(name)
        baf.initialized = True  # circumvent abstract init
        subbaf = BaseAnVILFolder(subname)
        subbaf.initialized = True
        baf[subbaf.name] = subbaf
        subbaf["A"] = "1"
        assert baf.get_object_from_path(subname + "/A") == "1"

    def test_iter(self):
        baf = BaseAnVILFolder("Hello")
        for x in [("A", "1"), ("B", "2"), ("C", "3")]:
            baf[x[0]] = x[1]
        keys = [x for x in baf]
        assert keys == ["A", "B", "C"]

    def test_getinfo(self):
        name = "beep"
        lm = None
        baf = BaseAnVILFolder(name, lm)
        info = baf.getinfo()
        expected_info = Info({
            "basic": {
                "name": name + "/",
                "is_dir": True,
            },
            "details": {
                "type": ResourceType.directory,
                "modified": lm
            }
        })
        assert (
            info == expected_info
        )

    def test_is_linkable_file(self):
        gs = "gs://some-bucket/some/file"
        drs = "drs://some-bucket/some/file"
        nada = "hey i'm a string look at meeee"
        baf = BaseAnVILFolder("A")
        assert (
            baf.is_linkable_file(gs).__name__ == "GoogleAnVILFile" and
            baf.is_linkable_file(drs).__name__ == "DRSAnVILFile" and
            baf.is_linkable_file(nada) is None
        )


class TestBaseAnVILFile:
    def test_init(self):
        name = "bort"
        size = 2
        lm = "Last Blurnsday"
        baf = BaseAnVILFile(name, size, lm)
        assert (
            baf.name == name and
            baf.size == size and
            baf.last_modified == lm
        )

    def test_getinfo(self):
        name = "itchy"
        size = 3
        lm = "Boxing Day"
        baf = BaseAnVILFile(name, size, lm)
        i = Info(
            {
                "basic": {
                    "name": name,
                    "is_dir": False,
                },
                "details": {
                    "type": ResourceType.file,
                    "size": size,
                    "modified": lm
                }
            }
        )
        assert baf.getinfo() == i

    def test_string_to_buffer(self):
        string = "Strrriiiiinnggggg"
        baf = BaseAnVILFile("A", len(string))
        buff = baf.string_to_buffer(string)
        assert buff.read() == string.encode('utf-8')

    def test_get_bytes_handler(self):
        with pytest.raises(NotImplementedError):
            BaseAnVILFile("1", 2).get_bytes_handler()
