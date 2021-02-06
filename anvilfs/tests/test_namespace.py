from anvilfs.anvilfs import Namespace


class TestNamespace:
    def test_init(self):
        name = "Space Name"
        ns = Namespace(name)
        assert ns.name == name + "/"

    def test_fetch_workspace(self, valid_gs_info):
        info = valid_gs_info
        ws = Namespace(info["Namespace"]).fetch_workspace(
            info["Workspace"]
        )
        assert ws.name == info["Workspace"] + "/"

    def test_str(self):
        name = "nsname"
        ns = Namespace(name)
        assert str(ns) == f"<Namespace {name}/>"
