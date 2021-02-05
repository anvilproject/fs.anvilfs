from anvilfs.anvilfs import Workspace


class TestWorkspace:
    def test_init(self, valid_gs_info):
        ns_name = valid_gs_info["Namespace"]
        ws_name = valid_gs_info["Workspace"]
        ws = Workspace(ns_name, ws_name)
        ws_d = ws.__dict__
        assert (
            ws.name == f"{ws_name}/" and
            "bucket_name" in ws_d and
            "attributes" in ws_d
        )

    def test_lazy_init(self, valid_gs_info):
        # things that will always be present:
        init_folders = [
            "Other Data/",
            "Reference Data/",
            "Tables/"
        ]
        ns_name = valid_gs_info["Namespace"]
        ws_name = valid_gs_info["Workspace"]
        ws = Workspace(ns_name, ws_name)
        assert (
            ws.keys() == init_folders
        )
