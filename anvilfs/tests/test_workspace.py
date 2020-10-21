import pytest

from anvilfs.anvilfs import Workspace

class TestWorkspace:
    def test_init(self, valid_gs_info):
        ns_name = valid_gs_info["Namespace"]
        ws_name = valid_gs_info["Workspace"]
        ws = Workspace(ns_name, ws_name)
        assert ws.name == f"{ws_name}/"