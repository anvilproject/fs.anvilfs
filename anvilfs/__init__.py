import configparser
import inspect

# Both shims below work around dependencies that no longer import on modern
# runtimes and have no fixed release. They run here rather than in the modules
# that trigger them so they apply ahead of every submodule, whatever the import
# order. Each is a no-op once its dependency is repaired.

# FISS calls configparser.SafeConfigParser at import time, which Python 3.12
# removed. It has been a plain alias of ConfigParser since 3.2, and the call
# site only uses read() and items(), so restoring the name is behaviour
# preserving. Remove once broadinstitute/fiss#194 ships.
if not hasattr(configparser, "SafeConfigParser"):
    configparser.SafeConfigParser = configparser.ConfigParser


def _translate_method_whitelist(original_init):
    # original_init is closed over rather than read from module scope, so a
    # second execution of this module cannot leave the installed wrapper
    # calling itself
    def retry_init(self, *args, **kwargs):
        if "method_whitelist" in kwargs:
            kwargs["allowed_methods"] = kwargs.pop("method_whitelist")
        return original_init(self, *args, **kwargs)

    retry_init.anvilfs_translates_method_whitelist = True
    return retry_init


def _install_retry_shim():
    # getm builds Retry(method_whitelist=["HEAD", "GET"]) at import time.
    # urllib3 renamed that argument to allowed_methods in 1.26 and removed it
    # in 2.0, so translate the keyword; pinning urllib3<2 is not an option in a
    # Galaxy process. Remove once getm ships a fix; 0.0.5, the latest, still
    # uses the old name.
    try:
        from urllib3.util.retry import Retry
    except ImportError:
        # absent while the package is being built, when nothing needs shimming
        return
    if getattr(Retry.__init__, "anvilfs_translates_method_whitelist", False):
        return
    if "method_whitelist" in inspect.signature(Retry.__init__).parameters:
        return
    Retry.__init__ = _translate_method_whitelist(Retry.__init__)


_install_retry_shim()
