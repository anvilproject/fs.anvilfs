import configparser

# FISS calls configparser.SafeConfigParser at import time, which Python 3.12
# removed. It has been a plain alias of ConfigParser since 3.2, and the call
# site only uses read() and items(), so restoring the name is behaviour
# preserving. Lives here rather than in drs.py so it runs before any submodule
# regardless of import order. Remove once broadinstitute/fiss#194 ships.
if not hasattr(configparser, "SafeConfigParser"):
    configparser.SafeConfigParser = configparser.ConfigParser
