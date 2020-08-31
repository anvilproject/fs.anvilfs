from inspect import getfullargspec

class BaseTest:
    TEST_INDENT_CHARS=24 #(3 tabs)
    def success(msg, details=None):
        if details:
            msg = msg + f": {details}"
        print(" - SUCCESS: {}".format(msg))
        return (1, 0)

    def failure(msg, details=None):
        if details:
            msg = msg + f": {details}"
        print(" - FAILURE: {}".format(msg))
        return (0, 1)

    @classmethod
    def run_tests(self, *args):
        name = self.__name__[4:] if self.__name__[0:4] == "Test" else self.__name__ 
        tests = self.get_tests()
        print("[ {} ]:\n tests: {}".format(name, tests))
        s = 0
        f = 0
        for t in tests:
            gap = 24 - len(t) + 3
            print(" - {}:{}\t".format(t, " "*gap), end='')
            arg_ct = len(getfullargspec(getattr(self, t)).args)
            # here because of "'NoneType' object is not iterable"?
            #   did you RETURN BaseTest.success/failure?
            _s, _f = getattr(self, t)(*args[0:arg_ct])
            s += _s
            f += _f
        print("\tTOTAL: {} FAILURES out of {} TESTS\n".format(f, s+f))
        return (s,f)

    @classmethod
    def get_tests(self):
        return [f for f in dir(self) if callable(getattr(self, f)) and f[0:5] == "test_" ]

class FakeBlob:
    DEFAULT_NAME = "afile/in/the/bucket.nfo"
    DEFAULT_SIZE = 1
    DEFAULT_UPDATED = "some time"

    def __init__(self, name=DEFAULT_NAME, size=DEFAULT_SIZE, updated=DEFAULT_UPDATED):
        self.name = name
        self.size = size
        self.updated = updated

    def __eq__(self, second):
        return (self.name == second.name
                and self.size == second.size
                and self.updated == second.updated)
