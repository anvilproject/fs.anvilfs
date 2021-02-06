from .clientrepository import ClientRepository


class BaseAnVILResource(ClientRepository):
    def getinfo(self):
        raise NotImplementedError("Method getinfo() not implemented")

    def __hash__(self):
        return hash((self.name, self.__class__.__name__))

    def __eq__(self, other):
        return self.getinfo().raw == other.getinfo().raw

    def __str__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)

    def __ne__(self, other):
        return not(self == other)
