class DatabaseProxy:

    def connect(self):
        raise NotImplementedError

    def insert(self, item):
        raise NotImplementedError

    def find_one(self, search):
        raise NotImplementedError

    def find_many(self, search):
        raise NotImplementedError
