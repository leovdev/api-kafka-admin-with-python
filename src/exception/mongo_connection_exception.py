class MongoConnectionException(Exception):
    def __init__(self):
        self.message = "Can not connect to Mongo"
        super().__init__(self.message)