class BrokerNotRunningException(Exception):
    def __init__(self, URI, message="Broker with URI {URI} may not be running"):
        self.message = message
        self.URI = URI
        super().__init__(self.message)