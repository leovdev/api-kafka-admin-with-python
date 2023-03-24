
from src.service.brokers_ops import BrokersOps

class IBrokersOps():
   
    def list_topics(self):
        try:
            return BrokersOps.list_topics(self)
        except:
            raise Exception
