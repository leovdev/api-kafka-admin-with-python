
import subprocess

from src.service.brokers_ops import BrokersOps
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException

class IBrokersOps():
   
    def list_topics(self):
        try:
            return BrokersOps.list_topics(self)
        except BrokerNotRunningException as e:
            print("abstrac 1")
            raise BrokerNotRunningException(e)
        except KafkaAdminException as e:
            print("abstrac 2")
            raise KafkaAdminException(e)
        except subprocess.TimeoutExpired as e:
            print("abstrac 3")
            raise subprocess.TimeoutExpired(e)
        except Exception as e:
            print("abstrac 4")
            raise Exception(e)

    def get_topics_from_broker(self):
        return BrokersOps.get_topics_from_broker(self)
    
    def assign_value(self, result):
        return BrokersOps.assign_value(self, result)

    def parse_stdout(self, result):
        return BrokersOps.parse_stdout(self, result)