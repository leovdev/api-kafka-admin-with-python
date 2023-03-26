
import subprocess

from src.service.brokers_ops import BrokersOps
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException

class IBrokersOps():
   
    def list_topics(self):
        try:
            return BrokersOps.list_topics(self)
        except BrokerNotRunningException as e:
            raise BrokerNotRunningException(e)
        except KafkaAdminException as e:
            raise KafkaAdminException(e)
        except subprocess.TimeoutExpired as e:
            raise subprocess.TimeoutExpired(e)
        except Exception as e:
            raise Exception(e)

    def get_topics_from_broker(self):
        return BrokersOps.get_topics_from_broker(self)
    
    def assign_value(self, result):
        return BrokersOps.assign_value(self, result)

    def parse_stdout(self, result):
        return BrokersOps.parse_stdout(self, result)
    
    def insert_topic(self, topic):
        return BrokersOps.insert_topic(self, topic)
    
    def update_topic(self, topic):
        return BrokersOps.update_topic(self, topic)
    
    def delete_topic(self, topic):
        return BrokersOps.delete_topic(self, topic)