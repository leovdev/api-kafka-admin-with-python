import subprocess

from src.service.brokers_ops import BrokersOps
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException

class IBrokersOps():
   
    def list_topics(self):
        return BrokersOps.list_topics(self)

    def get_topics_from_broker(self):
        return BrokersOps.get_topics_from_broker(self)
    
    def assign_value(self, result):
        return BrokersOps.assign_value(self, result)

    def parse_stdout(self, result):
        return BrokersOps.parse_stdout(self, result)
    
    def get_topics_from_broker_detailed(self):
        return BrokersOps.get_topics_from_broker_detailed(self)
    
    def assign_value_detailed(self, result):
        return BrokersOps.assign_value_detailed(self, result)

    def parse_stdout_detailed(self, result):
        return BrokersOps.parse_stdout_detailed(self, result)
    
    def insert_topic(self, topic):
        return BrokersOps.insert_topic(self, topic)
    
    def update_topic(self, topic):
        return BrokersOps.update_topic(self, topic)
    
    def delete_topic(self, topic):
        return BrokersOps.delete_topic(self, topic)