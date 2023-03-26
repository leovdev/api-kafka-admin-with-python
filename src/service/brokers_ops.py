import subprocess
import os

from src.exception.database_ops_exception import DatabaseOpsException
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException
from src.client.kafka_client import verify_broker_running, execute_command_get_topics, execute_command_insert_topic, execute_command_delete_topic, execute_command_update_topic
from src.repository.i_repository import IRepository

from src.exception.kafka_client_exception import KafkaClientException

class BrokersOps():

    def list_topics(self):
       
        try:
            repository = IRepository()
            database_topics_list = repository.get_topics()

            is_broker_running = verify_broker_running()
            if is_broker_running:
                topics_list = self.get_topics_from_broker()
            else:
                raise BrokerNotRunningException("e")
            
        except BrokerNotRunningException as e:
            raise BrokerNotRunningException(e)
        except subprocess.TimeoutExpired as e:
            raise subprocess.TimeoutExpired(e)
        except KafkaAdminException as e:
            raise KafkaAdminException(e)
        except Exception as e:
            print("concrete 4", e)
            raise Exception(e)
                
        return {"kafkaBroker": topics_list, "mongoDB":database_topics_list}

    def get_topics_from_broker(self):
        result = execute_command_get_topics()
        
        if (result.returncode != 0):
            print("Command result: ", result.stdout, "error", result.stderr)
            raise KafkaAdminException("Non-zero exit --list")
        elif (result != None and result.returncode == 0):
            topics_list = self.assign_value(result)

        return topics_list

    def assign_value(self, result):
        print("concrete asign value")
        topics_list = self.parse_stdout(result)
        if len(topics_list)>0:
            if len(topics_list) == 1 and topics_list[0]=="":
                topics_list=None
            else:
                topics_list = {"topics":topics_list}
        else:
            topics_list = None

        return topics_list

    def parse_stdout(self, result):
        topics = result.stdout.splitlines()
        topics_list=[]
        for topic in topics:
            topics_list.append(topic.decode('UTF-8'))
        return topics_list

    def insert_topic(self, topic):
        repository = IRepository()
        try:
            databaseResult = repository.insert_topic(topic)
            brokerResponse = execute_command_insert_topic(topic)
        except DatabaseOpsException as e:
            print(e)
            result = {"database":"Error inserting topic in DB"}
        except KafkaClientException as e:
            print(e)
            brokerResponse = {"broker":"Error inserting in broker"}
        except Exception as e:
            print(e)
            raise Exception
        
        return {databaseResult, brokerResponse}
    
    def update_topic(self, topic):
        repository = IRepository()

        result = repository.update_topic(topic)

        brokerResponse = execute_command_update_topic(topic)

        return True
    
    def delete_topic(self, topic):
        repository = IRepository()

        result = repository.delete_topic(topic)

        brokerResponse = execute_command_delete_topic(topic)

        return True

        