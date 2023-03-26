import subprocess
import os


from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException
from src.client.kafka_client import verify_broker_running, execute_command_get_topics
from src.repository.i_repository import IRepository

class BrokersOps():

    def list_topics(self):
       
        try:
            repository = IRepository()
            database_topics_list = repository.get_topics()

            is_broker_running = verify_broker_running()
            if is_broker_running:
                topics_list = self.get_topics_from_broker()

        except BrokerNotRunningException as e:
            print("concrete 1")
            raise BrokerNotRunningException(e)
        except subprocess.TimeoutExpired as e:
            print("concrete 2")
            raise subprocess.TimeoutExpired(e)
        except KafkaAdminException as e:
            print("concrete 3")
            raise KafkaAdminException(e)
        except Exception as e:
            print("concrete 4", e)
            raise Exception(e)
                
        return {"kafkaBroker": topics_list, "MongoDb":database_topics_list}

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

    
    