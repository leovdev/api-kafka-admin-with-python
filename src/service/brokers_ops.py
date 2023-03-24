import subprocess
import os


from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException
from src.client.kafka_client import verify_broker_running, execute_command_get_topics

class BrokersOps():

    def list_topics(self):
        topics_list=[]

        try:
            is_broker_running = verify_broker_running()
            print("is-broker-running", is_broker_running)
            result = execute_command_get_topics()
            
            if (result.returncode != 0):
                print("concrete 0", result.stdout, "error", result.stderr)
                raise KafkaAdminException("Non-zero exit --list")
            elif (result != None and result.returncode == 0):
                topics = result.stdout.splitlines()

                for topic in topics:
                    topics_list.append(topic.decode('UTF-8'))

                if len(topics_list) == 1 and topics_list[0]=="":
                    topics_list=None
                else:
                    topics_list = {"topics":topics_list}
            

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
                
        return topics_list

    
    