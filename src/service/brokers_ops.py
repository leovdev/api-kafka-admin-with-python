import subprocess
import os

from src.exception.database_ops_exception import DatabaseOpsException
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException
from src.client.kafka_client import verify_broker_running, execute_command_get_topics, execute_command_insert_topic, execute_command_delete_topic, execute_command_update_topic, execute_command_get_topics_detailed
from src.repository.i_repository import IRepository

from src.exception.kafka_client_exception import KafkaClientException

class BrokersOps():

    def list_topics(self):
        detailed=True
        try:
            repository = IRepository()
            database_topics_list = repository.get_topics()

            is_broker_running = verify_broker_running()
            if is_broker_running:
                if(detailed):
                    topics_list = self.get_topics_from_broker_detailed()
                else:
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
     
        return {"broker": topics_list, "database":database_topics_list}

    def get_topics_from_broker(self):
        result = execute_command_get_topics()
        
        if (result.returncode != 0):
            print("Command result: ", result.stdout, "error", result.stderr)
            raise KafkaAdminException("Non-zero exit --list")
        elif (result != None and result.returncode == 0):
            topics_list = self.assign_value(result)
        elif (result != None and result.returncode == 0):
            topics_list = self.assign_value(result)
        return topics_list

    def get_topics_from_broker_detailed(self):
        result = execute_command_get_topics_detailed()
        
        if (result.returncode != 0):
            print("Command result: ", result.stdout, "error", result.stderr)
            raise KafkaAdminException("Non-zero exit --describe")
        elif (result != None and result.returncode == 0):
            topics_list = self.assign_value_detailed(result)
        elif(result != None and result.returncode == 0):
            topics_list = None
        return topics_list

    def parse_stdout_detailed(self, result):
        topics = result.stdout.splitlines()
        topics_list=[]
        for topic in topics:
            topic = topic.decode('UTF-8')
            topic = topic.split('\t')
            topics_list.append(topic.decode('UTF-8'))

        return topics_list
    
    def assign_value_detailed(self, result):
        print("concrete assign value detailed", result)
        topics_list = self.parse_stdout_detailed(result)
        if len(topics_list)>0:
            if len(topics_list) == 1 and topics_list[0]=="":
                topics_list=None
            else:
                topics_list = {"topics":topics_list}
        else:
            topics_list = None

        return topics_list
    
    def assign_value(self, result):
        print("concrete assign value", result)
        topics_list = self.parse_stdout(result)
        if len(topics_list)>0:
            if len(topics_list) == 1 and topics_list[0]=="":
                topics_list=None
            else:
                topics_list = {"topics":topics_list}
        else:
            topics_list = None

        return topics_list
    
    def parse_stdout_detailed(self, result):
        topics = result.stdout.splitlines()
        topics_list=[]
        for topic in topics:
            topic = topic.decode('UTF-8')
            topic = topic.split('\t')
            
            name = topic[0]
            name = name[6:len(name)]
            
            partitions = topic[2]
            partitions = partitions[16:len(partitions)]

            replication_factor = topic[3]
            replication_factor = replication_factor[18:len(replication_factor)]

            configs = topic[4]
            configsOverride = []

            if len(configs)>12:
                pure_config = configs[9:len(configs)]
                pure_config = pure_config.split(',')
                for cf in pure_config:
                    cf = cf.split('=')
                    print({"name":cf[0], "value":cf[1]})
                    configsOverride.append({"name":cf[0], "value":cf[1]})
            else:
                configs = None
            if name != "" and partitions != "":
                topic_metadata = {
                    "name": name,
                    "partitions": partitions,
                    "replicationFactor": replication_factor,
                    "configs": configsOverride
                }
                topics_list.append(topic_metadata)

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
            databaseResult = f"{e}"
        except KafkaClientException as e:
            print("kafka client exception",e)
            brokerResponse = f"{e}"
        except Exception as e:
            print("insert service",e)
            raise Exception(e)
        
        return {"database":databaseResult, "broker":brokerResponse}
    
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

        