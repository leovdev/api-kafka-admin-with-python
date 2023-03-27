import subprocess
import os
import regex

from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_client_exception import KafkaClientException

BOOTSTRAPSERVER_HOST = os.environ.get('BOOTSTRAPSERVER_HOST')
BOOTSTRAPSERVER_PORT = os.environ.get('BOOTSTRAPSERVER_PORT')
URI = f'{BOOTSTRAPSERVER_HOST}:{BOOTSTRAPSERVER_PORT}'
UNKNOW_CONFIG_ERROR_IDENTIFIER = "Unknown topic config name:"

def verify_broker_running():
    try:
        result = subprocess.run(["kafka-broker-api-versions", "--bootstrap-server", 
                                 URI],
                                 stdout=subprocess.PIPE, timeout=10,
                                 stderr=subprocess.PIPE, check=False)
        
        if result.returncode != 0:
            print("Error exit with Non-Zero code", result.stderr)
            raise BrokerNotRunningException("Can not connect to the broker", result.stderr)
        
        return True
    except subprocess.TimeoutExpired as e:
        print("TimeOutException connecting to the broker", e)
    except BrokerNotRunningException as e:
        print("Can not connect to the broker", e)
    except Exception as e:
        print("Unknow exception",e)
    
    return False

def execute_command_get_topics():
        return subprocess.run(["kafka-topics", 
                                    "--list", "--bootstrap-server", URI],
                                    stdout=subprocess.PIPE, timeout=10,
                                    stderr=subprocess.PIPE, check=False)

def execute_command_get_topics_detailed():
        return subprocess.run(["kafka-topics", 
                                    "--describe", "--bootstrap-server", URI],
                                    stdout=subprocess.PIPE, timeout=10,
                                    stderr=subprocess.PIPE, check=False)

def execute_command_insert_topic(topic):
        commands = ["kafka-topics", "--create", "--bootstrap-server", URI]
        commands.append("--topic")
        commands.append(str(topic['name']))

        if 'partitions' in topic:
            commands.append("--partitions")
            commands.append(str(topic['partitions']))
        
        if 'replicationFactor' in topic:
            commands.append("--replication-factor")
            commands.append(str(topic['replicationFactor']))

        if 'configOverrides' in topic:
            for conf in topic['configOverrides']:
                key=conf['name']
                value=conf['value']
                cf=f'{key}={value}'
                commands.append("--config")
                commands.append(cf)
        
        print("Builded command", commands)

        try:
            result = subprocess.run(commands, stdout=subprocess.PIPE, timeout=10,
                            stderr=subprocess.PIPE, check=False)
            
            if result.returncode != 0:
                print("Error exit with Non-Zero code", result.stderr)

                if str(result.stderr).find('already exists.') != -1:
                    raise KafkaClientException("Topic already exists")
                elif str(result.stderr).find(UNKNOW_CONFIG_ERROR_IDENTIFIER) != -1:
                    unknow_config = get_unknow_config_name_from_stderr(result)
                    raise KafkaClientException(f"Unknown topic config name: {unknow_config}")
                raise KafkaClientException(f"Error: {result.stderr}")
            
            print("Topic inserted successfully", result.returncode)
                 
        except KafkaClientException as e:
             raise KafkaClientException(e)
        except Exception as e:
             print("Error: ", e)
             raise Exception(e)
        
        return "Topic created"

def get_unknow_config_name_from_stderr(result):
    regexword = r'\bUnknown topic config name:\s+\K\S+'
    unrecognize_config = regex.search(regexword, str(result.stderr))
    error_config = unrecognize_config[0]
    cutIndex = error_config.find(f"\n")
    unknow_config = error_config[0:cutIndex-1]
    return unknow_config

def execute_command_delete_topic(topic):
        commands = ["kafka-topics", "--delete","--bootstrap-server", URI]
        commands.append("--topic")
        commands.append(topic)

        try:
            result = subprocess.run(commands, stdout=subprocess.PIPE, timeout=10,
                            stderr=subprocess.PIPE, check=False)

            if result.returncode != 0:
                if str(result.stderr).find('does not exist as expected') != -1:
                    raise KafkaClientException(f"Topic {topic} does not exist")
                
        except KafkaClientException as e:
            print(e)
            raise KafkaClientException(e) 
        except Exception as e:
            print(e)
            raise Exception(e)
             

        return f"Topic {topic} deleted"

def execute_command_update_topic(topic):
        commands = ["kafka-topics", "--delete", "--bootstrap-server", URI, "--topic", topic]

        return subprocess.run(commands, stdout=subprocess.PIPE, timeout=10,
                            stderr=subprocess.PIPE, check=False)