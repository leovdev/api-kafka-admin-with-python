import subprocess
import os
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_client_exception import KafkaClientException

BOOTSTRAPSERVER_HOST = os.environ.get('BOOTSTRAPSERVER_HOST')
BOOTSTRAPSERVER_PORT = os.environ.get('BOOTSTRAPSERVER_PORT')
URI = f'{BOOTSTRAPSERVER_HOST}:{BOOTSTRAPSERVER_PORT}'

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
                raise KafkaClientException("Your Config keys or values should be reviewed", result.stderr)
            else:
                 print("Topic inserted successfully", result.returncode)
                 
        except KafkaClientException as e:
             raise KafkaClientException(e)
        except Exception as e:
             print("Command insert Excep", e)
             raise Exception(e)
        
        return True

def execute_command_update_topic(topic):
        commands = ["kafka-topics", "--create", "--bootstrap-server", URI]
        commands.append("--topic", topic['name'])
        
        if 'partitions' in topic:
             commands.append("--partitions", topic['partition'])
        
        if 'replicationFactor' in topic:
             commands.append("--replication-factor", topic['replicationFactor'])
        
        if 'configOverrides' in topic:
             for conf in topic['configOverrides']:
                 key=conf['name']
                 value=conf['value']
                 commands.append("--config", f'{key}={value}')
        
        print(commands)
        return subprocess.run(commands, stdout=subprocess.PIPE, timeout=10,
                            stderr=subprocess.PIPE, check=False)

def execute_command_delete_topic(topic):
        commands = ["kafka-topics", "--create", "--bootstrap-server", URI]
        commands.append("--topic", topic['name'])
        
        if 'partitions' in topic:
             commands.append("--partitions", topic['partition'])
        
        if 'replicationFactor' in topic:
             commands.append("--replication-factor", topic['replicationFactor'])
        
        if 'configOverrides' in topic:
             for conf in topic['configOverrides']:
                 key=conf['name']
                 value=conf['value']
                 commands.append("--config", f'{key}={value}')
        
        print(commands)
        return subprocess.run(commands, stdout=subprocess.PIPE, timeout=10,
                            stderr=subprocess.PIPE, check=False)