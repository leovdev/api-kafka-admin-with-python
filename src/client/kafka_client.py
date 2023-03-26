import subprocess
import os
from src.exception.broker_not_running_exception import BrokerNotRunningException

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