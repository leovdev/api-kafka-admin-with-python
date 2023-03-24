import subprocess
import os
from src.exception.broker_not_running_exception import BrokerNotRunningException

def verify_broker_running():
    try:
        BOOTSTRAPSERVER_HOST = os.environ.get('BOOTSTRAPSERVER_HOST')
        BOOTSTRAPSERVER_PORT = os.environ.get('BOOTSTRAPSERVER_PORT')
        URI = "{BOOTSTRAPSERVER_HOST:BOOTSTRAPSERVER_PORT}"
        result = subprocess.run(["kafka-broker-api-versions", "--bootstrap-server", 
                                 BOOTSTRAPSERVER_HOST+":"+BOOTSTRAPSERVER_PORT],
                                 stdout=subprocess.PIPE, timeout=10,
                                 stderr=subprocess.PIPE, check=False)
        
        if result.returncode != 0:
            print("util broker not 0 ", result.stderr)
            raise BrokerNotRunningException("Broker may not be running, report to the administrator")
        
    except subprocess.TimeoutExpired as e:
        raise subprocess.TimeoutExpired(e)
    except BrokerNotRunningException as e:
        print("util broker not 1", e)
        raise BrokerNotRunningException(e)
    except Exception as e:
        print("util broker not 2",e)
        raise BrokerNotRunningException()
    return True

def execute_command_get_topics():
        BOOTSTRAPSERVER_HOST = os.environ.get('BOOTSTRAPSERVER_HOST')
        BOOTSTRAPSERVER_PORT = os.environ.get('BOOTSTRAPSERVER_PORT')
        print("envars", BOOTSTRAPSERVER_HOST, BOOTSTRAPSERVER_PORT )
        return subprocess.run(["kafka-topics", 
                                    "--list", "--bootstrap-server", 
                                    BOOTSTRAPSERVER_HOST+":"+
                                    BOOTSTRAPSERVER_PORT],
                                    stdout=subprocess.PIPE, timeout=10,
                                    stderr=subprocess.PIPE, check=False)