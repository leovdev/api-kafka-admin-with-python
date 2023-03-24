import subprocess
import os
from src.exception.broker_not_running_exception import BrokerNotRunningException

def verify_broker_running():
    try:
        BOOTSTRAPSERVER_HOST = os.getenv('BOOTSTRAPSERVER_HOST')
        BOOTSTRAPSERVER_PORT = os.environ.get('BOOTSTRAPSERVER_PORT')

        result = subprocess.run(["kafka-broker-api-versions", "--bootstrap-server", 
                                 "localhost:9094"],
                                 stdout=subprocess.PIPE, timeout=10,
                                 stderr=subprocess.PIPE, check=True)
        
        if result.returncode != 0:
            raise BrokerNotRunningException()
        
    except subprocess.TimeoutExpired as e:
        raise subprocess.TimeoutExpired

    return True