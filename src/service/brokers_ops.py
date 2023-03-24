import subprocess
import os

from src.utils.utils_ops import verify_broker_running
from src.exception.broker_not_running_exception import BrokerNotRunningException

class BrokersOps():

    def list_topics(self):
        print("Llegué a tráves de la interfaz")
        try:
            BOOTSTRAPSERVER_HOST = os.getenv('BOOTSTRAPSERVER_HOST')
            BOOTSTRAPSERVER_PORT = os.environ.get('BOOTSTRAPSERVER_PORT')

            is_broker_running = verify_broker_running()
            print("is-broker-running", is_broker_running)
            result = subprocess.run(["kafka-topics", 
                                    "--list", "--bootstrap-server", 
                                    "{BOOTSTRAPSERVER_HOST}:{BOOTSTRAPSERVER_PORT}"],
                                    stdout=subprocess.PIPE, timeout=10,
                                    stderr=subprocess.PIPE, check=True)
            
            if (result != None and result.returncode == 0):
                topics = result.stdout.splitlines()
                if len(topics)-1 == 0:
                    print('número de topicis',len(topics), topics)
                    response_returncode=204
                    response_message="No topics have been created"
                    bodyResponse = None
                else:
                    print('número de topicis',len(topics), topics)
                    response_returncode=200
                    response_message='Ok'
                    bodyResponse = {'topics' : result.stdout.splitlines()}

            elif (result.returncode != 0):
                print('returncode', result.returncode)
                print(result.stderr)
                response_returncode=500
                response_message='Internal Server Error'
                bodyResponse = None

            
            print("returnCode: "+ str(result.returncode))
        except BrokerNotRunningException as e:
            print("entro aquí")
            raise BrokerNotRunningException("")
        except subprocess.TimeoutExpired as e:
            response_returncode=500
            response_message='Internal Server Error, timeout'
            bodyResponse = None
        # except Exception as e:
        #     response_returncode=500
        #     response_message='Internal Server Error'
        #     bodyResponse = None
        #     print("An exception ocurred, exception x", e)
        #     print(result.stderr)
                
        return {"returnCode": response_returncode, 
                "message": response_message,
                "bodyResponse": bodyResponse}
    