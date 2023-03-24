import subprocess
import os


from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException
from src.utils.utils_ops import verify_broker_running

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
                                    stderr=subprocess.PIPE, check=False)
            
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
                print("concrete 0")
                raise KafkaAdminException()

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
            print("concrete 4")
            raise Exception(e)
                
        return {"returnCode": response_returncode, 
                "message": response_message,
                "bodyResponse": bodyResponse}
    