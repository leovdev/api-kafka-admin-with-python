import subprocess

def verify_broker_running(self, host, port):
    try:
        result = subprocess.run(["ping", "localhost:9094"],
                                 stdout=subprocess.PIPE, timeout=3,
                                 stderr=subprocess.PIPE, check=False)
        if result==0:
            print(result.stdout)
        else:
            print("Error ",result.stderr)
            raise Exception("Kafka Broker is not running")
    except Exception as e:
        print("ping",e)
        response_returncode=500
        response_message='Internal Server Error '+str(e)+', report to the administrator'
        bodyResponse = None

        return {"returnCode": response_returncode, 
                    "message": response_message,
                    "bodyResponse": bodyResponse}
