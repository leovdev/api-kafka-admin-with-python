from typing import Union

from fastapi import FastAPI
import subprocess
import json

app = FastAPI()

@app.get("/v1/topics")
def read_root():
    container_route = "/usr/local/kafka/bin/kafka-configs"
    route = "/opt/confluent-7.3.1/bin/kafka-topics"
    try:
        result = subprocess.run(["sudo","/opt/confluent-7.3.1/bin/kafka-configs", 
                                 "--bootstrap-server", "localhost:9094", "--all" ,"--entity-type" ,
                                 "brokers", "--entity-name", "1", "--describe"], 
                                 stdout=subprocess.PIPE, timeout=10,
                                 stderr=subprocess.PIPE,check=False)
        
        print("returnCode: "+ str(result.returncode))
    except subprocess.TimeoutExpired:
        print("An exception ocurred")
        return {"error": "Time Out"}
    except:
        print("An exception ocurred")
        return {"error": "World"}
    return {"Hello": "World", "result":result.stdout, "error":result.stderr}

# @app.get("/v1/topics1")
# def read_root():
#     try:
#         result = subprocess.run(["sh","/usr/local/kafka/bin/kafka-configs.sh", "--bootstrap-server", "kafkaBroker1:9077", "--all" ,"--entity-type" ,"brokers", "--entity-name", "2", "--describe"], 
#                             check=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True)
#     except:
#         print("An exception ocurred")
#     return {"returnCode": result.returncode if result != None else "NULL", "result":result.stdout, "error":result.stderr}

# #Validaciones extremas con entrada, para no frenar el subprocess. 
# # Por ejemplo verificar siexiste el broker con id4 antes del subprocess
