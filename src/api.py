from src.service.i_brokers_ops import IBrokersOps
from src.exception.broker_not_running_exception import BrokerNotRunningException
from src.exception.kafka_admin_exception import KafkaAdminException
from src.repository.i_repository import IRepository
from fastapi import FastAPI, Response, status, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.responses import PlainTextResponse
import subprocess
import json 
from src.schema.schemas import TopicBaseSchema

server = FastAPI()

@server.get("/v1/topics")
def read_root(response: Response):
    try:
        service=IBrokersOps()
        topics_list=service.list_topics()
         
        if topics_list['database']==None and topics_list['broker']==None:
            response.status_code=204
        else:
            response.status_code=200
        return topics_list
    
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Internal Server Error. "+str(e.message) if hasattr(e,'message') else None)     

@server.post("/v1/topics/create")
def create_topic(topic: TopicBaseSchema,response: Response):
    try:
        json_compatible_item_data = jsonable_encoder(topic)
        
        service=IBrokersOps()
        response=service.insert_topic(json_compatible_item_data)
        print(response)

        return response
        # if topics_list['kafkaBroker']==None and topics_list['mongoDB']==None:
        #     response.status_code=204
        # else:
        #     response.status_code=200
        # return topics_list
    
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Internal Server Error. "+str(e.message) if hasattr(e,'message') else None)     
 
@server.get("/v1/broker/configs")
def read_root():
    container_route = "/usr/local/kafka/bin/kafka-configs"
    route = "/opt/confluent-7.3.1/bin/kafka-topics"
    try:
        result = subprocess.run(["kafka-configs", 
                                 "--bootstrap-server", "localhost:9094", "--all" ,
                                 "--entity-type", "brokers", "--entity-name", "1",
                                 "--describe"], stdout=subprocess.PIPE, timeout=10,
                                 stderr=subprocess.PIPE, check=False)
        
        print("returnCode: "+ str(result.returncode))
    except subprocess.TimeoutExpired as e:
        print("An exception ocurred",e)
        return {"error": "Time Out"}
    except Exception as e:
        print("An exception ocurred", e)
        return {"error": "World"}
    return {"Hello": "World", "result":result.stdout, "error":result.stderr}

# @server.get("/v1/topics1")
# def read_root():
#     try:
#         result = subprocess.run(["sh","/usr/local/kafka/bin/kafka-configs.sh", "--bootstrap-server", "kafkaBroker1:9077", "--all" ,"--entity-type" ,"brokers", "--entity-name", "2", "--describe"], 
#                             check=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True)
#     except:
#         print("An exception ocurred")
#     return {"returnCode": result.returncode if result != None else "NULL", "result":result.stdout, "error":result.stderr}

# #Validaciones extremas con entrada, para no frenar el subprocess. 
# # Por ejemplo verificar siexiste el broker con id4 antes del subprocess
