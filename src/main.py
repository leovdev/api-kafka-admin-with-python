from typing import Union

from fastapi import FastAPI
import subprocess


app = FastAPI()

@app.get("/v1/topics")
def read_root():
    result = subprocess.run(["sh","/usr/local/kafka/bin/kafka-configs.sh", "--bootstrap-server", "kafkaBroker1:9077", "--all" ,"--entity-type" ,"brokers", "--entity-name", "1", "--describe"], check=True, stdout=subprocess.PIPE, universal_newlines=True)
    
    return {"Hello": "World", "result":result.stdout}


#subprocess.Popen(f"curl --location 'http://localhost:8000/v1/topics/'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
#command = "echo prueba"
#response = os.system(command)


#result = subprocess.run(['ls','-lha'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#command = "sudo /opt/confluent-7.3.1/bin/kafka-configs --bootstrap-server localhost:9097 --all --entity-type brokers --entity-name 1 --describe"
#response = os.system(command)
#print(subprocess.STDOUT) 