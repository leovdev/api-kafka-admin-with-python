import os
import json
from pymongo import MongoClient
from pymongo.errors import InvalidName
from bson.json_util import dumps, loads
from pymongo.collection import ReturnDocument
from src.serializers.topic_serializers import topicEntity, topicListEntity
from src.exception.mongo_connection_exception import MongoConnectionException
from src.exception.database_connection_exception import DatabaseConnectionException
from src.exception.database_ops_exception import DatabaseOpsException

MONGODB_HOST = os.environ.get('MONGODB_HOST') or 'localhost'
MONGODB_PORT = os.environ.get('MOONGODB_PORT') or '27017'
URI = f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/'
    
class MongoDB():
    def __init__(self) -> None:
        pass
    
    def connect(self):
        try:
            client =  MongoClient(URI, username='root', password='root')
        except Exception as e:
            print('exception', e)
            raise MongoConnectionException()
        return client
    

    def get_topics(self):
        try:
            client = self.connect()
            topics = client.kafka_topics.topics.find({}, {"_id":0, "price":0, "origin":0})
            topics = list(topics) #Para enviar json completo, enviarlo así tal cual
            topics_count = len(list(topics))
            
            if topics_count == 0:
                return None
            
            client.close()
            detailed_list = True
            if (detailed_list):
                return {"topics": topics}
            
            topics_names_list = self.extract_only_topic_names(topics)
           
        except InvalidName as e:
            print(e,"cause 0", e.cause)
        except MongoConnectionException as e:
            print(e,"cause 1", e.cause)
            raise MongoConnectionException()
        except Exception as e:
            print(e,"cause 2")
            return None

        return {"topics": topics_names_list}

    def extract_only_topic_names(self, topics_list, topics):
        topics_list = []
        for topic in topics:
            if 'name' in topic:
                print('foreach', topic )
                topics_list.append(topic['name'])
        return topics_list
    

    def insert_topic(self, topic):
        try:
            
            client = self.connect()
            db = client.kafka_topics
            database_response = db.topics.find({"name":topic['name']})
            count = len(list(database_response))
            exist= True if count>0 else False
            
            if (not exist):
                db.topics.insert_one(topic)

                print("interg",{"name":topic['name']})
                client.close()
                return "Topic created"
            else:
                return "Topic already exists"
        except InvalidName as e:
            pass
        except MongoConnectionException as e:
            raise DatabaseOpsException(e)
        except Exception as e:
            raise DatabaseOpsException(e)
        
        return "Topic could not be created"
    
    def update_topic_db(self, topic):
        overrides_updated = None
        try:
            client = self.connect()
            db = client.kafka_topics
            
       
            if ('removeConfigOverrides' in topic or 'addConfigOverrides' in topic):
                current = db.topics.find_one({"name": topic["name"]})
                handle_overrides = current['configOverrides']
                if ('removeConfigOverrides' in topic and topic['removeConfigOverrides'] != None and len(topic['removeConfigOverrides']) > 0):
                    object_with_overrides_removed= self.check_config_overrides_existance(topic, handle_overrides)
                    overrides_updated = self.update_overrides_object(topic, object_with_overrides_removed)
                
                if ('addConfigOverrides' in topic and topic['addConfigOverrides'] != None and len(topic['addConfigOverrides']) > 0):
                    overrides_updated = self.update_overrides_object(topic, handle_overrides)
                
                if 'removeConfigOverrides' in topic:
                    topic.pop('removeConfigOverrides')
                if 'addConfigOverrides' in topic:    
                    topic.pop('addConfigOverrides')
                
            topic['configOverrides']=current['configOverrides']

            updated = db.topics.find_one_and_update(
                {'name':topic['name']},
                {'$set': topic},
                upsert= True,
                return_document=ReturnDocument.AFTER
            )
            print("respuesta", f"{updated}", "id ")
        except InvalidName as e:
            print(e,"cause 0", e.cause)
        except DatabaseOpsException as e:
            print(e)
            raise DatabaseOpsException(e)
        except MongoConnectionException as e:
            print(e)
            raise MongoConnectionException(e)
        except Exception as e:
            print(e)
            topics_list=None
        client.close()
        return None

    def update_overrides_object(self, topic, handle_overrides):
        dead_indexes=[]
        new_overrides = topic['addConfigOverrides']

        for i in range(len(new_overrides)):
            for j in range(len(handle_overrides)):
                print(new_overrides[i]['name'],handle_overrides[j]['name'])
                if new_overrides[i]['name'] == handle_overrides[j]['name']:
                    dead_indexes.append(j)
                        
        print(dead_indexes)
        for i in range(len(dead_indexes)):
            handle_overrides.pop(i)
        for i in new_overrides:
            handle_overrides.append(i['name'])
        print(handle_overrides)
        return handle_overrides
    
    def remove_overrides(self, topic, handle_overrides):
        self.check_config_overrides_existance(topic, handle_overrides)
        dead_indexes=[]
        new_overrides = topic['removeConfigOverrides']

        for i in range(len(new_overrides)):
            for j in range(len(handle_overrides)):
                print(new_overrides[i],handle_overrides[j]['name'])
                if new_overrides[i] == handle_overrides[j]['name']:
                    dead_indexes.append(j)
                        
        print(dead_indexes)
        for i in range(len(dead_indexes)):
            handle_overrides.pop(i)

        print(handle_overrides)
        return handle_overrides

    def check_config_overrides_existance(self, topic, current_overrides):
        not_setted = []
        setted = []

        for i in  current_overrides:
            setted.append(i['name'])
            
        for i in topic["removeConfigOverrides"]:
            if not i in setted:
                not_setted.append(i)
            
        if (len(not_setted)>0):
            raise DatabaseOpsException(f"Configs are not overrided, so can not be deleted: {not_setted}")
  
    def delete_topic_db(self, topic_name):
        try:
            client = self.connect()
            print("respuesta 1")
            db = client.kafka_topics
            print("respuesta 2")
            topics = db.topics.delete_one({'name':topic_name})
            count = topics.deleted_count
            client.close()

            if(count == 0):
                print("dsfads", count)
                return f"Topic {topic_name} does not exist"
            
        except InvalidName as e:
            print(e)
        except MongoConnectionException as e:
            print(e)
            raise MongoConnectionException(e)
        except Exception as e:
            print(e)
            raise Exception(e)
            
       
        return f"Topic {topic_name} deleted"
        