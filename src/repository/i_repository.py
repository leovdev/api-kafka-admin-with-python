from src.repository.mongo import MongoDB

class IRepository():
   
    def get_topics(self):
        database = MongoDB()
        return database.get_topics()
    
    def insert_topic(self, topic):
        database = MongoDB()
        return database.insert_topic(topic)
    
    def update_topic(self, topic):
        database = MongoDB()
        return database.update_topic_db(topic)
    
    def delete_topic_db(self,topic):
        database = MongoDB()
        return database.delete_topic_db(topic)