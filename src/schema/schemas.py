from datetime import datetime
from typing import List
from pydantic import BaseModel, EmailStr, constr
from bson.objectid import ObjectId

class TopicBaseSchema(BaseModel):
    name: str
    partitions: int
    replicationFactor: int
    configOverrides: List

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

class FilteredTopicResponse(TopicBaseSchema):
    name: str

class TopicResponse(TopicBaseSchema):
    name: str

class ListPostResponse(BaseModel):
    results: int
    topics: List[TopicResponse]