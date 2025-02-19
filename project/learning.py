from fastapi import FastAPI, Request
from pydantic import BaseModel, validator
from typing import List
import datetime  # Keep module-level import
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from kafka import KafkaProducer
import os 
import json 
import logging


logger = logging.getLogger(__name__)


app = FastAPI()

class Item(BaseModel):
    post_title : str
    id : str
    subreddit_name : str
    body : str
    category : str
    comment : List[str]
    posting_date : str
    
    @validator("posting_date", pre=True)
    def convert_datetime_to_timestamp(cls, value):
        if isinstance(value, datetime.datetime):
            return int(value.timestamp()) 
        return value

@app.post("/producer")
async def producer(request: Request, topic: str):  
    bootstrap_servers = "kafka1:19092"  

    # Create topic if not exists
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        new_topic = NewTopic(  
            name=topic, 
            num_partitions=3, 
            replication_factor=1
        )
        admin_client.create_topics([new_topic]) 
        logger.info("Successfully created Kafka topic")
    except NoBrokersAvailable:
        logger.warning("Failed to connect to Kafka brokers")
    except TopicAlreadyExistsError:
        logger.warning("Topic already exists")
    except Exception as e:
        logger.error(f"Unexpected error creating topic: {str(e)}")
    finally:
        if 'admin_client' in locals():
            admin_client.close()

    body = await request.json()

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        return {"error": f"Kafka producer initialization failed: {str(e)}"}

    try:
        
        future = producer.send(topic, value=body)
        result = future.get(timeout=10)
        return {"message": "Message sent successfully", "data": body}
    except Exception as e:
        return {"error": f"Failed to send message: {str(e)}"}
    finally:
        producer.close()