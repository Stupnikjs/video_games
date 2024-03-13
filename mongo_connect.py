from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os 
from dotenv import load_dotenv


def load_client() -> MongoClient:

    load_dotenv()

    password = os.getenv("MONGO_PASSWORD")

    uri = f"mongodb+srv://Stupnikjs:{password}@cluster0.oiowsz6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    if client.is_mongos:
        print('Client connected to mongodb')
    return client