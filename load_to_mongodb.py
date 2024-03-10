import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os 
from dotenv import load_dotenv

CURR_DIR = os.getcwd()


load_dotenv()

password = os.getenv("MONGO_PASSWORD")
print(password)
uri = f"mongodb+srv://Stupnikjs:{password}@cluster0.oiowsz6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.get_database('Cluster0')
    col = db.get_collection('games_rating')

    with open(f'{CURR_DIR}/games_ratings/Video_Games_5.json', 'r') as file:
    # Read the file line by line
        for line in file:
            # Load the JSON object from each line
            json_data = json.loads(line)

            # Insert the JSON data into the collection
            col.insert_one(json_data)
    

except Exception as e:
    print(e)

