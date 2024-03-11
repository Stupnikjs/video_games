import json
from mongo_connect import load_client
import os 
import time
from pymongo import InsertOne


CURR_DIR = os.getcwd()

client = load_client()

# Create a new client and connect to the server

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.get_database('Cluster0')
    col = db.create_collection('games_rating')

    with open(f'{CURR_DIR}/games_ratings/Video_Games_5.json', 'r') as file:
        lines = file.readlines()
    chunk_size = 1000
    num_chunk = len(lines)//1000
    start = time.time()
    print('starting chunking')
    
    for i in range(num_chunk):
        if i != num_chunk - 1:
            chunk = lines[i*1000:(i+1)*1000]
            bulk_data = [InsertOne(json.loads(line)) for line in chunk]
            # bulk_write to write multiples entries in the collection at once  
            col.bulk_write(bulk_data, ordered=False)
            print(f'End of chunk {i}')
        else:
            chunk = lines[i*1000:]
            bulk_data = [InsertOne(json.loads(line)) for line in chunk]
            col.bulk_write(bulk_data, ordered=False)
            print(f'End of last chunk {i}')
            
    end = time.time()

    print(f'took {end-start} second to chunk')
    

   

            
            

    print("end of the ingestion")

except Exception as e:
    print(e)

