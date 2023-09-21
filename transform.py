from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch MongoDB credentials and database name from .env
username = os.environ.get('MONGO_USERNAME')
password = os.environ.get('MONGO_PASSWORD')
dbname = os.environ.get('MONGO_DBNAME')

# Load your data
with open('transformed_data.json') as f:
    data_dict = json.load(f)

# Flatten the data into a list of dictionaries
data = []
for game, terms in data_dict.items():
    for term in terms:
        term["game"] = game  # add game name to each term document
        data.append(term)

# Create MongoDB client connection
client = MongoClient(f"mongodb+srv://{username}:{password}@cluster0.eqfzrq7.mongodb.net/{dbname}?retryWrites=true&w=majority&tlsAllowInvalidCertificates=true")

# Access the database
db = client[dbname]

# Replace 'your_collection' with your actual collection name
collection = db['gdata']

# Insert the flattened data into the collection
collection.insert_many(data)
