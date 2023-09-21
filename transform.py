from pymongo import MongoClient
import json

# load your data
with open('transformed_data.json') as f:
    data_dict = json.load(f)

# flatten the data into a list of dictionaries
data = []
for game, terms in data_dict.items():
    for term in terms:
        term["game"] = game  # add game name to each term document
        data.append(term)

# replace the placeholder data with your actual username, password, and dbname
username = "kcparks1234"
password = "TTs2JytYedkzoOpg"
dbname = "gdata"
client = MongoClient(f"mongodb+srv://{username}:{password}@cluster0.eqfzrq7.mongodb.net/{dbname}?retryWrites=true&w=majority&tlsAllowInvalidCertificates=true")

db = client[dbname]

# replace 'your_collection' with your actual collection name
collection = db['gdata']

# insert the flattened data into the collection
collection.insert_many(data)
