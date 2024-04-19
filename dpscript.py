#!/usr/bin/env python3

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.cluster0
# specify a collection
collection = db.pnxzwgz


# Listing Directory Contents
path = "data"
for (root, dirs, file) in os.walk(path):
    for f in file:
        print(f)


# Importing
# Loading or Opening the json file

with open('data/*.json') as file:
    file_data = json.load(file)


# Inserting the loaded data in the collection
# if JSON contains data more than one entry
# insert_many is used else insert_one is used
if isinstance(file_data, list):
    collection.insert_many(file_data)  
else:
    collection.insert_one(file_data)
