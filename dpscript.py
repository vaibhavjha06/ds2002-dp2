#!/usr/bin/env python3

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.vaibhav
# specify a collection
collection = db.jha

# Listing Directory Contents
path = "data"
for (root, dirs, file) in os.walk(path):
    for f in file:
        print(f)


# Importing

directory = 'data/'
load_error=0
import_error=0
for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    with open(filepath) as file:
        try:
            json_data = json.load(file)
        except Exception as e:
            print(e, "error when loading", file)
            load_error+=1
            continue
    if isinstance(json_data, list):
        try:
            collection.insert_many(json_data)
        except Exception as e:
            print(e, "when importing into mongo")
            import_error+=1
            continue
    else:
        try:
            collection.insert_one(json_data)
        except Exception as e:
            print(e)
            import_error+=1
            continue


print("Number of load errors: ", load_error)
print("Number of import errors: ", import_error)