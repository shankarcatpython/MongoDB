import pandas as pd
from pymongo import MongoClient
import json

# Read CSV into pandas DataFrame
df = pd.read_csv("companies500.csv")

# Convert DataFrame to JSON
json_data = json.loads(df.to_json(orient='records'))

# Connect to MongoDB
client = MongoClient('localhost', 27017)  
db = client['stock']  
collection = db['companies'] 

# Insert JSON data into MongoDB
collection.insert_many(json_data)

# Close MongoDB connection
client.close()
