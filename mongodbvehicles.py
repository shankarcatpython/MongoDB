import pandas as pd
from pymongo import MongoClient

# Load the CSV file into a DataFrame
file_path = 'vehicles.csv'
vehicles_df = pd.read_csv(file_path)

# Create a connection to MongoDB
client = MongoClient('localhost', 27017)  # Adjust the connection details as necessary
db = client['vehicle_database']
collection = db['vehicles']

# Convert the DataFrame to a dictionary and insert into MongoDB
vehicles_data = vehicles_df.to_dict(orient='records')
collection.insert_many(vehicles_data)

print("Data has been successfully inserted into the MongoDB database.")
