import pandas as pd
import json

def clean_json_string(json_str):
    # Load the JSON string into a Python dictionary
    json_dict = json.loads(json_str)
    # Dump it back to a JSON string with proper formatting
    clean_json_str = json.dumps(json_dict, ensure_ascii=False)
    return clean_json_str

def process_csv(csv_file_path, chunk_size=50000):
    chunk_number = 0
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, encoding='utf-8'):
        # Convert chunk to JSON format
        json_data = chunk.to_json(orient='records', lines=True)
        # Split the JSON data into individual JSON objects
        json_objects = json_data.splitlines()
        
        # Clean and write each chunk to a separate file
        with open(f'output_{chunk_number}.json', 'w', encoding='utf-8') as json_file:
            for json_obj in json_objects:
                clean_json_obj = clean_json_string(json_obj)
                json_file.write(clean_json_obj + '\n')
        
        chunk_number += 1

def main():
    # Specify the path to your CSV file
    csv_file_path = 'vehiclestest.csv'
    # Process the CSV file and split into multiple JSON files
    process_csv(csv_file_path)

if __name__ == "__main__":
    main()
