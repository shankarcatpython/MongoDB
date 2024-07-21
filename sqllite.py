import pandas as pd
import sqlite3

# Load the CSV file into a DataFrame
file_path = 'vehicles.csv'
vehicles_df = pd.read_csv(file_path)

# Create a connection to a SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('vehicles.db')
cursor = conn.cursor()

# Create a table for the vehicles data
create_table_query = '''
CREATE TABLE IF NOT EXISTS vehicles (
    id INTEGER PRIMARY KEY,
    url TEXT,
    region TEXT,
    region_url TEXT,
    price REAL,
    year INTEGER,
    manufacturer TEXT,
    model TEXT,
    condition TEXT,
    cylinders TEXT,
    fuel TEXT,
    odometer REAL,
    title_status TEXT,
    transmission TEXT,
    vin TEXT,
    drive TEXT,
    size TEXT,
    type TEXT,
    paint_color TEXT,
    image_url TEXT,
    description TEXT,
    county TEXT,
    state TEXT,
    lat REAL,
    long REAL,
    posting_date TEXT
)
'''
cursor.execute(create_table_query)
conn.commit()

# Insert the data from the DataFrame into the SQLite table
vehicles_df.to_sql('vehicles', conn, if_exists='replace', index=False)

# Close the connection
conn.close()

print("Data has been successfully inserted into the SQLite database.")