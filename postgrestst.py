import csv
import psycopg2

# Define input file and database connection details
input_csv_file = 'vehicles.csv'
db_config = {
    'dbname': 'companydatabase',
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',  # or your specific database host
    'port': '5432'        # or your specific database port
}

# Configuration for checkpoints
checkpoints = {
    'table_creation': True,
    'row_processing': True,
    'processing_interval': 50000  # Interval for row processing messages
}

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Define the table creation query (if not already created)
create_table_query = """
CREATE TABLE IF NOT EXISTS vehicles (
    id BIGINT PRIMARY KEY,
    url TEXT,
    region TEXT,
    price BIGINT,
    year INTEGER,
    manufacturer TEXT,
    model TEXT,
    condition TEXT,
    cylinders TEXT,
    fuel TEXT,
    odometer INTEGER,
    title_status TEXT,
    transmission TEXT,
    vin TEXT,
    drive TEXT,
    size TEXT,
    type TEXT,
    paint_color TEXT,
    county TEXT,
    state TEXT,
    lat FLOAT,
    long FLOAT
);
"""

# Create table if configured
if checkpoints['table_creation']:
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully.")

# Read the CSV data and insert into PostgreSQL table
row_count = 0
with open(input_csv_file, 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        row_count += 1

        # Define all required keys and set default value to None
        required_keys = [
            'id', 'url', 'region', 'price', 'year', 'manufacturer', 'model', 'condition',
            'cylinders', 'fuel', 'odometer', 'title_status', 'transmission', 'vin', 'drive',
            'size', 'type', 'paint_color', 'county', 'state', 'lat', 'long'
        ]

        # Clean and prepare row
        cleaned_row = {key: (row[key] if key in row and row[key] else None) for key in required_keys}

        try:
            # Check if the row with the same ID already exists
            check_query = "SELECT 1 FROM vehicles WHERE id = %s"
            cur.execute(check_query, (cleaned_row['id'],))
            exists = cur.fetchone()

            if exists:
                print(f"Row with ID {cleaned_row['id']} already exists. Skipping insertion.")
                continue

            # Insert the cleaned row into the PostgreSQL table
            insert_query = """
            INSERT INTO vehicles (
                id, url, region, price, year, manufacturer, model, condition,
                cylinders, fuel, odometer, title_status, transmission, vin,
                drive, size, type, paint_color, county, state, lat, long
            ) VALUES (%(id)s, %(url)s, %(region)s, %(price)s, %(year)s, %(manufacturer)s,
                      %(model)s, %(condition)s, %(cylinders)s, %(fuel)s, %(odometer)s,
                      %(title_status)s, %(transmission)s, %(vin)s, %(drive)s, %(size)s,
                      %(type)s, %(paint_color)s, %(county)s, %(state)s, %(lat)s, %(long)s);
            """
            cur.execute(insert_query, cleaned_row)
            conn.commit()

        except psycopg2.Error as e:
            print(f"Error inserting row with ID {cleaned_row['id']}: {e}")
            conn.rollback()  # Rollback the transaction to continue with the next row

        # Print progress checkpoint
        if checkpoints['row_processing'] and row_count % checkpoints['processing_interval'] == 0:
            print(f"Successfully processed {row_count} rows.")

# Close the database connection
cur.close()
conn.close()

print(f"Data has been inserted into the PostgreSQL table. Total rows processed: {row_count}")
