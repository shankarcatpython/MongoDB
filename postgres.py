import csv
import psycopg2

# Define input file and database connection details
input_csv_file = 'vehiclestest.csv'
db_config = {
    'dbname': 'companydatabase',
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',  # or your specific database host
    'port': '5432'        # or your specific database port
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
    price INTEGER,
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
cur.execute(create_table_query)
conn.commit()

# Read the CSV data and insert into PostgreSQL table
with open(input_csv_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Remove unwanted columns and replace empty fields with 'None'
        cleaned_row = {key: (value if value else None) for key, value in row.items() if key not in ['region_url', 'image_url', 'description', 'posting_date']}
        
        # Insert the cleaned row into the PostgreSQL table
        insert_query = """
        INSERT INTO vehicles (
            id, url, region, price, year, manufacturer, model, condition,
            cylinders, fuel, odometer, title_status, transmission, vin,
            drive, size, type, paint_color, county, state, lat, long
        ) VALUES (%(id)s, %(url)s, %(region)s, %(price)s, %(year)s, %(manufacturer)s,
                  %(model)s, %(condition)s, %(cylinders)s, %(fuel)s, %(odometer)s,
                  %(title_status)s, %(transmission)s, %(vin)s, %(drive)s, %(size)s,
                  %(type)s, %(paint_color)s, %(county)s, %(state)s, %(lat)s, %(long)s)
        ON CONFLICT (id) DO NOTHING;
        """
        cur.execute(insert_query, cleaned_row)
        conn.commit()

# Close the database connection
cur.close()
conn.close()

print("Data has been inserted into the PostgreSQL table.")
