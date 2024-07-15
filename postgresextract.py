import csv
import psycopg2
from datetime import datetime

# Database connection details
db_config = {
    'dbname': 'companydatabase',
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',
    'port': '5432'
}

# Output CSV file
output_csv_file = 'extracted_vehicles.csv'

# Record the start time of the job
job_start_time = datetime.now()
print(f"Job started at: {job_start_time}")

# Connect to the PostgreSQL database
conn_start_time = datetime.now()
conn = psycopg2.connect(**db_config)
cur = conn.cursor()
conn_end_time = datetime.now()
print(f"Connection established at: {conn_start_time}")
print(f"Connection established in: {conn_end_time - conn_start_time}")

# Query to select all data from the vehicles table
select_query = "SELECT * FROM vehicles;"

# Execute the query
query_start_time = datetime.now()
cur.execute(select_query)
rows = cur.fetchall()
query_end_time = datetime.now()
print(f"Query executed at: {query_start_time}")
print(f"Query executed in: {query_end_time - query_start_time}")

# Get column names from the cursor description
column_names = [desc[0] for desc in cur.description]

# Write data to CSV file
write_start_time = datetime.now()
with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(column_names)  # Write header
    writer.writerows(rows)  # Write data
write_end_time = datetime.now()
print(f"Data written to CSV at: {write_start_time}")
print(f"Data written in: {write_end_time - write_start_time}")

# Close the database connection
cur.close()
conn.close()

# Record the end time of the job
job_end_time = datetime.now()
print(f"Job ended at: {job_end_time}")

# Calculate the duration
job_duration = job_end_time - job_start_time
print(f"Job duration: {job_duration}")

# Calculate the total number of records read
record_count = len(rows)
print(f"Total number of records read: {record_count}")

# Output summary statistics
print("\nSummary Statistics:")
print(f"Job started at: {job_start_time}")
print(f"Connection established at: {conn_start_time}")
print(f"Connection established in: {conn_end_time - conn_start_time}")
print(f"Query executed at: {query_start_time}")
print(f"Query executed in: {query_end_time - query_start_time}")
print(f"Data written to CSV at: {write_start_time}")
print(f"Data written in: {write_end_time - write_start_time}")
print(f"Job ended at: {job_end_time}")
print(f"Job duration: {job_duration}")
print(f"Total number of records read: {record_count}")
