import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import sqlite3
import json
from datetime import datetime

# Replace with your actual SQLite database path
DATABASE_PATH = 'vehicles.db'

# Connect to the database
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# Create the data_meta table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS data_meta (
    table_name    TEXT,
    analysis_date TEXT,
    feature_name  TEXT,
    mean          REAL,
    median        REAL,
    std_dev       REAL,
    min_value     REAL,
    max_value     REAL,
    anomaly_count INTEGER,
    anomalies     TEXT
);
"""
cursor.execute(create_table_query)
conn.commit()

# Load the vehicles data
vehicles_df = pd.read_sql_query('SELECT * FROM vehicles', conn)

# Initialize a list to hold the meta data
meta_data = []

# Features to analyze
features = ['price', 'year', 'odometer']

# Current date for analysis_date
analysis_date = datetime.now().strftime('%Y-%m-%d')

# Compute statistics and detect anomalies for each feature
for feature in features:
    if feature in vehicles_df.columns:
        data = vehicles_df[feature].dropna().values.reshape(-1, 1)
        data_df = vehicles_df[[feature, 'id']].dropna().reset_index(drop=True)
        
        # Compute statistics
        mean = np.mean(data)
        median = np.median(data)
        std_dev = np.std(data)
        min_value = np.min(data)
        max_value = np.max(data)
        
        # Detect anomalies
        clf = IsolationForest(contamination=0.1, random_state=42)
        clf.fit(data)
        predictions = clf.predict(data)
        anomaly_count = np.sum(predictions == -1)
        
        # Collect anomaly information
        anomalies = data_df.loc[predictions == -1, ['id', feature]].to_dict(orient='records')
        
        # Append the computed values to the meta_data list
        meta_data.append((
            'vehicles',
            analysis_date,
            feature,
            mean,
            median,
            std_dev,
            min_value,
            max_value,
            anomaly_count,
            json.dumps(anomalies)
        ))

# Insert the meta data into the data_meta table
insert_query = """
INSERT INTO data_meta (table_name, analysis_date, feature_name, mean, median, std_dev, min_value, max_value, anomaly_count, anomalies)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
cursor.executemany(insert_query, meta_data)
conn.commit()

# Print the meta data
meta_df = pd.DataFrame(meta_data, columns=['table_name', 'analysis_date', 'feature_name', 'mean', 'median', 'std_dev', 'min_value', 'max_value', 'anomaly_count', 'anomalies'])
print(meta_df)

# Close the connection
conn.close()
