
import   time
import   re
import   logging

import   settings

## ------------------------------------------------------------------
## ------------------------------------------------------------------
## ------------------------------------------------------------------
## ------------------------------------------------------------------
## ------------------------------------------------------------------
## ------------------------------------------------------------------
'''
pip install pandas sqlalchemy pyodbc

import pandas as pd
from sqlalchemy import create_engine

# Sample DataFrame
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
}
df = pd.DataFrame(data)

# MSSQL Connection Details
server = 'your_server_name'  # e.g., 'localhost' or '127.0.0.1'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'

# Create SQLAlchemy engine for MSSQL
engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# Define table name
table_name = "your_table_name"

# Write DataFrame to MSSQL table
try:
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Table '{table_name}' created successfully and data inserted.")
except Exception as e:
    print(f"An error occurred: {e}")


df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=1000)

'''



## ------------------------------------------------------------------
## ------------------------------------------------------------------
