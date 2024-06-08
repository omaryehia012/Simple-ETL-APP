import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Function to connect to BigQuery
def connect_bigquery():
    return bigquery.Client()

# Function to load DataFrame to BigQuery in chunks
def load_df_to_bq_in_chunks(df, table_id, chunk_size, write_disposition="WRITE_APPEND"):
    client = connect_bigquery()
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    
    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        df_chunk = df.iloc[start:end]
        
        job = client.load_table_from_dataframe(df_chunk, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)
        st.write(f"Loaded chunk {i+1}/{num_chunks} with {len(df_chunk)} rows to {table_id}")

# Function to execute BigQuery command
def execute_bq_command(bq_command):
    client = connect_bigquery()
    client.query(bq_command)

# Function to connect to Oracle using SQLAlchemy
def connect_oracle(username, password, host, port, service_name):
    connection_string = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}"
    return create_engine(connection_string)

# Function to execute SQL query on Oracle
def execute_sql_query(query, engine):
    with engine.connect() as connection:
        return pd.read_sql(query, connection)

# Streamlit app layout
st.image("https://tse2.mm.bing.net/th?id=OIP._ayIasS428BL4Wh6-MXYYwHaCh&pid=Api&P=0&h=220", use_column_width=True)
st.title("ETL Tool")

# Google Cloud credentials
GOOGLE_APPLICATION_CREDENTIALS = r"C:\Users\oyehia\Tamer Innovate\tamer-group-bi.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# BigQuery configurations
PROJECT_ID = 'tamer-group-bi'
DATASET_ID = st.text_input("BigQuery Dataset ID")

# BigQuery table details
st.header("BigQuery Table Details")
table_name = st.text_input("BigQuery Table Name")
table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

# Write disposition options
write_disposition = st.selectbox("Write Disposition", ["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"])

# SQL query input
st.header("SQL Query")
sql_query = st.text_area("Enter SQL Query")

sql_file = st.file_uploader("Or Upload SQL File", type=["sql"])

# Chunk size input
chunk_size = st.number_input("Chunk Size", min_value=1, value=100000)

if st.button("Run ETL Process"):
    if sql_file:
        querystring = sql_file.read().decode("utf-8")
    else:
        querystring = sql_query

    try:
        
        # Connect to Oracle
        engine = connect_oracle(username="Mghoniemy", password="welcome12", host="10.1.1.200", port=1522, service_name="TMR")
        
        # Execute SQL query
        df_Q = execute_sql_query(querystring, engine)
        st.write(f"Retrieved {df_Q.shape[0]} rows")

        # Load DataFrame to BigQuery in chunks
        load_df_to_bq_in_chunks(df_Q, table_id, chunk_size, write_disposition=write_disposition)

    except SQLAlchemyError as e:
        st.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        st.error(f"An error occurred: {e}")

    st.write("ETL process completed.")
