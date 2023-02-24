# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st
import snowflake.connector

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    try:
        conn = snowflake.connector.connect(
            **st.secrets["snowflake"], client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

conn = init_connection()

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

rows = run_query("select emp_id from `NTT.employee`")

# Print results.
st.write("Some wise words from Shakespeare:")
for row in rows:
    st.write(row['emp_id'])


# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        try:
            cur.execute(query)
            return cur.fetchall()
        except Exception as e:
            st.error(f"Error running query: {e}")
            return None

# Create placeholder for results.
results_placeholder = st.empty()

# Get results.
rows = run_query("SELECT * from BQ_TO_SF.BQ_TO_SF_MIG.MIGRATION_LOG LIMIT 10;")

# Display results.
if rows is not None:
    results_placeholder.table(rows)
