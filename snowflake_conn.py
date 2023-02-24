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
rows = run_query("SELECT * from BQ_TO_SF.BQ_TO_SF_MIG.MIGRATION_LOG;")

# Display results.
if rows is not None:
    results_placeholder.table(rows)
