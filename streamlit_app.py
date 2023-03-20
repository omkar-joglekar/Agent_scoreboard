# streamlit_app.py

import streamlit as st
import snowflake.connector

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.experimental_singleton(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

rows = run_query("select top 10 march_agents, type, sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS' group by march_agents, type order by sum(sp_f) desc;")

# Print results.
st.header('Top EFS agents')

for row in rows:
    st.write(f"{row[0]}, Funded {row[2]} EFS loans")