# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd

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

df=pd.DataFrame(rows)
df.columns += 1
df.index = df.index + 1
df.columns = ["Agent Name", "Type", "Funded"]
df['Funded'] = df['Funded'].astype(int)

# Print results.
st.header('Top EFS agents')
st.dataframe(df)
