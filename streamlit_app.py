# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_resource(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

rows = run_query("select top 10 march_agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS' group by march_agents, type order by sum(sp_f) desc;")
df=pd.DataFrame(rows)
df.columns += 1
df.index = df.index + 1
df.columns = ["Agent Name", "Funded"]
df['Funded'] = df['Funded'].astype(int)


rows2 = run_query("select top 10 march_agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='FDN' and sp_f <>0 group by march_agents, type order by sum(sp_f) desc;")
df2=pd.DataFrame(rows2)
df2.columns += 1
df2.index = df2.index + 1
df2.columns = ["Agent Name", "Funded"]
df2['Funded'] = df2['Funded'].astype(int)

rows3 = run_query("select sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS';")
df3=pd.DataFrame(rows3)
#df3[0] = df3[0].astype(int)
df3[0] = df3[0].replace('',',').astype(int)

rows4 = run_query("select sum(sp_f) from SCOREBOARD_MAR2023 where type='FDN';")
df4=pd.DataFrame(rows4)
df4[0] = df4[0].astype(int)

col1, col2 = st.columns([1,2])

with col1:
   st.subheader('Total EFS Funded')
   st.metric("", df3[0])
   st.text('Team 1:')
   st.text('Team 2:')
   st.text('Team 3:')

with col2:
   st.header('Top EFS Agents')
   st.dataframe(df)


col3, col4 = st.columns([1,2])

with col3:
   st.subheader('Total FDN Funded')
   st.metric("", df4[0])
   st.text('Team 1:')
   st.text('Team 2:')
   st.text('Team 3:')

with col4:
   st.header('Top FDN Agents')
   st.dataframe(df2)
