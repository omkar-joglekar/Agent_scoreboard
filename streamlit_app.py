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

rows5 = run_query("select TEAM__GOOGLE_SHEETS, MARCH_AGENTS__GOOGLE_SHEETS, sum(SP_F__GOOGLE_SHEETS) from RAW_FUNNEL_SPRING_PROD_DB.FUNNEL__MCBJDTJ0LVHQIF12J1F.TEAMLEADS_MAR2023 where TYPE__GOOGLE_SHEETS='EFS' group by TEAM__GOOGLE_SHEETS, MARCH_AGENTS__GOOGLE_SHEETS order by 1;")
df5=pd.DataFrame(rows5)
df5.columns += 1
df5.index = df5.index + 1
df5.columns = ["Team", "Lead", "Funded"]
df5['Funded'] = df5['Funded'].astype(int)

rows6 = run_query("select TEAM__GOOGLE_SHEETS, MARCH_AGENTS__GOOGLE_SHEETS, sum(SP_F__GOOGLE_SHEETS) from RAW_FUNNEL_SPRING_PROD_DB.FUNNEL__MCBJDTJ0LVHQIF12J1F.TEAMLEADS_MAR2023 where TYPE__GOOGLE_SHEETS='FDN' group by TEAM__GOOGLE_SHEETS, MARCH_AGENTS__GOOGLE_SHEETS order by 1;")
df6=pd.DataFrame(rows6)
df6.columns += 1
df6.index = df6.index + 1
df6.columns = ["Team", "Lead", "Funded"]
df6['Funded'] = df6['Funded'].astype(int)

col1, col2 = st.columns([4,4])

with col1:
   st.subheader('Total EFS Funded')
   st.metric("", df3[0])
   st.dataframe(df5)

with col2:
   st.header('Top EFS Agents')
   st.dataframe(df)


col3, col4 = st.columns([4,4])

with col3:
   st.subheader('Total FDN Funded')
   st.metric("", df4[0])
   st.dataframe(df6)

with col4:
   st.header('Top FDN Agents')
   st.dataframe(df2)
