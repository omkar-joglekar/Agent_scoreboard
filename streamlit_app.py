# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
  
today = datetime.now()
    
month = today.strftime("%B")
year = today.year

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

rows = run_query("select top 10 march_agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS' and MONTH(CURRENT_DATE)=MONTH(DATE__GOOGLE_SHEETS) group by march_agents order by sum(sp_f) desc;")
df=pd.DataFrame(rows)
df.columns += 1
df.index = df.index + 1
df.columns = ["Agent Name", "Funded"]
df['Funded'] = df['Funded'].astype(int)

rows2 = run_query("select top 10 march_agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='FDN' and MONTH(CURRENT_DATE)=MONTH(DATE__GOOGLE_SHEETS) and sp_f <>0 group by march_agents, type order by sum(sp_f) desc;")
df2=pd.DataFrame(rows2)
df2.columns += 1
df2.index = df2.index + 1
df2.columns = ["Agent Name", "Funded"]
df2['Funded'] = df2['Funded'].astype(int)

rows3 = run_query("select sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS' and MONTH(CURRENT_DATE)=MONTH(DATE__GOOGLE_SHEETS);")
df3=pd.DataFrame(rows3)
df3[0] = df3[0].astype(int)

rows4 = run_query("select sum(sp_f) from SCOREBOARD_MAR2023 where type='FDN' and MONTH(CURRENT_DATE)=MONTH(DATE__GOOGLE_SHEETS);")
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

rows7 = ("select top 10 march_agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='DECLINEFUNDED' and MONTH(CURRENT_DATE)=MONTH(DATE__GOOGLE_SHEETS) group by march_agents order by sum(sp_f) desc;")
df7=pd.DataFrame(rows)
df7.columns += 1
df7.index = df7.index + 1
df7.columns = ["Agent Name", "Funded"]
df7['Funded'] = df7['Funded'].astype(int)

rows8 = run_query("select TEAM__GOOGLE_SHEETS, MARCH_AGENTS__GOOGLE_SHEETS, sum(SP_F__GOOGLE_SHEETS) from RAW_FUNNEL_SPRING_PROD_DB.FUNNEL__MCBJDTJ0LVHQIF12J1F.TEAMLEADS_MAR2023 where TYPE__GOOGLE_SHEETS='DECLINEFUNDED' group by TEAM__GOOGLE_SHEETS, MARCH_AGENTS__GOOGLE_SHEETS order by 1;")
df8=pd.DataFrame(rows8)
df8.columns += 1
df8.index = df8.index + 1
df8.columns = ["Team", "Lead", "Funded"]
df8['Funded'] = df8['Funded'].astype(int)

rows9 = run_query("select sum(sp_f) from SCOREBOARD_MAR2023 where type='DECLINEFUNDED' and MONTH(CURRENT_DATE)=MONTH(DATE__GOOGLE_SHEETS);")
df9=pd.DataFrame(rows9)
df9[0] = df9[0].astype(int)

html_str = f"""
<h1 style='text-align: center; color: white;'>{month} {year}</h1>
"""

#st.markdown("<h1 style='text-align: center; color: white;'>March 2023</h1>", unsafe_allow_html=True)
st.markdown(html_str, unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns([4,6,4,6])

with col1:
   #st.subheader('Total EFS Funded')
   st.metric("Total EFS Funded", df3[0])
   st.dataframe(df5)

with col2:
   st.header('Top EFS Agents')
   st.dataframe(df)


with col3:
   #st.subheader('Total FDN Funded')
   st.metric("Total FDN Funded", df4[0])
   st.dataframe(df6)

with col4:
   st.header('Top FDN Agents')
   st.dataframe(df2)

col5, col6 = st.columns([4,4])  
  
with col5:
   #st.subheader('Total FDN Funded')
   st.metric("Total Decline Funded", df9[0])
   st.dataframe(df8)

with col6:
   st.header('Top Decline Agents')
   st.dataframe(df7)
