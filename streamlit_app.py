# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
import pytz
import datetime as dt
from datetime import datetime
  
today = datetime.now()
    
month = today.strftime("%B")
year = today.year

# define the refresh interval in hours
refresh_interval_hours = 2

# get the current time
us_west_coast_tz = pytz.timezone('US/Pacific')
now = dt.datetime.now(us_west_coast_tz)


# calculate the next refresh time
next_refresh = now + dt.timedelta(hours=refresh_interval_hours - now.hour % refresh_interval_hours)

# calculate the time remaining until the next refresh
time_remaining = next_refresh - now

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

rows = run_query("select top 10 Agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS' and MONTH(CURRENT_DATE)=MONTH(DATE) group by Agents order by sum(sp_f) desc;")
df=pd.DataFrame(rows)
df.columns += 1
df.index = df.index + 1
df.columns = ["Agent Name", "Funded"]
df['Funded'] = df['Funded'].astype(int)

rows2 = run_query("select top 10 Agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='FDN' and MONTH(CURRENT_DATE)=MONTH(DATE) and sp_f <>0 group by Agents, Type order by sum(SP_F) desc;")
df2=pd.DataFrame(rows2)
df2.columns += 1
df2.index = df2.index + 1
df2.columns = ["Agent Name", "Funded"]
df2['Funded'] = df2['Funded'].astype(int)

rows3 = run_query("select sum(SP_F) from TEAMLEADS_MAR2023 where TYPE ='EFS' AND MONTH(CURRENT_DATE)=MONTH(DATE);")
df3=pd.DataFrame(rows3)
df3[0] = df3[0].astype(int)

rows4 = run_query("select sum(SP_F) from TEAMLEADS_MAR2023 where TYPE ='FDN' AND MONTH(CURRENT_DATE)=MONTH(DATE);")
df4=pd.DataFrame(rows4)
df4.columns = ["Total FDN Funded"]
df4['Total FDN Funded'] = df4['Total FDN Funded'].astype(int)

rows5 = run_query("select TEAM, AGENTS, sum(SP_F) from TEAMLEADS_MAR2023 where TYPE='EFS' group by TEAM, AGENTS order by 1;")
df5=pd.DataFrame(rows5)
df5.columns += 1
df5.index = df5.index + 1
df5.columns = ["Team", "Lead", "Funded"]
df5['Funded'] = df5['Funded'].astype(int)

rows6 = run_query("select TEAM, AGENTS, sum(SP_F) from TEAMLEADS_MAR2023 where TYPE='FDN' group by TEAM, AGENTS order by 1;")
df6=pd.DataFrame(rows6)
df6.columns += 1
df6.index = df6.index + 1
df6.columns = ["Team", "Lead", "Funded"]
df6['Funded'] = df6['Funded'].astype(int)

rows7 = run_query("select top 10 Agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='DECLINEFUNDED' and MONTH(CURRENT_DATE)=MONTH(DATE) group by Agents order by sum(sp_f) desc;")
df7=pd.DataFrame(rows7)
df7.columns += 1
df7.index = df7.index + 1
df7.columns = ["Agent Name", "Funded"]
df7['Funded'] = df7['Funded'].astype(int)

rows8 = run_query("select TEAM, AGENTS, sum(SP_F) from TEAMLEADS_MAR2023 where TYPE='DECLINEFUNDED' group by TEAM, AGENTS order by 1;")
df8=pd.DataFrame(rows8)
df8.columns += 1
df8.index = df8.index + 1
df8.columns = ["Team", "Lead", "Funded"]
df8['Funded'] = df8['Funded'].astype(int)

rows9 = run_query("select sum(SP_F) from TEAMLEADS_MAR2023 where TYPE='DECLINEFUNDED';")
df9=pd.DataFrame(rows9)
df9[0] = df9[0].astype(int)

html_str = f"""
<h1 style='text-align: center; color: white;'>{month} {year}</h1>
"""
st.markdown(html_str, unsafe_allow_html=True)

hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """

tab1, tab2, tab3 = st.tabs(["EFS", "Fundies", "CSR Declines"])

with tab1:
   
   col1, col2 = st.columns([4,4])

   with col1:
        st.subheader('Total EFS Funded')
        #df3['Dollar Amount']=df['Dollar Amount'].apply('{:,}'.format)
        st.metric("label3",df3[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(df5)

   with col2:
        st.subheader('Top EFS Agents')
        # Inject CSS with Markdown
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(df)
  
with tab2:
   col3, col4 = st.columns([4,4]) 

   with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", df4['Total FDN Funded'], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(df6)

   with col4:
        st.subheader('Top FDN Agents')
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(df2)

with tab3:
   col5, col6 = st.columns([4,4])  
  
   with col5:
          st.subheader('Total CSR Decline Funded')
          st.metric("label1",df9[0], label_visibility="collapsed")
          st.markdown(hide_table_row_index, unsafe_allow_html=True)
          st.table(df8)

   with col6:
          st.subheader('Top CSR Decline Agents')
          st.markdown(hide_table_row_index, unsafe_allow_html=True)
          st.table(df7)
          
st.caption('_Updates every 2 hours_')
st.write("Next data refresh at:", next_refresh.strftime("%I:%M %p"))
#st.write("Data will be refreshed in:", str(time_remaining))
#st.write(time_remaining)
st.write(dt.timedelta(hours=refresh_interval_hours - now.hour % refresh_interval_hours))
