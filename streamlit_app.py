# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
import pytz
import datetime as dt
import time
import threading
import uuid
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

rows = run_query("select top 10 Agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='EFS' and MONTH(CURRENT_DATE)=MONTH(DATE) group by Agents order by sum(sp_f) desc;")
df=pd.DataFrame(rows)
df.columns += 1
df.index = df.index + 1
df.insert(0, "Rank", df.index)
df.columns = ["Rank","Agent Name", "Funded"]
df['Funded'] = df['Funded'].astype(int)

rows2 = run_query("select top 10 Agents, sum(sp_f) from SCOREBOARD_MAR2023 where type='FDN' and MONTH(CURRENT_DATE)=MONTH(DATE) and sp_f <>0 group by Agents, Type order by sum(SP_F) desc;")
df2=pd.DataFrame(rows2)
df2.columns += 1
df2.index = df2.index + 1
df2.insert(0, "Rank", df2.index)
df2.columns = ["Rank","Agent Name", "Funded"]
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
df7.insert(0, "Rank", df7.index)
df7.columns = ["Rank","Agent Name", "Funded"]
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
        st.metric("label3",df3[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(df5)

   with col2:
        st.subheader('Top EFS Agents')
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
          st.table(df7)
          
#st.write(f"Time left until next refresh: {hours_left} hour{'s' if hours_left != 1 else ''}, {minutes_left} minute{'s' if minutes_left != 1 else ''}")
#st.write(f"Next refresh in {hours} hours {minutes} minutes ({next_refresh_time} {timezone.zone})")

def countdown_timer():
    target_times = ["08:00:00", "10:00:00", "12:00:00", "14:00:00", "16:00:00", "18:00:00", "20:00:00", "22:00:00", "00:00:00", "02:00:00", "04:00:00", "06:00:00", "08:00:00"]
    timezone = "US/Pacific"

    next_refresh_time = None
    for i, target_time in enumerate(target_times):
        target_datetime = dt.datetime.strptime(target_time, '%H:%M:%S').time()
        target_datetime = dt.datetime.combine(dt.date.today(), target_datetime)
        if i == 0:
            if dt.datetime.now(dt.timezone(timezone)) > target_datetime:
                target_datetime += dt.timedelta(days=1)
        else:
            if next_refresh_time < dt.datetime.now(dt.timezone(timezone)) <= target_datetime:
                pass
            elif dt.datetime.now(dt.timezone(timezone)) > target_datetime:
                target_datetime += dt.timedelta(days=1)
        if next_refresh_time is None or target_datetime < next_refresh_time:
            next_refresh_time = target_datetime

    while True:
        remaining_time = next_refresh_time - dt.datetime.now(dt.timezone(timezone))
        remaining_seconds = remaining_time.total_seconds()

        if remaining_seconds < 0:
            # Update next refresh time for the next cycle
            for i, target_time in enumerate(target_times):
                target_datetime = dt.datetime.strptime(target_time, '%H:%M:%S').time()
                target_datetime = dt.datetime.combine(dt.date.today() + dt.timedelta(days=1), target_datetime)
                if next_refresh_time < target_datetime:
                    next_refresh_time = target_datetime
                    break
            remaining_seconds = (next_refresh_time - dt.datetime.now(dt.timezone(timezone))).total_seconds()

        hours, remaining_seconds = divmod(remaining_seconds, 3600)
        minutes, remaining_seconds = divmod(remaining_seconds, 60)
        seconds = int(remaining_seconds)

        countdown_str = f'Next refresh in {int(hours):02d}:{int(minutes):02d}:{seconds:02d} ({timezone})'

        # Update session state variable to keep track of the countdown timer
        st.session_state.countdown_timer = countdown_str

        # Update the displayed timer only when the timer changes
        if st.session_state.countdown_timer != countdown_str:
            st.write(countdown_str)

        # Pause the loop for 1 second to allow other events to be processed by Streamlit
        st.experimental_sleep(1)


if 'countdown_timer' not in st.session_state:
    # Start the countdown timer thread only once
    st.session_state.countdown_timer = ''
    thread = st.thread(run_countdown_timer)
else:
    # Update the displayed timer on every streamlit iteration
    st.write(st.session_state.countdown_timer)

if st.button('Stop countdown timer'):
    # Stop the countdown timer thread
    st.session_state.stop_countdown_timer = True
  
    
