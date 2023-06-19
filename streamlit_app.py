# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
import pytz
import datetime as dt
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# update every 5 mins
st_autorefresh(interval=5 * 60 * 1000, key="datarefresh")

#Get Month and Year for app title
today = datetime.now()
month = today.strftime("%B")
year = today.year

#Calculate next refresh time
refresh_times = ["08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00", "00:00", "02:00", "04:00", "06:00"]
timezone = pytz.timezone('US/Pacific')

current_time = dt.datetime.now(timezone).strftime("%H:%M")

next_refresh_time = None
for time in refresh_times:
    if current_time < time:
        next_refresh_time = time
        break
        
if next_refresh_time is None:
    next_refresh_time = refresh_times[0] # if all refresh times have passed, the next refresh time will be the first one in the list

delta = dt.datetime.strptime(next_refresh_time, "%H:%M") - dt.datetime.strptime(current_time, "%H:%M")

hours = delta.seconds // 3600
minutes = (delta.seconds // 60) % 60


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
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
    
#Queries
rows = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 1' group by Agent, Date;")
df=pd.DataFrame(rows)
df.columns += 1
#df.index = df.index + 1
#df.insert(0, "Rank", df.index)
df.columns = ["Rank","Agent Name", "Funded", "Date"]
df['Funded'] = df['Funded'].astype(int)

rows2 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), Date from SCOREBOARD_MAR2023 where type='FDN' group by Agent, Date;")
df2=pd.DataFrame(rows2)
df2.columns += 1
#df2.index = df2.index + 1
#df2.insert(0, "Rank", df2.index)
df2.columns = ["Rank","Agent Name", "Funded", "Date"]
df2['Funded'] = df2['Funded'].astype(int)

rows3 = run_query("select sum(SP_F),Date from TEAMLEADS_MAR2023 where TYPE ='EFS' group by Date;")
df3=pd.DataFrame(rows3)
df3.columns = ["Total_EFS", "Date"]
df3['Total_EFS'] = df3['Total_EFS'].apply(lambda x: '{:,.0f}'.format(x))

rows4 = run_query("select sum(SP_F), Date from TEAMLEADS_MAR2023 where TYPE ='FDN' group by Date;")
df4=pd.DataFrame(rows4)
df4.columns = ["Total_FDN", "Date"]
df4['Total_FDN'] = df4['Total_FDN'].apply(lambda x: '{:,.0f}'.format(x))

rows5 = run_query("select TEAM, AGENT, sum(SP_F), Date from TEAMLEADS_MAR2023 where TYPE='EFS' group by TEAM, AGENT, Date order by 1;")
df5=pd.DataFrame(rows5)
df5.columns += 1
df5.index = df5.index + 1
df5.columns = ["Team", "Lead", "Funded", "Date"]
df5['Funded'] = df5['Funded'].astype(int)
distinct_team_EFS = df5['Team'].unique().tolist()


rows6 = run_query("select TEAM, AGENT, sum(SP_F), Date from TEAMLEADS_MAR2023 where TYPE='FDN' group by TEAM, AGENT, Date order by 1;")
df6=pd.DataFrame(rows6)
df6.columns += 1
df6.index = df6.index + 1
df6.columns = ["Team", "Lead", "Funded", "Date"]
df6['Funded'] = df6['Funded'].astype(int)
distinct_team_FDN = df6['Team'].unique().tolist()

rows7 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), Date from SCOREBOARD_MAR2023 where type='DECLINEFUNDED' group by Agent, Date;")
df7=pd.DataFrame(rows7)
df7.columns += 1
#df7.index = df7.index + 1
#df7.insert(0, "Rank", df7.index)
df7.columns = ["Rank","Agent Name", "Funded", "Date"]
df7['Funded'] = df7['Funded'].astype(int)

rows8 = run_query("select TEAM, AGENT, sum(SP_F), Date from TEAMLEADS_MAR2023 where TYPE='DECLINEFUNDED' group by TEAM, AGENT, Date order by 1;")
df8=pd.DataFrame(rows8)
df8.columns += 1
df8.index = df8.index + 1
df8.columns = ["Team", "Lead", "Funded", "Date"]
df8['Funded'] = df8['Funded'].astype(int)

rows9 = run_query("select sum(SP_F), Date from TEAMLEADS_MAR2023 where TYPE='DECLINEFUNDED' group by Date;")
df9=pd.DataFrame(rows9)
df9.columns = ["Total_DF", "Date"]
df9['Total_DF'] = df9['Total_DF'].apply(lambda x: '{:,.0f}'.format(x))

rows10 = run_query("select count(distinct id), TO_VARCHAR(to_date(closedate), 'YYYY-MM') AS DATE from RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.OPPORTUNITY where year(closedate)=year(current_Date) and loan_provider__c='Progressa' and stagename='Funded' group by DATE;")
df10 = pd.DataFrame(rows10)
df10.columns = ["Prog_funded", "Date"]
df10['Prog_funded'] = df10['Prog_funded'].astype(int)

rows11 = run_query("select count(distinct id), TO_VARCHAR(to_date(closedate), 'YYYY-MM') AS DATE from RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.OPPORTUNITY where year(closedate)=year(current_Date) and loan_provider__c='Lendful' and stagename='Funded' group by DATE;")
df11 = pd.DataFrame(rows11)
df11.columns = ["Lend_funded", "Date"]
df11['Lend_funded'] = df11['Lend_funded'].astype(int)

rows12 = run_query("select count(distinct id), TO_VARCHAR(to_date(closedate), 'YYYY-MM') AS DATE from RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.OPPORTUNITY where year(closedate)=year(current_Date) and loan_provider__c='Consumer Capital' and stagename='Funded' and lower(name) not like '%spring grad%' and lower(name) not like '%foundation grad%' group by DATE")
df12 = pd.DataFrame(rows12)
df12.columns = ["ccc_funded", "Date"]
df12['ccc_funded'] = df12['ccc_funded'].astype(int)

rows13 = run_query("select count(distinct id), TO_VARCHAR(to_date(funding_date_new__c), 'YYYY-MM') AS DATE from RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.OPPORTUNITY where year(funding_date_new__c)=year(current_date) and loan_provider__c='Spring Financial' and test_record__c=FALSE and recordtypename__c='Personal Loan' and stagename='Funded' group by DATE")
df13 = pd.DataFrame(rows13)
df13.columns = ["evergreen_funded", "Date"]
df13['evergreen_funded'] = df13['evergreen_funded'].astype(int)

rows14 = run_query("select TO_VARCHAR(to_date(closedate), 'YYYY-MM') AS DATE, DENSE_RANK() OVER (PARTITION BY DATE ORDER BY count(distinct a.id) desc) AS RANK, b.name, count(distinct a.id) from RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.OPPORTUNITY a left join RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.USER b on a.agent_name_opp__c=b.id where year(closedate)=year(current_date) and loan_provider__c='Consumer Capital' and recordtypename__c='Personal Loan' and stagename='Funded' and b.name is not null group by b.name, loan_provider__c, DATE;")
df14 = pd.DataFrame(rows14)
df14.columns += 1
#df14.index = df14.index + 1
#df14.insert(0, "Rank", df14.index)
df14.columns = ["Date","Rank","Agent Name", "Funded"]
df14['Funded']=df14['Funded'].astype(int)

rows15 = run_query("select TO_VARCHAR(to_date(funding_date_new__c), 'YYYY-MM') AS DATE, DENSE_RANK() OVER (PARTITION BY DATE ORDER BY count(distinct a.id) desc) AS RANK,b.name, count(distinct a.id) from RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.OPPORTUNITY a left join RAW_SALESFORCE_PROD_DB.SALESFORCE_PROD_SCHEMA.USER b on a.agent_name_opp__c=b.id where year(funding_date_new__c)=year(current_date) and funding_date_new__c is not null and loan_provider__c='Spring Financial' and recordtypename__c='Personal Loan' and stagename='Funded' and b.name is not null  group by b.name, DATE;")
df15 = pd.DataFrame(rows15)
df15.columns += 1
#df15.index = df15.index + 1
#df15.insert(0, "Rank", df15.index)
df15.columns = ["Date","Rank","Agent Name", "Funded"]
df15['Funded']=df15['Funded'].astype(int)

rows16 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 2' group by Agent, Date;")
df16=pd.DataFrame(rows16)
df16.columns += 1
df16.columns = ["Rank","Agent Name", "Funded", "Date"]
df16['Funded'] = df16['Funded'].astype(int)

rows17 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 3' group by Agent, Date;")
df17=pd.DataFrame(rows17)
df17.columns += 1
df17.columns = ["Rank","Agent Name", "Funded", "Date"]
df17['Funded'] = df17['Funded'].astype(int)

rows18 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 4' group by Agent, Date;")
df18=pd.DataFrame(rows18)
df18.columns += 1
df18.columns = ["Rank","Agent Name", "Funded", "Date"]
df18['Funded'] = df18['Funded'].astype(int)

rows19 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 5' group by Agent, Date;")
df19=pd.DataFrame(rows19)
df19.columns += 1
df19.columns = ["Rank","Agent Name", "Funded", "Date"]
df19['Funded'] = df19['Funded'].astype(int)

rows20 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' group by Agent, Date;")
df20=pd.DataFrame(rows20)
df20.columns += 1
df20.columns = ["Rank","Agent Name", "Funded", "Date"]
df20['Funded'] = df20['Funded'].astype(int)

rows21 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 1' group by Agent, Date;")
df21=pd.DataFrame(rows21)
df21.columns += 1
df21.columns = ["Rank","Agent Name", "Funded", "Date"]
df21['Funded'] = df21['Funded'].astype(int)

rows22 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 2' group by Agent, Date;")
df22=pd.DataFrame(rows22)
df22.columns += 1
df22.columns = ["Rank","Agent Name", "Funded", "Date"]
df22['Funded'] = df22['Funded'].astype(int)

rows23 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 3' group by Agent, Date;")
df23=pd.DataFrame(rows23)
df23.columns += 1
df23.columns = ["Rank","Agent Name", "Funded", "Date"]
df23['Funded'] = df23['Funded'].astype(int)

rows24 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 4' group by Agent, Date;")
df24=pd.DataFrame(rows24)
df24.columns += 1
df24.columns = ["Rank","Agent Name", "Funded", "Date"]
df24['Funded'] = df24['Funded'].astype(int)

rows25 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 5' group by Agent, Date;")
df25=pd.DataFrame(rows25)
df25.columns += 1
df25.columns = ["Rank","Agent Name", "Funded", "Date"]
df25['Funded'] = df25['Funded'].astype(int)

rows26 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='DECLINEFUNDED' and Team='Team 1 FDN' group by Agent, Date;")
df26=pd.DataFrame(rows26)
df26.columns += 1
df26.columns = ["Rank","Agent Name", "Funded", "Date"]
df26['Funded'] = df26['Funded'].astype(int)

rows27 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='DECLINEFUNDED' and Team='Team 2 FDN' group by Agent, Date;")
df27=pd.DataFrame(rows27)
df27.columns += 1
df27.columns = ["Rank","Agent Name", "Funded", "Date"]
df27['Funded'] = df27['Funded'].astype(int)

rows28 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 6' group by Agent, Date;")
df28=pd.DataFrame(rows28)
df28.columns += 1
df28.columns = ["Rank","Agent Name", "Funded", "Date"]
df28['Funded'] = df28['Funded'].astype(int)

rows29 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 7' group by Agent, Date;")
df29=pd.DataFrame(rows29)
df29.columns += 1
df29.columns = ["Rank","Agent Name", "Funded", "Date"]
df29['Funded'] = df29['Funded'].astype(int)

rows30 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='EFS' and Team='Team 8' group by Agent, Date;")
df30=pd.DataFrame(rows30)
df30.columns += 1
df30.columns = ["Rank","Agent Name", "Funded", "Date"]
df30['Funded'] = df30['Funded'].astype(int)

rows31 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 6' group by Agent, Date;")
df31=pd.DataFrame(rows31)
df31.columns += 1
df31.columns = ["Rank","Agent Name", "Funded", "Date"]
df31['Funded'] = df31['Funded'].astype(int)

rows32 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 7' group by Agent, Date;")
df32=pd.DataFrame(rows32)
df32.columns += 1
df32.columns = ["Rank","Agent Name", "Funded", "Date"]
df32['Funded'] = df32['Funded'].astype(int)

rows33 = run_query("select DENSE_RANK() OVER (PARTITION BY DATE ORDER BY sum(SP_F) DESC) AS RANK, Agent, sum(sp_f), DATE from SCOREBOARD_MAR2023 where type='FDN' and Team='Team 8' group by Agent, Date;")
df33=pd.DataFrame(rows33)
df33.columns += 1
df33.columns = ["Rank","Agent Name", "Funded", "Date"]
df33['Funded'] = df33['Funded'].astype(int)

#markdown
hide_streamlit_style = """
            <style>
            MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
           </style>
            """

df['Date'] =  pd.to_datetime(df['Date'])
df2['Date'] = pd.to_datetime(df2['Date'])
df3['Date'] = pd.to_datetime(df3['Date'])
df4['Date'] = pd.to_datetime(df4['Date'])
df5['Date'] = pd.to_datetime(df5['Date'])
df6['Date'] = pd.to_datetime(df6['Date'])
df7['Date'] = pd.to_datetime(df7['Date'])
df8['Date'] = pd.to_datetime(df8['Date'])
df9['Date'] = pd.to_datetime(df9['Date'])
df10['Date'] = pd.to_datetime(df10['Date'])
df11['Date'] = pd.to_datetime(df11['Date'])
df12['Date'] = pd.to_datetime(df12['Date'])
df13['Date'] = pd.to_datetime(df13['Date'])
df14['Date'] = pd.to_datetime(df14['Date'])
df15['Date'] = pd.to_datetime(df15['Date'])
df16['Date'] = pd.to_datetime(df16['Date'])
df17['Date'] = pd.to_datetime(df17['Date'])
df18['Date'] = pd.to_datetime(df18['Date'])
df19['Date'] = pd.to_datetime(df19['Date'])
df20['Date'] = pd.to_datetime(df20['Date'])
df21['Date'] = pd.to_datetime(df21['Date'])
df22['Date'] = pd.to_datetime(df22['Date'])
df23['Date'] = pd.to_datetime(df23['Date'])
df24['Date'] = pd.to_datetime(df24['Date'])
df25['Date'] = pd.to_datetime(df25['Date'])
df26['Date'] = pd.to_datetime(df26['Date'])
df27['Date'] = pd.to_datetime(df27['Date'])
df28['Date'] = pd.to_datetime(df28['Date'])
df29['Date'] = pd.to_datetime(df29['Date'])
df30['Date'] = pd.to_datetime(df30['Date'])
df31['Date'] = pd.to_datetime(df31['Date'])
df32['Date'] = pd.to_datetime(df32['Date'])
df33['Date'] = pd.to_datetime(df33['Date'])
month_filter = st.sidebar.radio(
    'Month:',
    pd.to_datetime(pd.concat([df['Date'], df2['Date']])).dt.strftime('%B %Y').unique()
)

selected_month = pd.to_datetime(month_filter).strftime("%B")
selected_year = pd.to_datetime(month_filter).year

# HTML string for the title
html_str = f"""
<h1 style='text-align: center; color: white;'>{selected_month} {selected_year}</h1>
"""
st.markdown(html_str, unsafe_allow_html=True)

filtered_df_1 = df[df['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_2 = df2[df2['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_3 = df3[df3['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_4 = df4[df4['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_5 = df5[df5['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_6 = df6[df6['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_7 = df7[df7['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_8 = df8[df8['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_9 = df9[df9['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_10 = df10[df10['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_11 = df11[df11['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_12 = df12[df12['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_13 = df13[df13['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_14 = df14[df14['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_15 = df15[df15['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_16 = df16[df16['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_17 = df17[df17['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_18 = df18[df18['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_19 = df19[df19['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_20 = df20[df20['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_21 = df21[df21['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_22 = df22[df22['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_23 = df23[df23['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_24 = df24[df24['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_25 = df25[df25['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_26 = df26[df26['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_27 = df27[df27['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_28 = df28[df28['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_29 = df29[df29['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_30 = df30[df30['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_31 = df31[df31['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_32 = df32[df32['Date'].dt.strftime('%B %Y') == month_filter]
filtered_df_33 = df33[df33['Date'].dt.strftime('%B %Y') == month_filter]
options = ["EFS", "Fundies", "CSR Declines", "Progressa & Lendful Funded","CCC & Evergreen Funded"]
selected_option = st.selectbox("Select:", options) #label_visibility="collapsed"


if selected_option == "EFS":
   
   radio = st.radio("Team:",(*distinct_team_EFS, 'All Teams') ,horizontal=True) 
   col1, col2 = st.columns([4,4])
   if radio == 'Team 1':
       
       with col1:
           st.subheader('Total EFS Funded')
           st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
           st.markdown(hide_table_row_index, unsafe_allow_html=True)
           st.table(filtered_df_5[["Team", "Lead", "Funded"]])

       with col2:
           st.subheader('Top Team 1 Agents')
           st.table(filtered_df_1[["Rank","Agent Name", "Funded"]].head(10))
        
   elif radio == 'Team 2':
       with col1:
            st.subheader('Total EFS Funded')
            st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            st.table(filtered_df_5[["Team", "Lead", "Funded"]])

       with col2:
            st.subheader('Top Team 2 Agents')
            st.table(filtered_df_16[["Rank","Agent Name", "Funded"]].head(10))
   elif radio == 'Team 3':
       with col1:
            st.subheader('Total EFS Funded')
            st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            st.table(filtered_df_5[["Team", "Lead", "Funded"]])

       with col2:
            st.subheader('Top Team 3 Agents')
            st.table(filtered_df_17[["Rank","Agent Name", "Funded"]].head(10))
   elif radio == 'Team 4':
       with col1:
            st.subheader('Total EFS Funded')
            st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            st.table(filtered_df_5[["Team", "Lead", "Funded"]])

       with col2:
            st.subheader('Top Team 4 Agents')
            st.table(filtered_df_18[["Rank","Agent Name", "Funded"]].head(10))
   elif radio == 'Team 5':
      with col1:
           st.subheader('Total EFS Funded')
           st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
           st.markdown(hide_table_row_index, unsafe_allow_html=True)
           st.table(filtered_df_5[["Team", "Lead", "Funded"]])

      with col2:
           st.subheader('Top Team 5 Agents')
           st.table(filtered_df_19[["Rank","Agent Name", "Funded"]].head(10))
   elif radio == 'Team 6':
      with col1:
           st.subheader('Total EFS Funded')
           st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
           st.markdown(hide_table_row_index, unsafe_allow_html=True)
           st.table(filtered_df_5[["Team", "Lead", "Funded"]])

      with col2:
           st.subheader('Top Team 6 Agents')
           st.table(filtered_df_28[["Rank","Agent Name", "Funded"]].head(10))
   elif radio == 'Team 7':
      with col1:
           st.subheader('Total EFS Funded')
           st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
           st.markdown(hide_table_row_index, unsafe_allow_html=True)
           st.table(filtered_df_5[["Team", "Lead", "Funded"]])

      with col2:
           st.subheader('Top Team 7 Agents')
           st.table(filtered_df_29[["Rank","Agent Name", "Funded"]].head(10)) 

   elif radio == 'Team 8':
      with col1:
           st.subheader('Total EFS Funded')
           st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
           st.markdown(hide_table_row_index, unsafe_allow_html=True)
           st.table(filtered_df_5[["Team", "Lead", "Funded"]])

      with col2:
           st.subheader('Top Team 8 Agents')
           st.table(filtered_df_30[["Rank","Agent Name", "Funded"]].head(10))      
   else:
      with col1:
            st.subheader('Total EFS Funded')
            st.metric("label3",filtered_df_3['Total_EFS'].iloc[0], label_visibility="collapsed")
            st.markdown(hide_table_row_index, unsafe_allow_html=True)
            st.table(filtered_df_5[["Team", "Lead", "Funded"]])

      with col2:
            st.subheader('Top EFS Agents')
            st.table(filtered_df_20[["Rank","Agent Name", "Funded"]].head(10))      
elif selected_option == "Fundies":
   radio = st.radio("Team:",(*distinct_team_FDN, 'All Teams'), horizontal=True)
   col3, col4 = st.columns([4,4]) 
   if radio == 'Team 1':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 1 Agents')
        st.table(filtered_df_21[["Rank","Agent Name", "Funded"]].head(10))
   elif radio == 'Team 2':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 2 Agents')
        st.table(filtered_df_22[["Rank","Agent Name", "Funded"]].head(10))
   elif radio =='Team 3':
        with col3:
         st.subheader('Total FDN Funded')
         st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
         st.markdown(hide_table_row_index, unsafe_allow_html=True)
         st.table(filtered_df_6[["Team", "Lead", "Funded"]])

        with col4:
         st.subheader('Top Team 3 Agents')
         st.table(filtered_df_23[["Rank","Agent Name", "Funded"]].head(10))
   elif radio =='Team 4':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 4 Agents')
        st.table(filtered_df_24[["Rank","Agent Name", "Funded"]].head(10))
   elif radio =='Team 5':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 5 Agents')
        st.table(filtered_df_25[["Rank","Agent Name", "Funded"]].head(10))
   elif radio =='Team 6':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 6 Agents')
        st.table(filtered_df_31[["Rank","Agent Name", "Funded"]].head(10))
   elif radio =='Team 7':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 7 Agents')
        st.table(filtered_df_32[["Rank","Agent Name", "Funded"]].head(10))

   elif radio =='Team 8':
       with col3:
        st.subheader('Total FDN Funded')
        st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
        st.markdown(hide_table_row_index, unsafe_allow_html=True)
        st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
        st.subheader('Top Team 8 Agents')
        st.table(filtered_df_33[["Rank","Agent Name", "Funded"]].head(10))
   else:
       with col3:
         st.subheader('Total FDN Funded')
         st.metric("label2", filtered_df_4['Total_FDN'].iloc[0], label_visibility="collapsed")
         st.markdown(hide_table_row_index, unsafe_allow_html=True)
         st.table(filtered_df_6[["Team", "Lead", "Funded"]])

       with col4:
         st.subheader('Top FDN Agents')
         st.table(filtered_df_2[["Rank","Agent Name", "Funded"]].head(10))
elif selected_option == "CSR Declines":
    radio = st.radio("Team:",('Team 1', 'Team 2', 'All Teams'),horizontal=True)
    col5, col6 = st.columns([4,4])  
    if radio == 'Team 1':
        with col5:
          st.subheader('Total CSR Decline Funded')
          st.metric("label1",filtered_df_9['Total_DF'].iloc[0], label_visibility="collapsed")
          st.markdown(hide_table_row_index, unsafe_allow_html=True)
          st.table(filtered_df_8[["Team", "Lead", "Funded"]])

        with col6:
          st.subheader('Top Team 1 Agents')
          st.table(filtered_df_26[["Rank","Agent Name", "Funded"]].head(10))
    elif radio =='Team 2':
        with col5:
          st.subheader('Total CSR Decline Funded')
          st.metric("label1",filtered_df_9['Total_DF'].iloc[0], label_visibility="collapsed")
          st.markdown(hide_table_row_index, unsafe_allow_html=True)
          st.table(filtered_df_8[["Team", "Lead", "Funded"]])

        with col6:
          st.subheader('Top Team 2 Agents')
          st.table(filtered_df_27[["Rank","Agent Name", "Funded"]].head(10))
    else:
        with col5:
          st.subheader('Total CSR Decline Funded')
          st.metric("label1",filtered_df_9['Total_DF'].iloc[0], label_visibility="collapsed")
          st.markdown(hide_table_row_index, unsafe_allow_html=True)
          st.table(filtered_df_8[["Team", "Lead", "Funded"]])

        with col6:
          st.subheader('Top CSR Decline Agents')
          st.table(filtered_df_7[["Rank","Agent Name", "Funded"]].head(10))

elif selected_option == "Progressa & Lendful Funded":
    col10, col11  = st.columns(2)
    
    with col10:
         #st.subheader('Total Progressa Funded')
         st.metric("Total Progressa Funded",filtered_df_10['Prog_funded'])
    with col11:
         st.metric('Total Lendful Funded',filtered_df_11['Lend_funded'])
     
elif selected_option == "CCC & Evergreen Funded":
    col12, col13 = st.columns([4,4])
    
    with col12:
         #st.subheader("Total CCC Funded")
         st.metric("Total CCC Funded",filtered_df_12['ccc_funded'],label_visibility="visible")
         st.subheader("Top CCC Agents")
         st.markdown(hide_table_row_index, unsafe_allow_html=True)
         st.table(filtered_df_14[["Rank","Agent Name", "Funded"]])
         
    with col13:
         st.metric('Total Evergreen Funded',filtered_df_13['evergreen_funded'])   
         st.subheader("Top Evergreen Agents")
         st.markdown(hide_table_row_index, unsafe_allow_html=True)
         st.table(filtered_df_15[["Rank","Agent Name", "Funded"]])
        
#Display next refresh time and logo    
col7, col8, col9 = st.columns([1.5,0.25,0.365])

with col7:
  if hours == 0:
    st.write(f"Next refresh in {minutes} minute{'s' if minutes != 1 else ''} ({next_refresh_time})")
  else:
    st.write(f"Next refresh in {hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''} ({next_refresh_time})")
with col8:
    st.write(" ")
with col9:
    st.image("logo.png")


css = '''
      <style>
      section.main > div:has(~ footer ) {
      padding-bottom: 5px;
      }
      </style>
      '''
st.markdown(css, unsafe_allow_html=True)
