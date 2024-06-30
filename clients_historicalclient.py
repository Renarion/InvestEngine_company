import os
import pandas as pd
import numpy as np
import mysql.connector
from google.cloud import bigquery
from datetime import timedelta, date, datetime, time
import db_dtypes
import pandas_gbq as pandas
import pytz


f = open("/var/log/clients_historicalclient.log", "a")

f.write(f'[{datetime.now()}]: Starting uploading - table: investengine.clients_historicalclient\n')
f.flush()

mydb = mysql.connector.connect(
  host="ro.db.investengine.com",
  user="renat.yunisov",
  password="HtyfnHtyfn2002%",
  database="investengine"
)

os.environ.setdefault("GCLOUD_PROJECT", "investengine-analytics")
client = bigquery.Client()

clients_historicalclient_mysql_checking_date_query = """
select max(date(history_date)) as max_date
from `investengine-analytics.analytics_mysql_tables.clients_historicalclient_mysql`
"""

project_id_clients_historicalclient = "investengine-analytics"
table_id_clients_historicalclient = 'analytics_mysql_tables.clients_historicalclient_mysql'
table_id_total_clients_historicalclient = "investengine-analytics.analytics_mysql_tables.clients_historicalclient_mysql"

def post_message_with_files(message,
                            channel):
    import slack_sdk
    SLACK_TOKEN = "xoxb-2254913596768-6955760044114-5GnEFzO1pPlQroXbYV0jCI21"
    client = slack_sdk.WebClient(token=SLACK_TOKEN)
    client.chat_postMessage(channel=channel, text=message)

start_timestamp_clients_historicalclient = datetime.now()

clients_historicalclient_mysql_checking_date_df = client.query(clients_historicalclient_mysql_checking_date_query).to_dataframe()
clients_historicalclient_mysql_checking_date_df = clients_historicalclient_mysql_checking_date_df['max_date'][0]

if ((clients_historicalclient_mysql_checking_date_df + timedelta(days=1)) > (date.today() - timedelta(days=1))) == True:
    f.write(f'[{datetime.now()}]: We cannot upload new data because of difference between today and max date in table, there is today or next day - table: investengine.clients_historicalclient')
    f.flush()
elif ((clients_historicalclient_mysql_checking_date_df + timedelta(days=1)) > (date.today() - timedelta(days=1))) == False:
    start_date_clients_historicalclient = (clients_historicalclient_mysql_checking_date_df + timedelta(days=1))
    end_date_clients_historicalclient = (date.today() - timedelta(days=1))

    date_list_clients_historicalclient = [(start_date_clients_historicalclient + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(((end_date_clients_historicalclient - start_date_clients_historicalclient).days)+1)]

    f.write(f'[{datetime.now()}]: Last date in the table: {clients_historicalclient_mysql_checking_date_df}. Transfering starts from: {date_list_clients_historicalclient[0]} until: {date_list_clients_historicalclient[-1]} - table: investengine.clients_historicalclient')
    f.flush()

for i in date_list_clients_historicalclient:

    clients_historicalclient_mysql_query = f"""
        select id,
        password,
        is_superuser,
        reference,
        email,
        email_verified,
        email_hash,
        is_staff,
        is_active,
        date_joined,
        password_filled,
        ip_address,
        wealth_source,
        other_wealth_source,
        employment_status,
        employer,
        experience_with_equities,
        approved_on,
        last_wrong_login,
        wrong_login_attempts,
        state,
        has_device,
        agreed_with_terms,
        agreed_with_risk_disclosure,
        agreed_with_isa_declaration,
        created,
        app_client_id,
        crm_account_guid,
        crm_legal_id_guid,
        data_confirmed,
        history_id,
        history_date,
        history_change_reason,
        history_type,
        history_user_id,
        type,
        data_confirmed_from_ip,
        is_test,
        agreed_with_privacy_policy,
        tfa,
        is_compliance_blocked,
        agreed_with_sipp_declaration
        from investengine.clients_historicalclient
        where date(history_date) = '{i}'
        """

    if ((datetime.now() - start_timestamp_clients_historicalclient).seconds >= 10800) == False:
        
        f.write(f'[{datetime.now()}]: Starting uploading {i} - table: investengine.clients_historicalclient')
        f.flush()
        
        try:
            clients_historicalclient_mysql_df = pd.read_sql(clients_historicalclient_mysql_query, mydb)
            clients_historicalclient_mysql_df['history_user_id'] = clients_historicalclient_mysql_df['history_user_id'].astype('str')
            clients_historicalclient_mysql_df['agreed_with_sipp_declaration'] = clients_historicalclient_mysql_df['agreed_with_sipp_declaration'].astype('str')
        except:
            f.write(f'[{datetime.now()}]: Got an error, wait... - table: investengine.clients_historicalclient')
            f.flush()

            time.sleep(5)
            
            mydb = mysql.connector.connect(
            host="ro.db.investengine.com",
            user="renat.yunisov",
            password="HtyfnHtyfn2002%",
            database="investengine"
            )

            clients_historicalclient_mysql_df = pd.read_sql(clients_historicalclient_mysql_query, mydb)
            clients_historicalclient_mysql_df['history_user_id'] = clients_historicalclient_mysql_df['history_user_id'].astype('str')
            clients_historicalclient_mysql_df['agreed_with_sipp_declaration'] = clients_historicalclient_mysql_df['agreed_with_sipp_declaration'].astype('str')

        job = client.load_table_from_dataframe(
            clients_historicalclient_mysql_df, 
            table_id_total_clients_historicalclient,
        )  
        job.result() 
        table = client.get_table(table_id_total_clients_historicalclient)

    elif ((datetime.now() - start_timestamp_clients_historicalclient).seconds >= 10800) == True:

        f.write(f'[{datetime.now()}]: Start last uploading {i} - table: investengine.clients_historicalclient')
        f.flush()

        clients_historicalclient_mysql_df = pd.read_sql(clients_historicalclient_mysql_query, mydb)
        clients_historicalclient_mysql_df['history_user_id'] = clients_historicalclient_mysql_df['history_user_id'].astype('str')
        clients_historicalclient_mysql_df['agreed_with_sipp_declaration'] = clients_historicalclient_mysql_df['agreed_with_sipp_declaration'].astype('str')

        job = client.load_table_from_dataframe(
            clients_historicalclient_mysql_df, 
            table_id_total_clients_historicalclient,
        )  
        job.result() 
        table = client.get_table(table_id_total_clients_historicalclient)

        break
       
f.write(f'[{datetime.now()}]: Sending message in Slack - table: investengine.clients_historicalclient')
f.flush()
        
post_message_with_files(
message=f"""Hey! :wave: I've succesfully uploaded new records from *_MySQL: investengine.clients_historicalclient_* into *__BigQuery: {table_id_clients_historicalclient}_* 
\n\n*Period of records:* {date_list_clients_historicalclient[0]} to {date_list_clients_historicalclient[-1]} 
\n*Starting datetime:* {start_timestamp_clients_historicalclient} 
\n*Ending datetime:* {datetime.now()} 
\n*Loading period:* {(datetime.now() - start_timestamp_clients_historicalclient).seconds / 60} min""",
channel="C074S8WU935",
)
    