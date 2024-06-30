import os
import pandas as pd
import numpy as np
import mysql.connector
from google.cloud import bigquery
from datetime import timedelta, date, datetime, time
import db_dtypes
import pandas_gbq as pandas
import pytz
import logging

logname = '/usr/log/chc.log'

logging.basicConfig(filename=logname,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Running python script clients_historicalclients")


mydb = mysql.connector.connect(
  host="ro.db.investengine.com",
  user="renat.yunisov",
  password="HtyfnHtyfn2002%",
  database="investengine"
)

os.environ.setdefault("GCLOUD_PROJECT", "investengine-analytics")
client = bigquery.Client()