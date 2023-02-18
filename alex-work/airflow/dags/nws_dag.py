from datetime import datetime, timedelta
from typing import Dict
import pandas as pd
import numpy as np
import requests
import os
from bs4 import BeautifulSoup
from utils.nws_utils import getColsFromTable, getDict
# Airflow imports: 
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


@task 
def getForecast():
  """Get dictionary of forecast data for next 48 hours from various points in Alaska"""
  locations = pd.read_csv("data/locations.csv")
  loc_dict = dict(zip(locations['station_location'], locations['nws_url']))

  col_list = []
  for location, url in loc_dict.items():
    result = requests.get(url)
    soup = BeautifulSoup(result.content, "html.parser")
    table48 = soup.find_all("table")[5].find_all("tr") # list of <tr> elements from main data table (really two tables combined: one for each day in next 48h period)
    colspan = table48[0]  # divided into two tables by two colspan elements
    table48 = [tr for  tr in table48 if tr != colspan] # remove colspan elements

    cols = getColsFromTable(table48,location)    
    col_list.extend(cols)
  
  return getDict(col_list)

@task
def transformDF(myDict): 
  """Cast dictionary from getForecast() to a dataframe, transform, and write (append) to .csv"""
  df = pd.DataFrame(myDict)
  df.columns = [col.lower() for col in df.columns] 
  df.replace({'':np.NaN, '--':np.NaN}, inplace=True)

  ## Datetime Transformations
  cur_year = datetime.now().year
  dt_strings = df['date'] + '/' + str(cur_year) + ' ' + df['hour (akst)'] + ':00 AKST'
  # Local time (AKST)
  df['lst_datetime'] = pd.to_datetime(dt_strings, format='%m/%d/%Y %H:%M AKST')
  # UTC time
  akst_offset = timedelta(hours=9)
  df['utc_datetime'] = df['lst_datetime'] + akst_offset

  # reorder columns 
  cols = ['location','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
  df = df[cols]

  # timestamp column: track when forecast was accessed -- DAG will run every 48 hours
  df['fcst_date'] = datetime.now()

  ## Write to CSV
  hdr = False  if os.path.isfile('data/forecasts.csv') else True
  df.to_csv('data/forecasts.csv', mode='a', index=False, header=hdr)

@task
def load_data_to_bq() -> None:
    """Load the transformed data to BigQuery"""
    table_id = 'team-week3.alaska.forecasts'
    bq_conn_id = 'google_cloud_default'

    job_config = {
        'schema': [
            {'name':'location', 'type':'STRING', "mode":"REQUIRED"},
            {'name':'utc_datetime', 'type':'DATETIME', "mode":"REQUIRED"},
            {'name':'lst_datetime', 'type':'DATETIME', "mode":"REQUIRED"},
            {'name':'temperature (°f)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'dewpoint (°f)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'wind chill (°f)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'surface wind (mph)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'wind dir', 'type':'STRING', "mode":"NULLABLE"},
            {'name':'gust', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'sky cover (%)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'precipitation potential (%)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'relative humidity (%)', 'type':'INTEGER', "mode":"NULLABLE"},
            {'name':'rain', 'type':'STRING', "mode":"NULLABLE"},
            {'name':'thunder', 'type':'STRING', "mode":"NULLABLE"},
            {'name':'snow', 'type':'STRING', "mode":"NULLABLE"},
            {'name':'freezing rain', 'type':'STRING', "mode":"NULLABLE"},
            {'name':'sleet', 'type':'STRING', "mode":"NULLABLE"},
            {'name':'fcst_date', 'type':'DATETIME', "mode":"REQUIRED"},
        ],
        'write_disposition': 'WRITE_APPEND',
        'destination_table_description': 'NWS Forecasts for locations in Alaska'
    }

    bq_insert_job = BigQueryInsertJobOperator(
        task_id='bq_insert_job',
        configuration=job_config,
        location='US',
        gcp_conn_id=bq_conn_id,
        task_concurrency=1,
        params={
            'project_id':'team-week3',
            'table_id':table_id
        }
    )

    df = pd.read_csv('data/forecasts.csv')
    
    bq_insert_job.execute(context={'data': df.to_dict(orient='records')})

@dag(
   schedule_interval="@once",
   start_date=datetime.utcnow(),
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
def nws_dag():
    t1 = getForecast()
    t2 = transformDF(t1)
    t3 = load_data_to_bq()

    t1 >> t2 >> t3

dag = nws_dag()
