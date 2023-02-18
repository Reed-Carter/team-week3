from google.cloud import bigquery
from google.oauth2 import service_account
from bs4 import BeautifulSoup
import requests
import pandas as pd 
import yaml 
import re
from collections import deque
from io import StringIO
from datetime import datetime, timedelta \
# Airflow imports: 
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

@task
def lastAdded() -> datetime: 
  """Reads/returns latest 'date_added_utc' value from .csv"""
  with open("data/uscrn.csv", 'r') as fp:
    q = deque(fp, 1)  
  last_added = pd.read_csv(StringIO(''.join(q)), header=None).iloc[0,-1]
  last_added = datetime.strptime(last_added, "%Y-%m-%d %H:%M:%S.%f")
  # Convert to EST from UTC -- 'Last modified' field in getNewFile() is given in EST
  last_added = last_added - timedelta(hours=5)

  return last_added

@task
def getNewFileURLs(last_added:datetime, ti=None) -> list: 
  """Check/obtain updates from USCRN updates page"""
  now = datetime.utcnow()
  updates_url = f"https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/updates/{now.year}"

  df = pd.read_html(updates_url, skiprows=[1,2])[0]
  df.drop(["Size", "Description"], axis=1, inplace=True)
  df.dropna(inplace=True)
  df['Last modified'] = pd.to_datetime(df['Last modified'])

  df = df[df['Last modified'] > last_added]

  # Will use update_range to name .csv later
  update_range = (min(df['Last modified']), max(df['Last modified'])) 
  ti.xcom_push(key="update_range", value=update_range)

  new_file_urls = list(updates_url + "/" + df['Name'])

  return new_file_urls

@task
def getUpdates(new_file_urls:list) -> dict: 
  """Scrape data from list of new urls, store and return as list of lists"""

  locations = pd.read_csv("data/locations.csv")[['station_location', 'wbanno']]
  locations['wbanno'] = locations['wbanno'].astype(int).astype(str)
  wbs = set(locations['wbanno'])

  rows = []
  for url in new_file_urls:
    response = requests.get(url)
    soup = BeautifulSoup(response.content,'html.parser')
    soup_lines = str(soup).strip().split("\n")[3:]
    ak_rows = [re.split('\s+', line) for line in soup_lines if line[0:5] in wbs]
    rows.extend(ak_rows)

  return rows

@task
def transformDF(rows:list, ti=None): 
  """Read rows from getUpdates(), cast to dataframe, transform, write to csv"""
  
  # Get column headers 
  columns = list(pd.read_csv("data/column_descriptions.csv")['col_name'])

  # Get locations
  locations = pd.read_csv("data/locations.csv")[['station_location', 'wbanno']]
  locations['wbanno'] = locations['wbanno'].astype(int).astype(str) 
  locations.set_index("wbanno", inplace=True)

  # Create dataframe
  df = pd.DataFrame(rows, columns=columns[1:])

  # Merge locations
  df = df.merge(locations, how="left", left_on="wbanno", right_index=True)

  # Reorder columns 
  columns = ['station_location'] + list(df.columns)[:-1]
  df = df[columns]

  # Change datatypes
  df = df.apply(pd.to_numeric, errors='ignore')

  # Replace missing value designators with NaN
  df.replace([-99999,-9999], np.nan, inplace=True) 
  df.replace({'crx_vn':{-9:np.nan}}, inplace=True)
  df = df.filter(regex="^((?!soil).)*$") # almost all missing values

  # Create datetime columns
  df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(int).astype(str) + df['utc_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')
  df['lst_datetime'] = pd.to_datetime(df['lst_date'].astype(int).astype(str) + df['lst_time'].astype(int).astype(str).str.zfill(4), format='%Y%m%d%H%M')

  # Drop old date and time columns 
  df.drop(['utc_date', 'utc_time', 'lst_date', 'lst_time'], axis=1, inplace=True)

  # Reorder columns 
  cols = ['station_location','wbanno','crx_vn','utc_datetime','lst_datetime'] + list(df.columns)[3:-2]
  df = df[cols]

  # Add date-added column (utc)
  df['date_added_utc'] = datetime.utcnow() 

  # Write to .csv
  # Pull `update_range` from XCOM (created by 'getNewFileUrls())
  update_range = ti.xcom_pull(key="update_range", task_id="getNewFileURLs")
  df.to_csv(f"data/updates/{update_range[0]}-{update_range[1]}.csv")


@task
def uploadBQ():
  """Upload latest update .csv file to BigQuery"""

  


  




@dag(
   schedule_interval="@once",
   start_date=datetime.utcnow(),
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
def uscrn_dag():
    
    t1 = lastAdded()
    t2 = getUpdates(t1)
    t3 = transformDF(t2)
    t4 = uploadBQ()

    t1 >> t2 >> t3 >> t4



