import pandas as pd
import requests
import re
import dateutil
import sys
import logging
from airflow import DAG
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from typing import List
from google.cloud import bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from datetime import timedelta
import pendulum

@task
def scrape_nws_forcast_data():
    '''
    This function uses beautiful soup to scrape weather forcast data from the websites below and returns a dataframe
    '''
    urls = [
            "https://forecast.weather.gov/MapClick.php?lat=57.0826&lon=-135.2692#.Y-vs_9LMJkg",
            'https://forecast.weather.gov/MapClick.php?lat=45.5118&lon=-122.6756#.Y-vtHNLMJkg',
            ]
        
    combined_df = pd.DataFrame()

    for url in urls:
        r = requests.get(url)
        soup = BeautifulSoup(r.content,"html.parser")

        #various containers
        item1 = soup.find_all(id='current_conditions-summary')

        #raw data
        temp_f = [item.find(class_="myforecast-current-lrg").get_text() for item in item1]
        temp_min = soup.find('p', {'class': 'temp temp-low'}).text.strip()
        temp_max = soup.find('p', {'class': 'temp temp-high'}).text.strip()


        #df of temperatures
        df_temperature = pd.DataFrame({"temp" : temp_f,'tempmin': temp_min,'tempmax': temp_max})

        #df_2 is a df of current conditions in detail (Humidity, Wind Speed, Barometer, Dewpoint, Visibility, Last update)
        table = soup.find_all('table')
        df_2 = pd.read_html(str(table))[0]
        df_2 = df_2.pivot(columns=0, values=1).ffill().dropna().reset_index().drop(columns=['index'])

        #merge both dataframes
        temp_df=pd.concat([df_temperature,df_2],axis=1)

        #scrape lattitude, longitude, and elevation 
        lat_lon_elev = soup.find('span', {'class': 'smallTxt'}).text.strip()
        lat, lon, elev = re.findall(r'[-+]?\d*\.\d+|\d+', lat_lon_elev)

        #scrape name
        station = soup.find('h2', {'class': 'panel-title'}).text.strip()

        #add location, lat, long, and elev to source_df
        temp_df['elevation_ft'] = elev
        temp_df['latitude'] = lat
        temp_df['longitude'] = lon
        temp_df['weather_station'] = station

        combined_df = pd.concat([temp_df, combined_df], ignore_index=True, sort=False)
        combined_df = combined_df.fillna(0)

        dict_df = combined_df.to_dict()

    return dict_df

@task
def scrape_precip_data():
    '''
    Scrapes precip data from the websites in the 'URLS' variable and returns a small 1 column dataframe of precip data for the cities of portland and Sitka.
    '''
    urls = [
            'https://www.localconditions.com/weather-portland-oregon/97201/past.php',
            "https://www.localconditions.com/weather-sitka-alaska/99835/past.php",
            ]

    precip_df = pd.DataFrame()

    for url in urls:
            r = requests.get(url)
            soup = BeautifulSoup(r.content,"html.parser")
            details = soup.select_one(".past_weather_express")
            data = details.find_all(text=True, recursive=False)
            data = [item.strip() for item in data]
            data = [item for item in data if item]
            data = data[2]
            df = pd.DataFrame([data], columns=['precip'])
            precip_df = pd.concat([precip_df, df], ignore_index=True, sort=False)
            
    precip_df['precip'] = precip_df['precip'].str.extract(pat='(\d+\.?\d*)').astype(float)
    precip_df = precip_df.fillna(0)
    precip_dict = precip_df.to_dict()
    
    return precip_dict

@task
def combine_dataframes(dict1, dict2):

    #create dataframes from the dicts since dicts can not be passed as xcoms
    df1 = pd.DataFrame.from_dict(dict1)
    df2 = pd.DataFrame.from_dict(dict2)

    #combine the two dataframes 
    source_df = pd.concat([df1,df2],axis=1)

    #converty the df back to a dictionary to be passed to the next task and return the dict
    source_dict = source_df.to_dict()
    return source_dict

@task
def data_transformation(dict1):

    #convert the dict to a df to be transformed
    df = pd.DataFrame.from_dict(dict1)
    # Convert 'lat' and 'lon' columns to float type
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)

    # Convert 'elev' column to int type
    df['elevation_ft'] = df['elevation_ft'].astype(int)

    # Extract the numeric part of the temperature string and convert it to int
    df['temp'] = df['temp'].str.extract('(\d+)').astype(float)

    # Extract the numeric part of the tempmin string and convert it to int
    df['tempmin'] = df['tempmin'].str.extract('(\d+)').astype(float)

    # Extract the numeric part of the temperature string and convert it to int
    df['tempmax'] = df['tempmax'].str.extract('(\d+)').astype(float)

    # Split wind speed values into components and convert speed to int type
    df['Wind Speed'] = df['Wind Speed'].str.extract('(\d+)', expand=False).fillna(0).astype(float)

    # Convert 'humidity' column to int type
    df['Humidity'] = df['Humidity'].str.extract('(\d+)', expand=False).astype(float)

    # Convert 'barometer' column to float type, and convert inches to millibars
    df['Barometer'] = round(df['Barometer'].apply(lambda x: float(x.split()[0]) * 33.8639 if 'in' in x and x != 'NA' else None), 2)

    # Convert 'Visibility' column to float type
    df['Visibility'] = df['Visibility'].str.extract('(\d+\.\d+|\d+)', expand=False).astype(float).round(2)

    #Convert 'last_update' column to UTC
    df['Last update'] = df['Last update'].apply(lambda x: dateutil.parser.parse(x, tzinfos={"EST": -5 * 3600, "CST": -6 * 3600, "MST": -7 * 3600,"PST": -8 * 3600,"AKST": -9 * 3600,"HST": -10 * 3600}))
    df['Last update'] = df['Last update'].apply(lambda x: x.astimezone(dateutil.tz.tzutc()))
    df['datetime'] = df['Last update'].dt.strftime('%Y-%m-%d')
    # df['datetime'] = pd.to_datetime(df['datetime'])

    # make wind chill a float if exists and only display degree F
    try:
        df[['Wind Chill']] = df['Wind Chill'].str.extract('(\d+)', expand=True).astype(float)
    except:
        None

    # extract the numeric value of dewpoint and only display the degree n farenheit
    df[['Dewpoint']] = df['Dewpoint'].str.extract('(\d+)', expand=True).astype(float)

    #change precip data type to float
    df['precip'] = df['precip'].astype(float)

    #rename weather station column to the city
    def rename_station(value):
        if value == 'Portland, Portland International Airport (KPDX)':
            return 'Portland'
        elif value == 'Sitka - Sitka Airport (PASI)':
            return 'Sitka'

    df['name'] = df['weather_station'].map(rename_station)

    #change the names and order of columns to better fit the historical data
    df = df.rename({'Humidity': 'humidity', 'Wind Speed': 'windspeed', 'Visibility': 'visibility','Wind Chill': 'windchill','Dewpoint':'dewpoint'}, axis=1) 
    #this line only includes necesarry columns
    df = df.reindex(['name','datetime','tempmax','tempmin','temp','windchill','dewpoint','humidity','precip','windspeed','visibility'], axis=1)

    df = df.fillna(0)
    df_dict = df.to_dict()
    return df_dict

@task
def write_weather_data_to_bigquery(data):

    PROJECT_ID = "team-week3"
    DATASET_ID = "reed_weather_data"
    DAILY_TABLE_ID = "weather_data"

    SCHEMA = [
                # indexes are written if only named in the schema
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('datetime', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('tempmax', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('tempmin', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('temp', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('windchill', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('dewpoint', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('humidity', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('precip', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('windspeed', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('visibility', 'FLOAT64', mode='NULLABLE'),
            ]

    df = pd.DataFrame(data)

    client = bigquery.Client()

    try:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = client.get_dataset(dataset_ref)
    except:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)

    table_ref = dataset.table(DAILY_TABLE_ID)

    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = client.create_table(table)

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


@task
def load_data_to_BigQuery(
    df: pd.DataFrame, 
    client: bigquery.Client, 
    table_name: str, 
    schema: List[bigquery.SchemaField], 
    create_disposition: str = 'CREATE_IF_NEEDED', 
    write_disposition: str = 'WRITE_APPEND'
    ) -> None:
    """load dataframe into bigquery table

    Args:
        df (pd.DataFrame): dataframe to load
        client (bigquery.Client): bigquery client
        table_name (str): full table name including project and dataset id
        schema (List[bigquery.SchemaField]): table schema with data types
        create_disposition (str, optional): create table disposition. Defaults to 'CREATE_IF_NEEDED'.
        write_disposition (str, optional): overwrite table disposition. Defaults to 'WRITE_TRUNCATE'.
    """
    # *** run some checks ***
    # test table name to be full table name including project and dataset name. It must contain to dots
    assert len(table_name.split('.')) == 3, f"Table name must be a full bigquery table name including project and dataset id: '{table_name}'"



    # setup bigquery load job:
    #  create table if needed, replace rows, define the table schema
    job_config = bigquery.LoadJobConfig(
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        schema=schema
    )
    logger = logging.getLogger(__name__) 
    logger.info(f"loading table: '{table_name}'")
    job = client.load_table_from_dataframe(df, destination=table_name, job_config=job_config)
    job.result()        # wait for the job to finish
    # get the resulting table
    table = client.get_table(table_name)
    logger.info(f"loaded {table.num_rows} rows into {table.full_table_id}")

    logger.info(f"loaded weather_data")

@dag(
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2023, 2, 19, 7, 55, tz="UTC"),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['dsa', 'dsa-example'],
)

def ETL_pipeline():

    #create df1
    scrape_detailed_data_task = scrape_nws_forcast_data()

    #create df2
    scrape_precip_data_task = scrape_precip_data()

    #combine df's to make a source df
    combine_df_task = combine_dataframes(scrape_detailed_data_task, scrape_precip_data_task)

    #data transformation task
    data_transformation_task = data_transformation(combine_df_task)

    #load to BigQuery
    load_to_BigQuery_task = write_weather_data_to_bigquery(data_transformation_task)

    #orchestrate tasks
    [scrape_detailed_data_task, scrape_precip_data_task] >> combine_df_task >> data_transformation_task >> load_to_BigQuery_task

dag = ETL_pipeline()




#----------------unused task-----------------------------------------

# @task(multiple_outputs=True)
# def define_load_parameters():
#     #authorization
#     key_path = "/home/reed/.creds/dsa-deb-sa.json"
#     credentials = service_account.Credentials.from_service_account_file(
#         key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

#     client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

#     # change to match your filesystem
#     PROJECT_NAME = credentials.project_id
#     DATASET_NAME = "reed_weather_data"

#     # **** BIGQUERY DATASET CREATION ****

#     dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
#     dataset = bigquery.Dataset(dataset_id)
#     dataset.location = "US"
#     dataset = client.create_dataset(dataset, exists_ok=True)
    
#     logger = logging.getLogger(__name__) 
#     logger.info(f"Created weather dataset: {dataset.full_dataset_id}")

#     FACTS_TABLE_METADATA = {
#         'weather_data': {
#             'table_name': 'weather_data',
#             'schema': [
#                 # indexes are written if only named in the schema
#                 bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
#                 bigquery.SchemaField('datetime', 'DATETIME', mode='NULLABLE'),
#                 bigquery.SchemaField('tempmax', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('tempmin', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('temp', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('windchill', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('dewpoint', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('humidity', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('precip', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('windspeed', 'FLOAT64', mode='NULLABLE'),
#                 bigquery.SchemaField('visibility', 'FLOAT64', mode='NULLABLE'),
#             ]
#         }      
#     }

#     # Load scraped Sitka and Portland daily weather to the historical data table

#     # get table name and schema from FACTS_TABLE_METADATA config param
#     table_name = f"{PROJECT_NAME}.{DATASET_NAME}.{FACTS_TABLE_METADATA['weather_data']['table_name']}"
#     schema = FACTS_TABLE_METADATA['weather_data']['schema']
    
#     return {'client':client, 'table':table_name, 'schema':schema}