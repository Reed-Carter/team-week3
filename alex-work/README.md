### **Alaska Weather ETL Pipeline** 

There are two main data sources for my portion of the project:
* [USCRN Hourly Historical Weather Data](https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/): This page contains hourly weather data from the U.S. Climate Reference Network / U.S. Regional Climate Reference Network (USCRN/USRCRN) stored in text files. 
* [NWS Forecasts](https://forecast.weather.gov/MapClick.php?lat=60.7506&lon=-160.5006&unit=0&lg=english&FcstType=digital): The National Weather Service has forecast offices in Fairbanks and Anchorage which provide hourly forecasts by coordinate location in AK. These are available in 48-Hour blocks up to four days out, stored in a tabular format.
  
![nws_tabular_example](img/nws_tabular_ex.png)

#### **Steps in the ETL**  
1. Scrape all currently available USCRN data from stations in Alaska, then transform and load to BigQuery.
2. Use Airflow to orchestrate the same process for any updates issued by USCRN and NWS.
3. Connect BigQuery dataset to Looker Studio Dashboard.

#### **Directory Structure** 
```
├── notebooks
│   ├── uscrn_scrape.ipynb
│   └── uscrn_scrape.py
├── airflow
│   ├── airflow.sh   # Activate Airflow CL  
│   ├── dags
│   │   ├── nws_dag.py       
│   │   ├── uscrn_updates.py 
│   │   └── utils
│   │       └── utils.py
│   ├── data
│   │   ├── sources.yaml
│   │   ├── nws_updates    # Stores updates from nws_dag.py
│   │   └── uscrn_updates # Stores updates from uscrn_updates.py
│   └── docker-compose.yaml
├── img
└── README.md
```

`./notebooks/uscrn_scrape.ipynb` &nbsp;- &nbsp; Explains and contains code to scrape the main USCRN data as well as supplemental data on column headers and descriptions.  

`./notebooks/uscrn_scrape.py` &nbsp; - &nbsp; Contains a python script to scrape all currently available data from the USCRN database (run this to download the data instead of the notebook).


####  **Updating Data** 
The `./airflow/dags/` directory contains two dag files (`uscrn_updates.py` and `nws_dag.py`) that can scrape updates from the USCRN and NWS data sources at regularly scheduled interviews. This scheduling parameter is customizable via the `dag` decorator at the end of each file: 

```python 
@dag(
   schedule_interval="@once", # Change here 
   start_date=dt.datetime.utcnow(),
   catchup=False,
   default_view='graph',
   is_paused_upon_creation=True,
)
```

