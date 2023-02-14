### ETL Overview 

There are two main data sources for my portion of the project:
* [USCRN Hourly Historical Weather Data](https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/): This page contains hourly weather data from the U.S. Climate Reference Network / U.S. Regional Climate Reference Network (USCRN/USRCRN) stored as tabular .txt files. 
* [NWS Forecasts](https://forecast.weather.gov/MapClick.php?lat=60.7506&lon=-160.5006&unit=0&lg=english&FcstType=digital): The National Weather Service has forecast offices in Fairbanks and Anchorage which provide hourly forecasts from various weather stations in AK. These are available in 48-Hour blocks up to four days out, stored in a tabular format.
  
![nws_tabular_example](img/nws_tabular_ex.png)


_USCRN pipeline_

For data that's already available: 

1. Save column names and description from [headers.txt](https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/headers.txt) to .csv
2.  Scrape all existing USCRN data from weather stations in Alaska and save locally to a .csv
       - For each year ([2000-2023](https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/)): 
         - Find all filenames containing 'AK'
         - For each 'AK' file: 
           - Retrieve .txt data as dataframe 
           - Add column headers from `headers.txt`
           - Add a location column based on filename 
           - Append to .csv file
           - Add location name to `sources.yaml` under `cities`.
3.  Upload .csv to Big Query 

For future data, write a dag with tasks: 
1. getUpdates()
    -  Checks the value for 'Last modified' on the current year's [index page](https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/2023/) 
    -  If more recent than the last recorded value: 
       -  For every AK dataset on page: 
          -  Retrieve the .txt file 
          -  Add column headers from `headers.txt`
          -  Filter for records after current 'Last modified' value
          -  Write (append) new records to .csv  
       -  Store the new 'Last modified' value 
       -  Trigger next task
    -  Else, pass and do not trigger next task
2. upload() 
   - Upload (append) .csv of new records to BigQuery table 

_NWS pipeline_

I have not found records of previous weather forecasts from NWS, so there won't be any preliminary data uploading for this data source. 

For future data, add new tasks to the DAG: 

1. getCurrentForecast():
   - For each AK location: 
     - Scrape hourly forecast for next 48 hours
     - Save as dataframe (need to "transpose" rows and columns, first)
     - Add column for location 
     - Append to .csv
2. upload():
   - Upload (append) .csv of new forecast records to BigQuery.
   - Can use the same function defined for the other data source -- just change destination in BigQuery and check column schema to match

It might be interesting to check how the accuracy of weather forecasts is affected by how far out the prediction is made. To that effect, I could modify getCurrentForecast() to include farther-out forecasts and track when a given forecast was made: 
   - For each AK Location:
      - From current date to farthest out: 
        - Scrape hourly forecast for next 48 hours
        - Save as dataframe (need to "transpose" rows and columns, first)
        - Click 'Forward 2 Days' with selenium and repeat above steps until furthest date is reached
      - Add timestamp column ('forecast_made')
      - Add column for location
      - Append to .csv
