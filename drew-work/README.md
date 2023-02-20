## National Weather Service Web Scrape and ETL
#### By [Drew White](https://www.linkedin.com/in/drew-riley-white/)

#### Scraping and transforming daily weather data from the National Weather Service and taking that data to perform weekly aggregations.

## Technologies Used


* Python
* Apache Airflow
* Pandas
* BeautifulSoup
* Google BigQuery

</br>

## Sources:
_A dictionary of the sources of the city weather data:_

<img src="./../images/dw_cities.png" height=50% width=50%>

## Description:
## dw_weather_scrape.py
<img src="../images/dw_webscrape_daily_gif.gif">

* `scrape_weather_data`
    - Uses BeautifulSoup to scrape National Weather Service and put into Pandas data frame.
* `transform_weather_data`
    - Takes Pandas data frame and makes transformations on data to create more usable values.
* `write_weather_data_to_bq`
    - Writes the scraped/transformed data to Google BigQuery daily appending on to existing `daily` table.

<img src="../images/dw_weather_scrape.png" height=60% width=60%>

### Daily Schema:
<img src="../images/dw_daily_schema.png">

## dw_weekly_avg.py
<img src="../images/dw_weekly_avg_gif.gif" height=60% width=60%>

* `calculate_weekly_averages`
    - Pulls `daily` data from BigQuery and gets averages of select columns.
* `write_weekly_avg_to_bq`
    - Writes averages to BigQuery on weekly schedule to `weekly_avg` table.

<img src="../images/dw_weekly_avg.png" height=60% width=60%>

### Weekly Avg Schema:
<img src="../images/dw_weekly_avg_schema.png">
<br>

## Known Bugs

* No known bugs

<br>

_If you find any issues, please reach out at: **d.white0002@gmail.com**._

Copyright (c) _2023_ _Drew White_

</br>