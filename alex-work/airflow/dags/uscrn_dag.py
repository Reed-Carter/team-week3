from google.cloud import bigquery
from google.oauth2 import service_account
from bs4 import BeautifulSoup
import requests
import pandas as pd 
import yaml 
import re
# Airflow imports: 
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
