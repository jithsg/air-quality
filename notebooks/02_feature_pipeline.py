import datetime
import time
# import sys
# sys.path.append('../src')

import sys
from pathlib import Path

# Get the absolute path to the directory containing app.py
current_directory = Path(__file__).parent.absolute()

# Calculate the path to the root directory (assuming root_directory is one level up)
root_directory = current_directory.parent

# Add the root directory to sys.path
sys.path.append(str(root_directory))
import pandas as pd
import hopsworks
import json
import hopsworks
from src.functions import *  # Assuming functions.py contains necessary functions
import warnings
from urllib.request import urlopen
from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv()

# Access the API key
HOPSWORKS_API_KEY = os.getenv('HOPSWORKS_API_KEY')
HOPSWORKS_PROJECT_NAME  = 'airqualityjith'

warnings.filterwarnings("ignore")


project = hopsworks.login(
    project=HOPSWORKS_PROJECT_NAME,
    api_key_value=HOPSWORKS_API_KEY
)

def features():
    target_url = 'https://repo.hops.works/dev/jdowling/target_cities.json'
    response = urlopen(target_url)
    target_cities = json.loads(response.read())
    
    today = datetime.date.today()
    hindcast_day = today - datetime.timedelta(days=1)
    forecast_day = today + datetime.timedelta(days=7)
    
    start_of_cell = time.time()
    
    df_aq_raw = pd.DataFrame()
    
    for continent in target_cities:
        for city_name, coords in target_cities[continent].items():
            df_ = get_aqi_data_from_open_meteo(city_name=city_name,
                                               coordinates=coords,
                                               start_date=str(hindcast_day),
                                               end_date=str(today))
            df_aq_raw = pd.concat([df_aq_raw, df_]).reset_index(drop=True)
    end_of_cell = time.time()
    print("-" * 64)
    print(f"Parsed new PM2.5 data for ALL locations up to {str(today)}.")
    print(f"Took {round(end_of_cell - start_of_cell, 2)} sec.\n")
    
    
    df_aq_update = df_aq_raw
    
    df_aq_update['date'] = pd.to_datetime(df_aq_update['date'])
    df_aq_update = df_aq_update.dropna()
    
    df_weather_update = pd.DataFrame()
    
    start_of_cell = time.time()
    for continent in target_cities:
        for city_name, coords in target_cities[continent].items():
            df_ = get_weather_data_from_open_meteo(city_name=city_name,
                                                   coordinates=coords,
                                                   start_date=str(today),
                                                   end_date=str(forecast_day),
                                                   forecast=True)
            df_weather_update = pd.concat([df_weather_update, df_]).reset_index(drop=True)
            
    end_of_cell = time.time()
    print("-" * 64)
    print(f"Parsed new weather data for ALL cities up to {str(today)}.")
    print(f"Took {round(end_of_cell - start_of_cell, 2)} sec.\n")
    
    
    df_aq_update.date = pd.to_datetime(df_aq_update.date)
    df_weather_update.date = pd.to_datetime(df_weather_update.date)
    
    df_aq_update["unix_time"] = df_aq_update["date"].apply(convert_date_to_unix)
    df_weather_update["unix_time"] = df_weather_update["date"].apply(convert_date_to_unix)
    
    
    df_aq_update.date = df_aq_update.date.astype(str)
    df_weather_update.date = df_weather_update.date.astype(str)
    
    return df_aq_update, df_weather_update

    
    # ... Continue with the rest of the features function ...

def main():
    df_aq_update, df_weather_update = features()
    
    project = project = hopsworks.login(
    project=HOPSWORKS_PROJECT_NAME,
    api_key_value=HOPSWORKS_API_KEY
)
    fs = project.get_feature_store() 
    
    air_quality_fg = fs.get_feature_group(
        name='air_quality',
        version=1
    )
    weather_fg = fs.get_feature_group(
        name='weather',
        version=1
    )
    air_quality_fg.insert(df_aq_update, write_options={"wait_for_job": False})
    weather_fg.insert(df_weather_update, write_options={"wait_for_job": False})

if __name__ == "__main__":
    main()
