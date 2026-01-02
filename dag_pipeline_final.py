# Downloading the dataset using Kaggle API:
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from datetime import datetime
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import pandas as pd
import numpy as np
import logging


def download_dataset(ti):
    api = KaggleApi()

    # authenticating by checking the API tokens that were set as environment variables.
    api.authenticate()

    # defining the dataset download address or ID or whatever and then defining the directory where the dataset will be downloaded.
    dataset = "muthuj7/weather-dataset"
    directory = "/home/thorsundell/airflow/datasets"

    # a method to create the previously defined directory, if alrade exists then no error.
    os.makedirs(directory, exist_ok=True)

    # method to download the dataset in the defined dir but not unzipping it yet.
    api.dataset_download_files(dataset, path=directory, unzip=False)

    # using zipfile to unzip the downloaded file
    with zipfile.ZipFile("/home/thorsundell/airflow/datasets/weather-dataset.zip", 'r') as unzipper:
        unzipper.extractall(directory)

    # defining the path to the downloaded dataset for xcom push. Using this method so that no errors arise
# from difference in OS path type differences.
    dataset_path = os.path.join(directory, 'weatherHistory.csv')

    # loading dataset and setting it as the DataFrame
    df = pd.read_csv(dataset_path)

    ti.xcom_push(key='dataset_directory', value=directory)
    ti.xcom_push(key='dataframe', value=df)


def transform_dataset(ti):

    # Cleaning up the dataset:
    df = ti.xcom_pull(key='dataframe', task_ids='Extraction')

    # Reformatting the date columns to datetime format.
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='raise', utc=True)

    # defining medians for certain columns
    temperature_median = df['Temperature (C)'].median()
    humidity_median = df['Humidity'].median()
    wind_speed_median = df['Wind Speed (km/h)'].median()
    pressure_median = df['Pressure (millibars)'].median()

    # replacing nan values with the medians.
    df['Temperature (C)'] = df['Temperature (C)'].replace(np.nan, temperature_median)
    df['Humidity'] = df['Humidity'].replace(np.nan, humidity_median)
    df['Wind Speed (km/h)'] = df['Wind Speed (km/h)'].replace(np.nan, wind_speed_median)

    # checking for 'impossible' values and replacing them with the median
    df = df.mask((df['Temperature (C)'] < -50) | (df['Temperature (C)'] > 50), temperature_median)
    df = df.mask((df['Humidity'] < 0) | (df['Humidity'] > 1), humidity_median)
    df = df.mask((df['Wind Speed (km/h)'] < 0) | (df['Wind Speed (km/h)'] > 500), wind_speed_median)

    # replacing empty precip types from NaN to 'None'
    df['Precip Type'] = df['Precip Type'].replace(np.nan, 'None')

    # removing possible duplicate rows from dataset
    df = df.drop_duplicates()



    # Creating the daily table:
    df = df.set_index('Formatted Date')

    # Resampling data to daily buckets
    daily_averages = df.resample('D').agg({
        'Temperature (C)':'mean',
        'Humidity':'mean',
        'Wind Speed (km/h)':'mean'
    })
    # making the daily bucket average columns into a dataframe
    daily_averages = pd.DataFrame(daily_averages)

    # renaming those columns so there will be no duplicates when joining tables.
    daily_averages = daily_averages.rename(columns={
        'Temperature (C)': 'Avg Temperature (C)',
        'Humidity': 'Avg Humidity',
        'Wind Speed (km/h)': 'Avg Wind Speed (km/h)'
    })

    # merging the original weather dataframe with the daily averages dataframe.

    # Now this new merged table is quite weird because it contains hourly weather data from the first hour of a day
# and the average weather data of the WHOLE day aswell...
    daily_table = pd.merge(df, daily_averages, left_index=True, right_index=True)
    daily_table = daily_table.reset_index()
    daily_table = daily_table.drop(['Wind Bearing (degrees)', 'Loud Cover', 'Daily Summary', 'Summary'], axis=1)

    # defining wind strength categorization function
    def categorize_wind(wind_kmh):
        wind_ms = round((wind_kmh / 3.6), 1)
        
        if wind_ms <= 1.5:
            return 'Calm'
        elif wind_ms >= 1.6 and wind_ms <= 3.3:
            return 'Light Air'
        elif wind_ms >=3.4 and wind_ms <= 5.4:
            return 'Light Breeze'
        elif wind_ms >= 5.5 and wind_ms <= 7.9:
            return 'Gentle Breeze'
        elif wind_ms >= 8 and wind_ms <= 10.7:
            return 'Moderate Breeze'
        elif wind_ms >= 10.8 and wind_ms <= 13.8:
            return 'Fresh Breeze'
        elif wind_ms >= 13.9 and wind_ms <= 17.1:
            return 'Strong Breeze'
        elif wind_ms >= 17.2 and wind_ms <= 20.7:
            return 'Near Gale'
        elif wind_ms >= 20.8 and wind_ms <= 24.4:
            return 'Gale'
        else:
            return np.nan

    # applying categorization method to the daily table and creating a new column out of it.
    daily_table['Wind Strength'] = daily_table['Wind Speed (km/h)'].apply(categorize_wind)

    # changing the new columns place
    col = daily_table.pop('Wind Strength')
    daily_table.insert(6, 'Wind Strength', col)

    daily_table['Formatted Date'] = daily_table['Formatted Date']


    ti.xcom_push(key='daily_table', value=daily_table)



    # Creating the monthly table:

    # function to check the precip mode foe each month
    def check_modes(month):
        mode = month.mode()
        if len(mode) == 1:
            return mode.iloc[0]

        else:
            return np.nan

    # applying the function
    monthly_precip_mode = df['Precip Type'].resample('M').apply(check_modes)
    # making it into a dataframe
    monthly_precip_mode = pd.DataFrame(monthly_precip_mode)

    # calculating monthly averages of selected columns
    monthly_averages = df.resample('M').agg({
        'Temperature (C)': 'mean',
        'Apparent Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
    })

    # making the column averages into a dataframe
    monthly_averages = pd.DataFrame(monthly_averages)

    #combining the two previous dataframes
    monthly_table = pd.merge(monthly_averages, monthly_precip_mode, left_index=True, right_index=True)
    monthly_table = monthly_table.drop('Wind Speed (km/h)', axis=1)

    monthly_table = monthly_table.rename(columns={
        'Temperature (C)': 'Avg Temperature (C)',
        'Apparent Temperature (C)': 'Avg Apparent Temperature (C)',
        'Humidity': 'Avg Humidity',
        'Visibility (km)': 'Avg Visibility (km/h)',
        'Pressure (millibars)': 'Avg Pressure (millibars)',
        'Precip Type': 'Mode Precip Type'
    })

    monthly_table = monthly_table.reset_index()

    # changing the "Formatted Date" column into a "Month" column.
    monthly_table = monthly_table.rename(columns={'Formatted Date': 'Month'})
    monthly_table['Month'] = monthly_table['Month'].dt.date

    ti.xcom_push(key='monthly_table', value=monthly_table)




def data_validation(ti):
    daily_table = ti.xcom_pull(key='daily_table', task_ids='Transformation')
    monthly_table = ti.xcom_pull(key='monthly_table', task_ids='Transformation')

    # checking certain colums for erroneous or nonsensical values. Raising an airflow exception if something is wrong.
    if daily_table.isna().values.any():
        raise AirflowFailException('Empty values in daily table')
    
    if monthly_table.isna().values.any():
        raise AirflowFailException('Empty values in monthly table')

    if daily_table['Temperature (C)'].values.any() < -50 or daily_table['Temperature (C)'].values.any() > 50:
        raise AirflowFailException('Erroneous value in daily table')
    
    if daily_table['Humidity'].values.any() < 0 or daily_table['Humidity'].values.any() > 1:
        raise AirflowFailException('Erroneous value in daily table')
    
    if daily_table['Wind Speed (km/h)'].values.any() < 0:
        raise AirflowFailException('Erroneous value in daily table')



    def define_season(row):
        winter = [12, 1, 2]
        spring = [3, 4, 5]
        summer = [6, 7, 8]
        fall = [9, 10, 11]
        if row in winter:
            return 'winter'
        elif row in spring:
            return 'spring'
        elif row in summer:
            return 'summer'
        elif row in fall:
            return 'fall'

    # calculating the seasonal average and standard deviation for average temperatures in daily_table so that we can flag outliers in Airflow.
    daily_table['season'] = daily_table['Formatted Date'].dt.month.map(define_season)
    seasonal_temperature_data = daily_table.groupby('season')['Avg Temperature (C)'].agg(['mean', 'std']).to_dict('index')
    # creates a dictionary of dictionaries that holds the average and std for every season.

    # If a daily avg temperature is over 3 standard deviations form the seasonal average then it is logged in airflow logs as an outlier.

    def flag_seasonal_outlier(row):
        mean = seasonal_temperature_data[row['season']]['mean']
        std = seasonal_temperature_data[row['season']]['std']
        if abs(row['Avg Temperature (C)'] - mean) > 3 * std:
            logging.warning(f"Detected outliers: {row[['Formatted Date', 'Avg Temperature (C)']].to_string}")

        
    daily_table.apply(flag_seasonal_outlier, axis=1)

    daily_table = daily_table.drop('season', axis=1)
    daily_table['Formatted Date'] = daily_table['Formatted Date'].dt.date

    # loading "daily_table" and "monthly_table" as csv-files into the same directory as the original dataset

    # loading original dir
    dataset_directory = ti.xcom_pull(key='dataset_directory', task_ids='Extraction')

    # creating dir for daily_table, monthly_table and uploading them as a csv files there.
    daily_table_directory = os.path.join(dataset_directory, 'daily_table.csv')
    daily_table.to_csv(daily_table_directory, date_format='%Y-%m-%d')

    monthly_table_directory = os.path.join(dataset_directory, 'monthly_table.csv')
    monthly_table.to_csv(monthly_table_directory, date_format='%Y-%m-%d')

    # pushing table directories
    ti.xcom_push(key='daily_table_dir', value=daily_table_directory)
    ti.xcom_push(key='monthly_table_dir', value=monthly_table_directory)




def load_into_postgres(ti):

    # loading table directories
    daily_dir = ti.xcom_pull(key='daily_table_dir', task_ids='Validation')
    monthly_dir = ti.xcom_pull(key='monthly_table_dir', task_ids='Validation')

    # establishing connection with the database through airflow connections

    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()

    cur = conn.cursor()


    # copying the contents of the csv files that were just created and loading them into the database
    with open(daily_dir, 'r') as f:
        next(f)
        cur.copy_from(f, 'daily_weather', sep=',', null='')

    with open(monthly_dir, 'r') as f:
        next(f)
        cur.copy_from(f, 'monthly_weather', sep=',', null='')


    conn.commit()
    cur.close()
    conn.close()




default_args = {
    "owner": "thorsundell",
    "start_date": datetime(2025, 11, 28)
}

dag = DAG(
    "Weather_ETL_2",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)


task_extract = PythonOperator(
    task_id = 'Extraction',
    python_callable = download_dataset,
    dag = dag
)

task_transform = PythonOperator(
    task_id = 'Transformation',
    python_callable = transform_dataset,
    dag = dag
)

task_validate = PythonOperator(
    task_id = 'Validation',
    python_callable = data_validation,
    dag = dag
)

task_load = PythonOperator(
    task_id = 'Loading',
    python_callable = load_into_postgres,
    dag = dag,
    trigger_rule = 'all_success'
)



task_extract >> task_transform >> task_validate >> task_load
