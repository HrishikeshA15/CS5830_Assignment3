# Big Data Lab - Assignment 2


# ## Task 1 (DataFetch Pipeline)

# Importing required modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
from bs4 import BeautifulSoup # For parsing HTML
import random
import os
import urllib
import shutil # For archive making and moving


url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
year = 2002
num_files = 2

archive_dir = '/tmp/archives' 
data_dir = '/tmp/data/' + '{{params.year}}/' 
HTML_dir = '/tmp/web/' 

# Creating a default dictionary
conf = dict(
    url = url,
    year = Param(year, type="integer", minimum=1901,maximum=2024),
    num_files = num_files,
    archive_dir = archive_dir,  
)

# DAG properties
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Instantiate DAG
dag1 = DAG(
    dag_id = "Fetch_data",
    default_args=default_args,
    params = conf,
    description='A simple pipeline',
    schedule=None,  
)


# Task 1. Fetch page containing the location wise datasets for that year. (Bash Operator with wget or curl command)
# Dictionary for storing parameters for fetching page
fetch_page_param = dict(
    url = "{{ dag_run.conf.get('url', params.url) }}",
    file_save_dir = HTML_dir
    )

# BashOperator task for fetching the page using wget
fetch_page = BashOperator(
    task_id=f"get_data",
    bash_command="curl {{params.url}}{{params.year }}/ --create-dirs -o {{params.file_save_dir}}{{params.year}}.html",
    params = fetch_page_param,
    dag=dag1,
)


# 2. Based on the required number of data files, select the data files randomly from the available list of files. (Python Operator)
def parse_content(data, url, year):
    res = []
    page_url = f"{url}/{year}/"

    # Parse the HTML page 
    soup = BeautifulSoup(data, 'html.parser')
    hyperlinks = soup.find_all('a')

    # Iterate through each hyperlink
    for link in hyperlinks:
        href = link.get('href')
        if ".csv" in href:
            file_url = f'{page_url}{href}'
            res.append(file_url)

    # Return the list of extracted CSV file URLs
    return res

def select_random(num_files, url, year_param,file_save_dir,**kwargs):
    filename = f"{file_save_dir}{year_param}.html"
    with open(filename, "r") as f:
        pages_content = f.read()
    available_files = parse_content(pages_content,url=url,year=year_param)

    # Select random files
    selected_files_url = random.sample(available_files, int(num_files))
    
    return selected_files_url


# Define a dictionary to store parameters for selecting files
select_fil_params = dict(
    num_files = "{{ dag_run.conf.get('num_files', params.num_files) }}",
    year_param = "{{ dag_run.conf.get('year', params.year) }}",
    url = "{{ dag_run.conf.get('url', params.url) }}",
    file_save_dir = HTML_dir
)
# Define Select files task
select_files = PythonOperator(
    task_id='select_files',
    python_callable=select_random,
    op_kwargs=select_fil_params,
    dag=dag1,
)

# 3. Fetch the individual data files (Bash or Python Operator)
def download_file(file_url, csv_output_dir):
    os.makedirs(csv_output_dir, exist_ok=True)
    file_name = urllib.parse.unquote(os.path.basename(file_url))
    file_path = os.path.join(csv_output_dir, file_name)
    os.system(f"curl {file_url} -o {file_path}")
    return file_name



def get_indvd_files(csv_output_dir,**kwargs):
    ti = kwargs['ti']
    selected_files = ti.xcom_pull(task_ids='select_files')

    for file_url in selected_files:
        # Implementing logic to download each file
        download_file(file_url, csv_output_dir)

# Defining parameters to pass
fetch_files_param = dict( csv_output_dir = data_dir )

# Task to download CSV
fetch_files = PythonOperator(
    task_id='get_files',
    python_callable=get_indvd_files,
    op_kwargs=fetch_files_param,
    dag=dag1,
)


# 4. Zip them into an archive. (Python Operator)
def zip_files(output_dir, archive_path, **kwargs):
    shutil.make_archive(archive_path, 'zip', output_dir)

archive_path = data_dir[:-1] if data_dir[-1]=='/' else data_dir
zip_params = dict(output_dir = data_dir,
                        archive_path = archive_path)

# Creating Task
zip_files = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs=zip_params,
    dag=dag1,
)


def move_archive(archive_path, target_location, **kwargs):
    os.makedirs(target_location, exist_ok=True)
    shutil.move(archive_path + '.zip', os.path.join(target_location , str(kwargs['dag_run'].conf.get('year'))) + '.zip')

# Params for move_archive function
move_params = dict(target_location = "{{ dag_run.conf.get('archive_dir', params.archive_dir) }}",
                        archive_path = archive_path)

# Creating Task
move_archive = PythonOperator(
    task_id='move_archive',
    python_callable=move_archive,
    op_kwargs=move_params,
    dag=dag1,
)


# Define task dependencies
fetch_page >> select_files >> fetch_files >> zip_files >> move_archive





#------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------
# Task 2 (analytic pipeline)

# from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import apache_beam as beam
import pandas as pd
import geopandas as gpd
from geodatasets import get_path
import matplotlib.pyplot as plt
import shutil
import os
import numpy as np
import logging
from ast import literal_eval as make_tuple


# Path to your archive file
archive_path = "/tmp/archives/2004.zip"
req_fields = "WindSpeed, BulbTemperature"

conf = dict(
    archive_path = archive_path,
    req_fields = req_fields
)

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'analytics_pipeline',
    default_args=default_args,
    description='An analytics pipeline for data visualization',
    params = conf,
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
)

# Task 1: Wait for the archive to be available
wait_for_archive = FileSensor(
    task_id = 'wait_for_archive',
    mode="poke",
    poke_interval = 5,  
    timeout = 5,  
    filepath = "{{params.archive_path}}",
    dag=dag,
    fs_conn_id = "my_file_system", 
)

# Task 2: Unzip the archive
unzip_archive = BashOperator(
    task_id='unzip_archive',
    bash_command="unzip -o {{params.archive_path}} -d /tmp/data2",
    dag=dag,
)



# Task 3: Extract CSV contents and filter data
def parseCSV(data):
        df = data.split('","')
        df[0] = df[0].strip('"')
        df[-1] = df[-1].strip('"')
        return list(df)


class ExtractAndFilterFields(beam.DoFn):
    def __init__(self,req_fields,**kwargs):
        super().__init__(**kwargs)
        self.req_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in req_fields:
                    if j.lower() in i.lower():
                        self.req_fields.append(ind)
        self.headers = {i:ind for ind,i in enumerate(headers_csv)} # defining headers

    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.req_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            yield ((lat, lon), data)



def process_csv_files(req_fields,**kwargs):
    req_fields = list(map(lambda a:a.strip(),req_fields.split(",")))
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(req_fields=req_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/tmp/results/result.txt')
required_f = dict(
    req_fields = "{{ params.req_fields }}",
)

process_csv_files = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    op_kwargs = required_f,
    dag=dag,
)

class ExtractFieldsWithMonth(beam.DoFn):
    def __init__(self,req_fields,**kwargs):
        super().__init__(**kwargs)
        self.req_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in req_fields:
                    if j.lower() in i.lower():
                        self.req_fields.append(ind)

        self.headers = {i:ind for ind,i in enumerate(headers_csv)} # defining headers

    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.req_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            Measuretime = datetime.strptime(element[headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            Month_format = "%Y-%m"
            Month = Measuretime.strftime(Month_format)
            yield ((Month, lat, lon), data)


# Task 4: Compute monthly averages
def compute_averages(data):
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    res = np.ma.average(masked_data, axis=0)
    res = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),res)

def compute_monthly_averages(req_fields, **kwargs):
    req_fields = list(map(lambda a:a.strip(),req_fields.split(",")))
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractFieldsWithMonth(req_fields=req_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_averages(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/tmp/results/averages.txt')
        

compute_monthly_averages = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_averages,
    op_kwargs = required_f,
    dag=dag,
)

class Aggregated(beam.CombineFn):
    def __init__(self,req_fields,**kwargs):
        super().__init__(**kwargs)
        self.req_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in req_fields:
                    if j.lower() in i.lower():
                        self.req_fields.append(i.replace('Hourly',''))

    def create_accumulator(self):
        return []
    
    def add_input(self, accumulator, element):
        accumulator2 = {key:value for key,value in accumulator}
        data = element[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
        val_data = np.reshape(val_data,val_data_shape)
        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
        res = np.ma.average(masked_data, axis=0)
        res = list(res.filled(np.nan))
        for ind,i in enumerate(self.req_fields):
            accumulator2[i] = accumulator2.get(i,[]) + [(element[0],element[1],res[ind])]

        return list(accumulator2.items())
    
    def merge_accumulators(self, accumulators):
        merged = {}
        for a in accumulators:
                a2 = {key:value for key,value in a}
                for i in self.req_fields:
                    merged[i] = merged.get(i,[]) + a2.get(i,[])

        return list(merged.items())
    
    def extract_output(self, accumulator):
        return accumulator
    

def plotgeomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1],dtype='float')
    d1 = np.array(data,dtype='float')
    world = gpd.read_file(get_path('naturalearth.land'))

    data = gpd.GeoDataFrame({
        values[0]:d1[:,2]
    }, geometry=gpd.points_from_xy(*d1[:,(1,0)].T))
    
    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    world.plot(ax=ax, color='white', edgecolor='black')
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/tmp/results/plots', exist_ok=True)
    plt.savefig(f'/tmp/results/plots/{values[0]}_heatmap_plot.png')

# Task 5: Create visualization using geopandas
def create_heatmap_visualization(req_fields,**kwargs):
    req_fields = list(map(lambda a:a.strip(),req_fields.split(",")))
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/results/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(req_fields = req_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(plotgeomaps)            
        )

create_heatmap_visualization = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    op_kwargs = required_f,
    dag=dag,
)

# Task 7: Delete CSV file
def delete_csv_file(**kwargs):
    shutil.rmtree('/tmp/data2')

delete_csv_file = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv_file,
    dag=dag,
)

# Set task dependencies
wait_for_archive >> unzip_archive >> process_csv_files
process_csv_files >> delete_csv_file
unzip_archive >> compute_monthly_averages
compute_monthly_averages >> create_heatmap_visualization
create_heatmap_visualization >> delete_csv_file
