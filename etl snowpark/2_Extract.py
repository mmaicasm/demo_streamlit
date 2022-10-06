# %%
# Librerias necesarias
import pandas as pd
from datetime import datetime
import requests
from zipfile import ZipFile
from io import BytesIO
import os

# Funciones necesarias
from config import connect

# Establecer la conexión
session = connect()

# %%
# Para ficheros como 201306-citibike-tripdata.zip
date_range1 = pd.period_range(start=datetime.strptime("201306", "%Y%m"), end=datetime.strptime("201612", "%Y%m"), freq='M').strftime("%Y%m")
file_name_end1 = '-citibike-tripdata.zip'
files_to_download = [date+file_name_end1 for date in date_range1.to_list()]

# %%
# Para ficheros como 201701-citibike-tripdata.csv.zip (cambiaron el formato)
date_range2 = pd.period_range(start=datetime.strptime("201701", "%Y%m"), end=datetime.strptime("202112", "%Y%m"), freq='M').strftime("%Y%m")
file_name_end2 = '-citibike-tripdata.csv.zip'
files_to_download = files_to_download + [date+file_name_end2 for date in date_range2.to_list()]

# %%
# Carga de prueba
files_to_download = [files_to_download[i] for i in [0,102]] #19,50,100,102]]
files_to_download

# %%
schema1_download_files = list()
schema2_download_files = list()
schema2_start_date = datetime.strptime('202102', "%Y%m")

for file_name in files_to_download:
  file_start_date = datetime.strptime(file_name.split("-")[0], "%Y%m")
  if file_start_date < schema2_start_date:
    schema1_download_files.append(file_name)
  else:
    schema2_download_files.append(file_name)

# %%
schema1_download_files, schema2_download_files

schema1_load_stage = 'LOAD_STAGE/schema1/'
schema2_load_stage = 'LOAD_STAGE/schema2/'

schema1_files_to_load = list()
for zip_file_name in schema1_download_files:
    
  url = 'https://s3.amazonaws.com/tripdata/' + zip_file_name
  
  print('Downloading and unzipping: ' + url)
  r = requests.get(url)
  file = ZipFile(BytesIO(r.content))
  csv_file_name = file.namelist()[0]
  file.extract(csv_file_name)
  file.close()
  
  print('Putting ' + csv_file_name + ' to stage: ' + schema1_load_stage)
  session.file.put(local_file_name = csv_file_name, 
    stage_location = schema1_load_stage, 
    source_compression = 'NONE', 
    overwrite = True)
  schema1_files_to_load.append(csv_file_name)
  os.remove(csv_file_name)
    
schema2_files_to_load = list()
for zip_file_name in schema2_download_files:
    
  url = 'https://s3.amazonaws.com/tripdata/' + zip_file_name
  
  print('Downloading and unzipping: ' + url)
  r = requests.get(url)
  file = ZipFile(BytesIO(r.content))
  csv_file_name = file.namelist()[0]
  file.extract(csv_file_name)
  file.close()
  
  print('Putting ' + csv_file_name + ' to stage: ' + schema2_load_stage)
  session.file.put(local_file_name = csv_file_name, 
    stage_location = schema2_load_stage, 
    source_compression = 'NONE', 
    overwrite = True)
  schema2_files_to_load.append(csv_file_name)
  os.remove(csv_file_name)
    
# %% 
session.sql("list @LOAD_STAGE pattern='.*20.*[.]gz'").collect()
# %%
