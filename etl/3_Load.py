# %%
# Librerias necesarias
from snowflake.snowpark import types as T

# Funciones necesarias
from config import connect

# Establecer la conexión
session = connect()

# %%
# Esquema de 2013 a 2021
load_schema1 = T.StructType([
  T.StructField("tripduration", T.StringType()),
  T.StructField("STARTTIME", T.StringType()), 
  T.StructField("STOPTIME", T.StringType()), 
  T.StructField("START_STATION_ID", T.StringType()),
  T.StructField("START_STATION_NAME", T.StringType()), 
  T.StructField("START_STATION_LATITUDE", T.StringType()),
  T.StructField("START_STATION_LONGITUDE", T.StringType()),
  T.StructField("END_STATION_ID", T.StringType()),
  T.StructField("END_STATION_NAME", T.StringType()), 
  T.StructField("END_STATION_LATITUDE", T.StringType()),
  T.StructField("END_STATION_LONGITUDE", T.StringType()),
  T.StructField("bike_id", T.StringType()),
  T.StructField("USERTYPE", T.StringType()), 
  T.StructField("birth_year", T.StringType()),
  T.StructField("gender", T.StringType())
])

# Esquema de Febrero 2021 en adelante (lo cambiaron)
load_schema2 = T.StructType([
  T.StructField("ride_id", T.StringType()), 
  T.StructField("rideable_type", T.StringType()), 
  T.StructField("STARTTIME", T.StringType()), 
  T.StructField("STOPTIME", T.StringType()), 
  T.StructField("START_STATION_NAME", T.StringType()), 
  T.StructField("START_STATION_ID", T.StringType()),
  T.StructField("END_STATION_NAME", T.StringType()), 
  T.StructField("END_STATION_ID", T.StringType()),
  T.StructField("START_STATION_LATITUDE", T.StringType()),
  T.StructField("START_STATION_LONGITUDE", T.StringType()),
  T.StructField("END_STATION_LATITUDE", T.StringType()),
  T.StructField("END_STATION_LONGITUDE", T.StringType()),
  T.StructField("USERTYPE", T.StringType())
])

# %%
# Crear tablas vacias
df1 = session.create_dataframe(data = [[None]*len(load_schema1.names)], schema = load_schema1)
df2 = session.create_dataframe(data = [[None]*len(load_schema2.names)], schema = load_schema2)
# Drop de filas con nulls
df1.na.drop().write.save_as_table('RAW_SCHEMA1')
df2.na.drop().write.save_as_table('RAW_SCHEMA2')
# %%
# Cargar datos en RAW_SCHEMA1
csv_file_format_options = {"FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'", "skip_header": 1}

loaddf = session.read.option("SKIP_HEADER", 1)\
  .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
  .option("COMPRESSION", "GZIP")\
  .option("NULL_IF", "\\\\N")\
  .option("NULL_IF", "NULL")\
  .option("pattern", "'.*20.*[.]gz'")\
  .schema(load_schema1)\
  .csv('@LOAD_STAGE/schema1/')\
  .copy_into_table('RAW_SCHEMA1', format_type_options = csv_file_format_options)

# %%
# Cargar datos en RAW_SCHEMA2
loaddf = session.read.option("SKIP_HEADER", 1)\
  .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
  .option("COMPRESSION", "GZIP")\
  .option("NULL_IF", "\\\\N")\
  .option("NULL_IF", "NULL")\
  .option("pattern", "'.*20.*[.]gz'")\
  .schema(load_schema2)\
  .csv('@LOAD_STAGE/schema2/')\
  .copy_into_table('RAW_SCHEMA2', format_type_options = csv_file_format_options)
# %%
