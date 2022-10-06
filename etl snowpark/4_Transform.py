# %%
# Librerias necesarias
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T

# Funciones necesarias
from config import connect

# Establecer la conexión
session = connect()

# %%
# Esquema de 2013 a 2021
trips_table_schema = T.StructType([
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

trips_table_schema_names = [field.name for field in trips_table_schema.fields]
transdf1 = session.table('RAW_SCHEMA1')[trips_table_schema_names]
transdf2 = session.table('RAW_SCHEMA2')[trips_table_schema_names]
transdf = transdf1.union_by_name(transdf2)

# %%
# Formateos de fechas
date_format_2 = "1/1/2015 [0-9]:.*$"      #1/1/2015 1:30 -> #M*M/D*D/YYYY H*H:M*M(:SS)*
date_format_3 = "1/1/2015 [0-9][0-9]:.*$" #1/1/2015 10:30 -> #M*M/D*D/YYYY H*H:M*M(:SS)*
date_format_4 = "12/1/2014.*"             #12/1/2014 02:04:53 -> M*M/D*D/YYYY 

date_format_match = "^([0-9]?[0-9])/([0-9]?[0-9])/([0-9][0-9][0-9][0-9]) ([0-9]?[0-9]):([0-9][0-9])(:[0-9][0-9])?.*$"
date_format_repl = "\\3-\\1-\\2 \\4:\\5\\6"

# %%
transdf.with_column('STARTTIME', F.regexp_replace(F.col('STARTTIME'),
    F.lit(date_format_match), 
    F.lit(date_format_repl)))\
  .with_column('STARTTIME', F.to_timestamp('STARTTIME'))\
  .with_column('STOPTIME', F.regexp_replace(F.col('STOPTIME'),
    F.lit(date_format_match), 
    F.lit(date_format_repl)))\
  .with_column('STOPTIME', F.to_timestamp('STOPTIME'))\
  .select(F.col('STARTTIME'), 
    F.col('STOPTIME'), 
    F.col('START_STATION_ID'), 
    F.col('START_STATION_NAME'), 
    F.col('START_STATION_LATITUDE'), 
    F.col('START_STATION_LONGITUDE'), 
    F.col('END_STATION_ID'), 
    F.col('END_STATION_NAME'), F.col('END_STATION_LATITUDE'), 
    F.col('END_STATION_LONGITUDE'), 
    F.col('USERTYPE'))\
  .write.mode('overwrite').save_as_table('TRIPS')

# %%
testdf = session.table('TRIPS')
testdf.schema

# %%
testdf.count()
# %%
