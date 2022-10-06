# %%
# Funciones necesarias
from config import connect

# Establecer la conexión
session = connect()

# %%
def reset_schema(session, prestaged = False):
  session.sql('CREATE OR REPLACE SCHEMA CITIBIKE').collect() 

  if prestaged:
    session.sql('CREATE OR REPLACE STAGE LOAD_STAGE URL = https://s3.amazonaws.com/tripdata/').collect()
  else: 
    session.sql('CREATE STAGE IF NOT EXISTS LOAD_STAGE').collect()

# %%
# Crear el esquema
reset_schema(session)
# %%
