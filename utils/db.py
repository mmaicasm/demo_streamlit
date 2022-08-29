# Streamlit
import streamlit as st
# Librerias necesarias
import pandas as pd
# Snowpark
from snowflake.snowpark.session import Session


@st.experimental_singleton()
def connect():
  # Parámetros de conexión
  connection_parameters = {
    "account": st.secrets["snowflake_account"],
    "user": st.secrets["snowflake_user"],
    "password": st.secrets["snowflake_password"],
    "role": "SYSADMIN",
    "database": "TEST_SNOWPARK",
    "schema": "STREAMLIT",
    "warehouse": "COMPUTE_WH"
  }
  
  # Crear conexión
  session = Session.builder.configs(connection_parameters).create()
      
  return session

def available_roles(session) -> list:
  df = session.sql("SELECT CURRENT_AVAILABLE_ROLES() AS ROLE").to_pandas()
  roles = df['ROLE'][0]
  lst = roles.strip('][').replace('"', '').split(', ')

  return lst

def available_wh(session) -> pd.DataFrame:
  session.sql("SHOW WAREHOUSES").collect()
  df = session.sql('SELECT "name" as NAME, "size" as SIZE FROM table(result_scan(last_query_id()))').to_pandas()

  return df

def collect(session) -> pd.DataFrame:
  df = session.table("TB_COMMENTS")
  df = df.select("NAME", "DATE", "COMMENT").to_pandas()
  
  return df

def insert(session, name, date, comment) -> int:
  if name:
    try:
      session.sql(f"INSERT INTO TB_COMMENTS VALUES ('{name}', '{date}', '{comment}')").collect()
      return 0
    except:
      return 2
  else:
    return 1