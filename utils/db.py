import pandas as pd
import streamlit as st
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

def collect(session) -> pd.DataFrame:
  df = session.table("TB_COMMENTS")
  df = df.select("NAME", "DATE", "COMMENT").to_pandas()
  
  return df

def insert(session, name, date, comment) -> int:
  if name:
    st.write('test')
    try:
      session.sql(f"INSERT INTO TB_COMMENTS VALUES ('{name}', '{date}', '{comment}')").collect()
      return 0
    except:
      #st.error(f"Error en insert de la fila -> ('{name}', '{date}', '{comment}')")
      return 2
  else:
    st.write('test2')
    #st.warning("Introduce tu nombre")
    return 1