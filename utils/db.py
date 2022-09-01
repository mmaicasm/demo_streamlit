# Streamlit
import streamlit as st
# Librerias necesarias
import pandas as pd
# Snowpark
from snowflake.snowpark.session import Session

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

@st.experimental_singleton()
def connect():
  session = Session.builder.configs(connection_parameters).create()
  return session

@st.experimental_singleton()
def available_roles(_session) -> list:
  # Listado de roles
  df = _session.sql("SELECT CURRENT_AVAILABLE_ROLES() AS ROLE").to_pandas()
  roles = df['ROLE'][0]
  lst = roles.strip('][').replace('"', '').strip().split(', ')
  
  refresh_role(lst, connection_parameters['role'])
  
  return lst

def refresh_role(list, role):
  # Rol por defecto
  if role in list:
    st.session_state['role'] = role
    st.session_state['role_index'] = list.index(role)
  else:
    st.session_state['role'] = list[0]
    st.session_state['role_index'] = 0

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