# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
# Librerias necesarias
import pandas as pd
# Funciones necesarias 
from utils import db

# Formato de página
st.set_page_config(
  page_title = "Generador de scripts",
  page_icon = "🏭",
  layout = "wide",
  initial_sidebar_state = "auto",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app made by Miguel Maicas and powered by Snowpark for Python and Streamlit"
  }
)
st.title('Generador de scripts')
#st.header("Cabecera")
#st.subheader('Subcabecera')
st.markdown('xxxxxxxxxxxx')

# Funciones
def xxxxxx(type, object):
  df = pd.DataFrame()
  # Tablas
  if 'TABLE' in type:
    session.sql(f"SHOW TERSE TABLES LIKE '%{object}%' IN ACCOUNT").collect()
    df1 = session.sql('SELECT "kind" as KIND, "database_name" as DATABASE_NAME, "schema_name" as SCHEMA_NAME, "name" as NAME  FROM table(result_scan(last_query_id()))').to_pandas()
    df = pd.concat([df, df1], ignore_index = True)
  
  return df

# Conexión
session = db.connect()

# Widgets
w1 = st.selectbox(options = ['TABLE', 'VIEW', 'TASK', 'STREAM'], help = 'Cada tipo añadido reduce el rendimiento de la consulta', label = 'Tipos')
w2 = st.text_input(placeholder = 'Nombre del objeto', label = 'Objeto')

# Solo refrescar si hay texto
if w1 and w2:
  # Buscar objeto
  with st.spinner(f'Buscando en Snowflake...'):
    df = search_object(w1, w2)