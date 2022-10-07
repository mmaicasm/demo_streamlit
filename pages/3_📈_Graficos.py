# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
# Librerias necesarias
import pandas as pd
# Funciones necesarias 
from utils import db
from utils.functions import space


#Set page context
st.set_page_config(
  page_title = "Gráficos",
  page_icon = "📈",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)

st.title('Gráficos')

# Conexión
session = db.connect()

df1 = session.sql('SELECT * FROM TRIPS LIMIT 1000').to_pandas()

df1 = df1.rename({'START_STATION_LATITUDE': 'latitude', 'START_STATION_LONGITUDE': 'longitude'}, axis = 'columns').astype({'latitude': 'float', 'longitude': 'float'})

# Mapa
st.dataframe(data = df1)

space(2)

df2 = session.sql('SELECT USERTYPE as USUARIO, COUNT(*) AS VIAJES FROM TRIPS GROUP BY USERTYPE').to_pandas()

# Barras
st.line_chart(data = df2, x = 'USUARIO', y = 'VIAJES')