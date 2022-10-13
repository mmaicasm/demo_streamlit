# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
# Librerias necesarias
import pandas as pd
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
# Funciones necesarias 
from utils import db
from utils.functions import space


#Set page context
st.set_page_config(
  page_title = "Tablas",
  page_icon = "🗄️",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)

st.title('Tablas')

# Conexión
session = db.connect()

session.sql(f"SHOW TABLES").collect()

# Tabla
df1 = session.sql('SELECT * FROM table(result_scan(last_query_id()))').to_pandas()
#st.write(df1)

space(2)

tabla = st.selectbox(options = df1['name'], label = 'Tabla')

df2 = session.sql('SELECT * FROM ' + tabla + ' LIMIT 1000').to_pandas()
st.dataframe(df2)

# Grid
grid = AgGrid(df2)