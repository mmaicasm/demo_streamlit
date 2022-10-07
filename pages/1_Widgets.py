# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
# Librerias necesarias
# Funciones necesarias 
from utils import db
from utils.functions import space


#Set page context
st.set_page_config(
  page_title = "Widgets",
  page_icon = "",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)

st.title('Widgets')

# Conexión
session = db.connect()

st.button('Click aquí')
st.checkbox('Aceptar')
st.multiselect('Escoge uno', options = ['Gato', 'Perro']) #selectbox multiselect

space(1)

st.text_input('Introduce un texto')
st.number_input('Elige una fecha')
st.date_input('Elige una fecha')

space(10)

# IMAGEN
img = st.camera_input(label = "Foto")
space(5)