# Streamlit
import streamlit as st
# Librerias necesarias
import time
# Funciones necesarias 
from utils import db

# Formato de página
st.set_page_config(
  page_title = "Login",
  page_icon = ":snowflake:",
  layout = "wide",
  initial_sidebar_state = "auto",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app made by Miguel Maicas and powered by Snowpark for Python and Streamlit"
  }
)
st.title('Login')
st.subheader('Conexión a Snowflake a traves de Snowpark')

# Widget manual
with st.form(key = "login"):
  
  entorno = st.radio(options = ['PRO','PRE','DEV'], disabled = True, label = 'Entorno')
  user = st.text_input(placeholder = 'usuario@hiberus.com', label = 'Usuario')
  password = st.text_input(type = 'password', label = 'Contraseña')
  
  login = st.form_submit_button("Conectar")
  if login:
    if user and password:
      # Info en barra lateral
      progress_bar = st.sidebar.progress(0)
      status_text = st.sidebar.empty()
      status_text.text('Conectando a Snowflake (1/3)')
      
      # Crear sesión
      with st.spinner('Conectando a Snowflake...'):
        session = db.connect()
        time.sleep(1)
        progress_bar.progress(33)
        time.sleep(1)
        progress_bar.progress(66)
        status_text.text('Conexión establecida (2/3)')
        time.sleep(1)
      
      # Ocultar índices de tablas
      hide_table_row_index = """
        <style>
        thead tr th:first-child {display:none}
        tbody th {display:none}
        </style>
        """
      st.markdown(hide_table_row_index, unsafe_allow_html = True)
      
      # Informar conexión correcta
      progress_bar.progress(100)
      status_text.text('Sesión confirmada (3/3)')
      st.success('Sesión confirmada!')
      st.snow()
      
      # Mostrar parámetros de la sesión
      st.write('Parámetros de la sesión:')
      st.table(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
      
      time.sleep(3)
      progress_bar.empty()
      status_text.empty()
      
    else:
      st.error("Introduce tu usuario y contraseña")