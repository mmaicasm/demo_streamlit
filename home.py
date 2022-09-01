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
st.subheader('Conexión a Snowflake utilizando Snowpark')

# Recuperar singletons
if 'role' in st.session_state:
  session = db.connect()
  role_list = db.available_roles(session)
  role = st.sidebar.selectbox(options = role_list, index = st.session_state['role_index'], label= 'Rol')

# Widget manual
with st.form(key = "login"):
  
  entorno = st.radio(options = ['PRO','PRE','DEV'], disabled = True, label = 'Entorno')
  user = st.text_input(placeholder = 'usuario@hiberus.com', label = 'Usuario')
  password = st.text_input(type = 'password', label = 'Contraseña')
  
  login = st.form_submit_button("Conectar")
  if login:
    if user and password:
      
      # Crear sesión
      with st.spinner('Conectando a Snowflake...'):
        session = db.connect()
      
      # Ocultar índices de tablas
      hide_table_row_index = """
        <style>
        thead tr th:first-child {display:none}
        tbody th {display:none}
        </style>
        """
      st.markdown(hide_table_row_index, unsafe_allow_html = True)
      
      # Informar conexión correcta
      st.success('Sesión confirmada!')
      st.snow()
      
      # Obtener roles disponibles
      role_list = db.available_roles(session)
      
      # Widget en barra lateral
      #role = st.sidebar.selectbox(options = role_list, index = st.session_state['role_index'], on_change = db.refresh_role, args=(new_project, ), label= 'Rol')
      role = st.sidebar.selectbox(options = role_list, index = st.session_state['role_index'], label= 'Rol')
      st.session_state['role'] = role
      st.session_state['role_index'] = role_list.index(role)
      
      # Mostrar parámetros de la sesión
      st.write('Parámetros de la sesión:')
      st.table(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
      
    else:
      st.error("Introduce tu usuario y contraseña")