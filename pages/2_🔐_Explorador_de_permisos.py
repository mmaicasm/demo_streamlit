# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
# Librerias necesarias
import pandas as pd
import graphviz
import os
import time
# Funciones necesarias 
from utils import db

# Formato de página
st.set_page_config(
  page_title = "Explorador de permisos",
  page_icon = ":closed_lock_with_key:",
  layout = "wide",
  initial_sidebar_state = "auto",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app made by Miguel Maicas and powered by Snowpark for Python and Streamlit"
  }
)
st.title('Explorador de permisos')
#st.header("Cabecera")
#st.subheader('Subcabecera')
st.markdown('Herramienta para analizar permisos a nivel de esquema/tabla o de usuario')

# Necesario para exportar
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin/'

# Conexión
session = db.connect()


# Funciones
@st.experimental_memo
def db_list():
  session.sql(f'SHOW DATABASES').collect()
  df = session.sql('SELECT "name" as DATABASE FROM table(result_scan(last_query_id()))').to_pandas()
  ls = df['DATABASE'].to_list()
  
  return ls

@st.experimental_memo
def schema_list(db):
  session.sql(f'SHOW SCHEMAS IN {db}').collect()
  df = session.sql('SELECT "name" as SCHEMA FROM table(result_scan(last_query_id()))').to_pandas()
  ls = df['SCHEMA'].to_list()
  ls.insert(0, '') # Valor inicial vacio
  
  return ls

@st.experimental_memo
def table_list(db, schema):
  if schema:
    session.sql(f'SHOW TABLES IN SCHEMA {db}.{schema}').collect()
    df = session.sql('SELECT "name" as OBJECT FROM table(result_scan(last_query_id()))').to_pandas()
    ls = df['OBJECT'].to_list()
    ls.insert(0, '') # Valor inicial vacio
  else:
    ls = ['']
  
  return ls

# Función principal de PERMISOS
def permisos(db, schema, object):
  session.sql(f"CALL TEST_SNOWPARK.SNOWPARK.ARBOL_PERMISOS('{db}', '{schema}', '{object}')").collect()

  # Cargamos los gráficos
  df_roles = session.table('TEST_SNOWPARK.SNOWPARK.TB_DF_ROLES')
  df_users = session.table('TEST_SNOWPARK.SNOWPARK.TB_DF_USERS')
  
  # Gráfico de Roles
  v = (df_roles['ROLE_NAME_OF'] + ' ' + df_roles['GRANTEE_NAME']).to_list() # Vectorization
  graph = graphviz.Digraph(node_attr= {'shape': 'box', 'color': 'lightblue2', 'style': 'filled'}, format = 'svg')
  for n in v:
    x = n.split()
    if x[0] == x[1]:
      graph.node(x[0])
    else:
      graph.edge(x[1], x[0])
      
  # Gráfico de Usuarios
  v_users = (df_users['ROLE_NAME_OF'] + ' ' + df_users['GRANTEE_NAME']).to_list() # Vectorization
  with graph.subgraph(node_attr = {'shape': 'record', 'fillcolor' : 'lightgrey'}) as subgraph:
    role = v_users[0].split()[0]
    ls_users = []
    for n in v_users:
      x = n.split()
      if x[0] != role:
        l = '{' + ' | '.join(ls_users) + '}'
        subgraph.node('Usuarios_' + role, label = l, rankdir = 'LR')
        subgraph.edge(role, 'Usuarios_' + role)
        role = x[0]
        ls_users = [x[1]]
      else:
        ls_users.append(x[1])
    subgraph.node('Usuarios_' + role, label = l, rankdir = 'LR')
    subgraph.edge(role, 'Usuarios_' + role)
  
  # Mostrar el gráfico
  st.graphviz_chart(graph)
  
  # Export
  if object:
    export = st.download_button(data = graph.pipe(format='svg'), file_name = f'diagrama_permisos - {object}.svg', mime = 'image/svg+xml', label = 'Exportar')
  else:
    export = st.download_button(data = graph.pipe(format='svg'), file_name = f'diagrama_permisos - {schema}.svg', mime = 'image/svg+xml', label = 'Exportar')


# Columnas
db, schema, object = st.columns(3)

# Widget de Database
with db:
  w1 = st.selectbox(options = db_list(), label = 'Database')

# Widget de Schema
with schema:
  w2 = st.selectbox(options = schema_list(w1), label = 'Schema')

# Widget de Table
with object:
  w3 = st.selectbox(options = table_list(w1, w2), help = 'Dejar vacio si se quiere buscar a nivel de esquema', label = 'Table')

# Botón para generar permisos
w4 = st.button(key = 'w41', label = 'Generar')
if w4:
  if w2:
    if w3:
      with st.spinner(f'Generando diagrama de permisos de la tabla {w3}'):
        permisos(w1, w2, w3)
    else:
      with st.spinner(f'Generando diagrama de permisos del esquema {w2}'):
        permisos(w1, w2, '')
  else:
    st.warning('Escoge un esquema')