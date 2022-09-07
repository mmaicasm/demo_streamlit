# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col, lit, in_
# Librerias necesarias
import pandas as pd
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
# Funciones necesarias 
from utils import db

# Formato de página
st.set_page_config(
  page_title = "Buscador de objetos",
  page_icon = "🔎",
  layout = "wide",
  initial_sidebar_state = "auto",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app made by Miguel Maicas and powered by Snowpark for Python and Streamlit"
  }
)
st.title('Buscador de objetos')
#st.header("Cabecera")
#st.subheader('Subcabecera')
st.markdown('Herramienta para buscar y analizar objetos')
  
# Variables
i1 = 0
i2 = 0

# Funciones
@st.experimental_memo
def search_object(type, object):
  df = pd.DataFrame()
  # Tablas
  if 'TABLE' in type:
    session.sql(f"SHOW TERSE TABLES LIKE '%{object}%' IN ACCOUNT").collect()
    df1 = session.sql('SELECT "kind" as KIND, "database_name" as DATABASE_NAME, "schema_name" as SCHEMA_NAME, "name" as NAME  FROM table(result_scan(last_query_id()))').to_pandas()
    df = pd.concat([df, df1], ignore_index = True)
    
  # Vistas
  if 'VIEW' in type:
    session.sql(f"SHOW TERSE VIEWS LIKE '%{object}%' IN ACCOUNT").collect()
    df2 = session.sql('SELECT "kind" as KIND, "database_name" as DATABASE_NAME, "schema_name" as SCHEMA_NAME, "name" as NAME  FROM table(result_scan(last_query_id()))').to_pandas()
    df = pd.concat([df, df2], ignore_index = True)
  
  # Tasks
  if 'TASK' in type:
    session.sql(f"SHOW TERSE TASKS LIKE '%{object}%' IN ACCOUNT").collect()
    df3 = session.sql(f'SELECT \'TASK\' as KIND, "database_name" as DATABASE_NAME, "schema_name" as SCHEMA_NAME, "name" as NAME  FROM table(result_scan(last_query_id()))').to_pandas()
    df = pd.concat([df, df3], ignore_index = True)
  
  # Streams
  if 'STREAM' in type:
    session.sql(f"SHOW TERSE STREAMS LIKE '%{object}%' IN ACCOUNT").collect()
    df4 = session.sql(f'SELECT \'STREAM\' as KIND, "database_name" as DATABASE_NAME, "schema_name" as SCHEMA_NAME, "name" as NAME  FROM table(result_scan(last_query_id()))').to_pandas()
    df = pd.concat([df, df4], ignore_index = True)
  
  return df

# Conexión
session = db.connect()

# Widgets
w1 = st.multiselect(options = ['TABLE', 'VIEW', 'TASK', 'STREAM'], help = 'Cada tipo añadido reduce el rendimiento de la consulta', label = 'Tipos')
w2 = st.text_input(placeholder = 'Nombre del objeto', label = 'Objeto')

# Solo refrescar si hay texto
if w1 and w2:
  # Buscar objeto
  with st.spinner(f'Buscando en Snowflake...'):
    df = search_object(w1, w2)

  # Añadimos control por selección individual
  gb = GridOptionsBuilder.from_dataframe(df)
  gb.configure_selection(selection_mode = "single", use_checkbox = False)
  #gb.configure_grid_options(domLayout = 'autoHeight') HACE COSAS RARAS PARA MUCHAS FILAS
  gridOptions = gb.build()
  
  grid_return = AgGrid(df, 
    gridOptions = gridOptions, 
    enable_enterprise_modules = True, 
    allow_unsafe_jscode = True, 
    update_mode = GridUpdateMode.SELECTION_CHANGED)
  selected_row = grid_return['selected_rows']
  
  # Procesamos la selección
  if selected_row:
    # Refrescamos los parámetros de la selección
    type = selected_row[0]['KIND']
    db = selected_row[0]['DATABASE_NAME']
    schema = selected_row[0]['SCHEMA_NAME']
    name = selected_row[0]['NAME']
    
    # Caso TABLA/VISTA
    if type in ('TABLE', 'VIEW'):
      tab1, tab2, tab3 = st.tabs(['Columns', 'Keys', 'Preview'])
      
      # Extraer info
      session.sql(f'DESC {type} {db}.{schema}.{name}').collect()
      ssql = 'SELECT "name" as NAME, "type" as TYPE, "kind" as KIND, "null?" as NULLEABLE, "default" as DEFAULT, "primary key" as PRIMARY_KEY, "unique key" as UNIQUE_KEY, '
      ssql += '"check" as "CHECK", "expression" as EXPRESSION, "comment" as COMMENT, "policy name" as POLICY_NAME FROM table(result_scan(last_query_id()))'
      df_desc = session.sql(ssql).to_pandas()
      
      # Pestaña de columnas
      with tab1:
        df1 = df_desc[df_desc['KIND'] == 'COLUMN']
        df1 = df1[['NAME', 'TYPE', 'NULLEABLE', 'DEFAULT', 'COMMENT', 'POLICY_NAME']]
        
        AgGrid(df1)
      
      # Pestaña de claves
      with tab2:
        df2 = df_desc[(df_desc['PRIMARY_KEY'] != 'N') | (df_desc['UNIQUE_KEY'] != 'N')]
        df2 = df2[['NAME', 'TYPE', 'PRIMARY_KEY', 'UNIQUE_KEY']]
        
        if df2.empty:
          st.write('No hay claves que mostrar')
        else:
          AgGrid(df2)
          
      # Pestaña de preview
      with tab3:
        df3 = session.sql(f'SELECT * FROM {db}.{schema}.{name} LIMIT 12').to_pandas()
        
        AgGrid(df3)
        
    # Caso TASK
    elif type == 'TASK':
      tab1, tab2, tab3 = st.tabs(['Info', 'Definition', 'Condition'])
      
      # Extraer info
      session.sql(f'DESC {type} {db}.{schema}.{name}').collect()
      ssql = 'SELECT "name" as NAME, "state" as STATE, "schedule" as SCHEDULE, "warehouse" as WAREHOUSE, "predecessors" as PREDECESSORS, "allow_overlapping_execution" as ALLOW_OVERLAPPING_EXECUTION, '
      ssql += '"error_integration" as ERROR_INTEGRATION, "definition" as DEFINITION, "condition" as CONDITION, "comment" as COMMENT, "owner" as OWNER FROM table(result_scan(last_query_id()))'
      df_desc = session.sql(ssql).to_pandas()
      
      # Pestaña de info
      with tab1:
        df1 = df_desc[['NAME', 'STATE', 'SCHEDULE', 'WAREHOUSE', 'PREDECESSORS', 'ALLOW_OVERLAPPING_EXECUTION', 'ERROR_INTEGRATION', 'COMMENT', 'OWNER']]
        df1 = pd.melt(df1)
        
        AgGrid(df1)
      
      # Pestaña de definición
      with tab2:
        text2 = df_desc['DEFINITION'][0]
        # Formatear el código
        text2 = text2.replace(';', ';\n').replace(' AS ', ' \nAS \n', 1)
        text2 = text2.replace(',', ',\n').replace('SELECT ', 'SELECT \n ')
        text2 = text2.replace( 'FROM ', '\nFROM ').replace(' GROUP BY ', '\nGROUP BY ') + ';'
        
        st.code(text2, language = 'sql')
        
      # Pestaña de condición
      with tab3:
        text3 = df_desc['CONDITION'][0]
        
        st.code(text3, language = 'sql')