# Streamlit
import streamlit as st
# Librerias necesarias
import time
from datetime import datetime
# Funciones necesarias 
from utils import db
from utils.functions import space


#Set page context
st.set_page_config(
  page_title = "Demo video",
  page_icon = "🧊",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)

# Video part
st.video(data = 'https://www.youtube.com/watch?v=E0jxqWoB8D8')
space(1)

# Comments part
conn = db.connect()
comments = db.collect_comments(conn)

# Template
COMMENT_TEMPLATE_MD = """{} - {}
> {}"""

# Funciones
@st.experimental_memo
def current_user(_session) -> str:
  df = _session.sql(f'SELECT current_user() as USER').collect()
  user = df['USER'][0]
  
  return user

with st.expander("💬 Abrir comentarios"):

  # Show comments
  st.write("**Comments:**")

  for index, entry in enumerate(comments.astype(str).itertuples()):
    st.markdown(COMMENT_TEMPLATE_MD.format(entry.NAME, entry.DATE, entry.COMMENT))

  space(2)

  # Insert comment
  st.write("**Añade tu comentario:**")
  with st.form("comment"):
    name = st.text_input(value = current_user(conn), label = "Nombre")
    comment = st.text_area(label = "Commentario")
    
    submit = st.form_submit_button("Añadir comentario")
    if submit:
      date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      result = db.insert(conn, name, date, comment)
      
      if result == 1:
        st.warning("No has introducido tu nombre")
      elif result == 2:
        st.error(f"Error en insert de la fila -> ('{name}', '{date}', '{comment}')")
      else:
        st.success("☝️ Tu comentario se ha añadido!")
        time.sleep(3)
        st.experimental_rerun()
