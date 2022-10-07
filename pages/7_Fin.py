# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session


#Set page context
st.set_page_config(
  page_title = "Fin",
  page_icon = "",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)

st.title('Fin')

w1 = st.button('Click para acabar')
if w1:
  st.balloons()
  st.snow()
  
  st.image('https://www.amigoszaragoza.com/cacheimagenes/fotosblog600-loch-ness.jpg')