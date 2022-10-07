# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session


#Set page context
st.set_page_config(
  page_title = "Futuro",
  page_icon = "",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)

st.title('Futuro')

# VIDEO
st.video(data = 'https://www.youtube.com/watch?v=e8kZQDKeNwk', start_time = 68)