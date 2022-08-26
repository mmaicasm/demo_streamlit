# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session

# Set page config and title
st.set_page_config(
  page_title = "Snowpark",
  page_icon = "🧊",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://developers.snowflake.com',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and the *amazing* Miguel Maicas"
  }
)
st.title('Snowpark')

# SESSION
st.subheader('Session')

text = """
connection_parameters = {
  "account": st.secrets["snowflake_account"],
  "user": st.secrets["snowflake_user"],
  "password": st.secrets["snowflake_password"],
  "role": "SYSADMIN",
  "database": "TEST_SNOWPARK",
  "schema": "STREAMLIT",
  "warehouse": "COMPUTE_WH"
}"""

st.code(text, language = "yaml")

text = """
from snowflake.snowpark.session import Session
def connect():
  session = Session.builder.configs(connection_parameters).create()
  return session
  
session = connect()
"""

st.code(text, language = "python")

# QUERY
st.subheader('Query')

text = """
df = session.table("TABLE_NAME").collect()
df = session.sql("SELECT * FROM TABLE_NAME").collect()
"""
st.code(text, language = "python")

text = """
df = session.sql("SELECT * FROM TABLE_NAME").collect()
df = session.sql("SELECT * FROM TABLE_NAME").show()
df = session.sql("SELECT * FROM TABLE_NAME").count()
df = session.sql("SELECT * FROM TABLE_NAME").to_pandas()
"""
st.code(text, language = "python")