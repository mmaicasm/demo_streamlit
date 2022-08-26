import streamlit as st

def space(num_lines = 1):
  """Adds empty lines to the Streamlit app."""
  for _ in range(num_lines):
    st.write("")