from datetime import datetime

import streamlit as st
#from vega_datasets import data

from utils import db
from utils.functions import space

COMMENT_TEMPLATE_MD = """{} - {}
> {}"""

st.set_page_config(layout="centered", page_icon="💬", page_title="Commenting app")

# Data visualisation part
st.title("💬 Commenting app")
space(1)

# Comments part
conn = db.connect()
comments = db.collect(conn)

with st.expander("💬 Open comments"):

    # Show comments
    st.write("**Comments:**")

    for index, entry in enumerate(comments.astype(str).itertuples()):
      st.markdown(COMMENT_TEMPLATE_MD.format(entry.NAME, entry.DATE, entry.COMMENT))

      #is_last = index == len(comments) - 1
      #is_new = "just_posted" in st.session_state and is_last
      #if is_new:
      #  st.success("☝️ Your comment was successfully posted.")

    space(2)

    # Insert comment
    st.write("**Añade tu comentario:**")
    form = st.form("comment")
    name = form.text_input("Nombre")
    comment = form.text_area("Commentario")
    submit = form.form_submit_button("Añadir comentario")

    if submit:
      date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      result = db.insert(conn, name, date, comment)
      st.write(result)
      
      #if "just_posted" not in st.session_state:
      #  st.session_state["just_posted"] = True
      if result == 1:
        st.warning("No has introducido tu nombre")
      elif result == 2:
        st.error(f"Error en insert de la fila -> ('{name}', '{date}', '{comment}')")
      else:
        st.success("☝️ Your comment was successfully posted")
      st.experimental_rerun()
