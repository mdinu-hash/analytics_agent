import streamlit as st
from db_agent_v1 import graph, create_config,objects_documentation,database_content,sql_dialect

st.set_page_config(page_title="DB Agent Demo", page_icon="🤖", layout="wide")
st.title("🤖 Database Query Agent Demo")
st.markdown("Ask questions about the feedback database and get insights!")

with st.sidebar:
    st.header("Database Info")
    st.markdown("""
    **Available Data:**
    - 413k feedback records (2002-2023)
    - 8,145 products
    - 12 companies (Apple, Samsung, Sony, Nike, etc.)
    - Ratings from 1-5 stars
    """)

st.button("Reset", type="primary")
if st.button("Say hello"):
    st.write("Why hello there")
else:
    st.write("Goodbye")