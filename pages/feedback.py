from PIL import Image

import requests
import streamlit as st
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import numpy as np

# from st_aggrid import AgGrid
im = Image.open("./assets/feedback.png")

st.set_page_config(
    page_title="Adidas Oliver Feedback",
    page_icon=im,
    layout='wide'
)



# def app():
# Variables
table_names = ['project-fermi.adidas.dab_omg_match_algo_viewer', 'project-fermi.adidas.feedback']
column_names = []
# users list
users = ["Nick", "Krish", "Dani", "Jane", "Julian", "Hassaan"]

st.title("Feedbacks")
user_name = st.selectbox("Select User", users)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


@st.cache(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

query = f"""
    SELECT  * FROM `{table_names[1]}`
    WHERE `user` = "{user_name}"
    """

get_data = st.button("Load Data")
if get_data:
    results = run_query(query=query)
    rows =  [r for r in results]
    st.dataframe(rows)