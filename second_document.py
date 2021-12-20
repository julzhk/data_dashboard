import datetime
from logging import captureWarnings
from dataclasses import dataclass
from math import floor, ceil
import pandas as pd
import json

import streamlit as st
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# Variables
table_names = ['project-fermi.adidas.dab_omg_match_algo_viewer']
column_names = []

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

# Get Distinct values from Columns
def get_distinct_values_frm_cols(table_name,column_name):
    query = f"""
    SELECT  DISTINCT {column_name} FROM `{table_name}`
    """
    results = run_query(query=query)
    return [r[column_name] for r in results]


# Get Rows based on filter
def get_rows(campaign_option, weights_version, total_score):
    query = f"""
    SELECT  * FROM `{table_names[0]}`
    WHERE campaign = "{campaign_option}"
        AND version = "{weights_version}"
        AND total_score >= {total_score}
            ORDER BY total_score DESC
    LIMIT 50
    """
    results = run_query(query=query)
    return [r for r in results]

# Get Step Size for Sliders Function
def round_up(num, divisor):
    return ceil(num / divisor) * divisor

def round_down(num, divisor):
    return floor(num / divisor) * divisor

def get_step_size(biggest, number_of_steps, smallest, step_units):
    # get the params for the slider - round down/up to get the whole range
    # and set a step size to the nearest 5 to give <number_of_steps> steps
    # ie 10 seems sensible
    return round_up(((biggest - smallest) // number_of_steps), step_units)

def get_max_min_range(column_name, number_of_steps=10, step_units=5):
    query = f"""
    SELECT MAX({column_name}), MIN({column_name})
        FROM `{table_names[0]}`
            """
    biggest, smallest = run_query(query=query)[0].values()
    print(biggest)
    step_size = get_step_size(biggest, number_of_steps, smallest, step_units)
    return round_up(biggest, step_size), round_down(smallest, step_size), step_size

# Setting page width
st.set_page_config(layout="wide")


# -----------------------------------
# Creating Form Filters Variables
# Get All Campaigns Names
column_names.append("campaign")
campaigns = get_distinct_values_frm_cols(table_names[0],column_names[0])
# Get All Campaigns Names

# Get All Distinct Versions
column_names.append("version")
# print(column_names[-1])
versions = get_distinct_values_frm_cols(table_names[0], column_names[-1])
# Get All Distinct Versions

# Get MAX and MIN Total Score
column_names.append("total_score")
min_val, max_val, step_size = get_max_min_range(column_names[-1])
print(max_val)
# get_max_min_range()
# Get All Distinct Versions


# -----------------------------------


# -----------------------------------
temp_col_names = column_names
temp_col_names.append('uri_dab')
temp_col_names.append('uri_omg')
temp_col_names.append('property_score')
temp_col_names.append('token_match')
temp_col_names.append('size_score')
# Main Form
with st.form("form_output"):
    st.header("Get Data")

    # Campaign Filter
    campaign_option = st.selectbox(
        'Campaign',
        campaigns
    )

    # Weights Version Filter
    weights_version = st.selectbox(
        'Weights Version',
        versions
    )

    # Slider for Total Score
    total_score = st.slider(
        label='Minimum Total Score ?',
        min_value=min_val,
        max_value=max_val,
        step=step_size)

    submitted = st.form_submit_button("Submit")
    if submitted:
        query_results = get_rows(campaign_option, weights_version, total_score)
        query_results = pd.DataFrame(query_results)

        # Print All columns Names
        # st.header("Columns from Table: ")
        # for col in query_results.columns:
        #     st.markdown(f"* {col}")

        st.dataframe(query_results[temp_col_names])