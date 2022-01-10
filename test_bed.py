import datetime
from logging import captureWarnings
from dataclasses import dataclass
from math import floor, ceil
import pandas as pd
import json
from PIL import Image

import requests
import streamlit as st
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import numpy as np

# from st_aggrid import AgGrid

# Variables
table_names = ['project-fermi.adidas.dab_omg_match_algo_viewer', 'project-fermi.adidas.feedback']
column_names = []

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)



def extract_bucket_and_fn(path: str):
    path = path.replace('gs://', '')
    path_elements = path.split('/')
    # path_elements = list(map(urllib.parse.quote, path_elements))
    fn = path_elements.pop()
    bucket = '/'.join(path_elements)
    bucket = bucket.replace(' ', '%20')
    return (bucket, fn)


def generate_download_signed_url_v4(bucket_name, blob_name):
    # https://docs.dolby.io/media-apis/docs/gcp-cloud-storage
    # If you don't have the authentication file set to an environment variable
    # See: https://cloud.google.com/docs/authentication/getting-started for more information
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="<PATH_TO_GCP_KEY>.json"

    # generate_download_signed_url_v4('dab_asset_iin/IIN/H23078-group1_300x50-736772/Misc/group1_300x50/MISC', 'talent_03.jpg')
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        url = blob.generate_signed_url(
            version="v4",
            # This URL is valid for 60 minutes
            expiration=datetime.timedelta(minutes=60),
            # Allow GET requests using this URL.
            method="GET",
        )
    except UnicodeError:
        print(blob_name)
        return ''
    return url


@dataclass
class SignedImage:
    uri: str = ''
    bucket: str = ''
    filename: str = ''
    signed_uri: str = ''

    def __post_init__(self):
        self.bucket, self.filename = extract_bucket_and_fn(self.uri)
        self.signed_uri = generate_download_signed_url_v4(self.bucket, self.filename)


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
            ORDER BY total_score ASC
    LIMIT 10
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
# with st.form("form_output"):
#     st.header("Get Data")
#
#     # Campaign Filter
#     campaign_option = st.selectbox(
#         'Campaign',
#         campaigns
#     )
#
#     # Weights Version Filter
#     weights_version = st.selectbox(
#         'Weights Version',
#         versions
#     )
#
#     # Slider for Total Score
#     total_score = st.slider(
#         label='Minimum Total Score ?',
#         min_value=min_val,
#         max_value=max_val,
#         step=step_size)
#
#     submitted = st.form_submit_button("Submit")
#     if submitted:
#         query_results = get_rows(campaign_option, weights_version, total_score)
#         # query_results = pd.DataFrame(query_results)
#
#         # Print All columns Names
#         # st.header("Columns from Table: ")
#         # for col in query_results.columns:
#         #     st.markdown(f"* {col}")
#
#         # st.dataframe(query_results[temp_col_names])
#
#         col1, col2, col3, col4 = st.columns([2, 2, 5, 3])
#         with col1:
#             st.header("OMG Asset")
#         with col2:
#             st.header("DAB Asset")
#         with col3:
#             st.header("Matching Info.")
#         # Iterate over each row and mapped it into columns
#         for r in query_results:
#             # print(r[0])
#             with col1:
#                 temp_image = Image.open(requests.get(SignedImage(r['uri_omg']).signed_uri, stream=True).raw)
#                 # temp_image = temp_image.resize((200,50))
#                 # st.image(temp_image)
#                 col1.image(temp_image, use_column_width=True)
#             with col2:
#                 temp_image = Image.open(requests.get(SignedImage(r['uri_dab']).signed_uri, stream=True).raw)
#                 # temp_image = temp_image.resize((200,50))
#                 # st.image(temp_image)
#                 col2.image(temp_image, use_column_width=True)
#
#             with col3:
#                 match_info = f"""
#                             * orientation_dab: {r['orientation_dab']}
#                             * orientation_omg: {r['orientation_omg']}
#                             * size_dab: {r['size_dab']}
#                             * size_omg: {r['size_omg']}
#                             * first_frac_rgb_dab: {r['first_frac_rgb_dab']}
#                             * first_frac_rgb_omg: {r['first_frac_rgb_omg']}
#                             * second_frac_rgb_dab: {r['second_frac_rgb_dab']}
#                             * second_frac_rgb_omg: {r['second_frac_rgb_omg']}
#                             * third_frac_rgb_dab:  {r['third_frac_rgb_dab']}
#                             * third_frac_rgb_omg: {r['third_frac_rgb_omg']}
#                             * property_score: {r['property_score']}
#                             * token_match: {r['token_match']}
#                             * size_score: {r['size_score']}
#                             * property: {r['property']}
#                             * label: {r['label']}
#                             * size: {r['size']}
#                             * total_score: {r['total_score']}
#                             * version: {r['version']}
#                             * campaign: {r['campaign']}
#                             """
#                 # st.markdown(match_info)
#                 # st.markdown("---")
#                 # st.write(r['total_score'])
#                 st.dataframe(pd.DataFrame(r.items()).astype(str))
#
#             with col4:
#                 add_button()

# -------------------------------

# *********************************************
st.header("Adidas Reporting 2.0 (Testbed)")
# ------- Without Form ----------
post_value_list = []
global feedback_text
feedback_text = []

global property_weights_feedback
global size_weights_feedback
global label_weights_feedback

property_weights_feedback = []
size_weights_feedback = []
label_weights_feedback = []


# -------------------------------
# Submit Feedback button
def submit_feedback(**data_dict):
    # print(feedback_text)
    # print(feedback_text[btn_key])
    # print(property_weights_feedback[data_dict['btn_key']])
    # For Development and Testing purpose
    # for key, value in data_dict.items():
    #     print(f"{key}: {value}")

    # Building up query to insert data to feedback table
    query = f"""
        INSERT INTO `{table_names[1]}` (algorithm_version, match_uri, source_uri, date, total_score, token_match, size_score, property_score, label, size, property, reason, property_feedback, size_feedback, label_feedback)
        VALUES (
                "{data_dict['algorithm_version']}",
                "{data_dict['match_uri']}",
                "{data_dict['source_uri']}",
                "{data_dict['date']}",
                {data_dict['total_score']},
                {data_dict['token_match']},
                {data_dict['size_score']},
                {data_dict['property_score']},
                {data_dict['label']},
                {data_dict['size']},
                {data_dict['property']},
                "Bad Score due to color mismatch",
                "{property_weights_feedback[data_dict['btn_key']]}",
                "{size_weights_feedback[data_dict['btn_key']]}",
                "{label_weights_feedback[data_dict['btn_key']]}")
        """
    # print(query)

    # RUN SQL
    results = run_query(query=query)
    print(results)


# Initialize Session state
if 'load_state' not in st.session_state:
    st.session_state.load_state = False
# Tuple for radio button options
options = ('Matched', 'Partially Matched', 'Unmatched')
# --------------------------
# Creating Container
with st.container():
    # Setting up 2 columns for input fields
    col1, col2 = st.columns([6,6])
    # Column 1: For Campaign Option
    with col1:
        # Campaign Filter
        campaign_option = st.selectbox(
            'Campaign',
            campaigns
        )
    # Column 2: For Weights Version
    with col2:
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
    # Button to get Data
    load = st.button("Load Data")

    if (load or st.session_state.load_state):
        st.session_state.load_state = True
        feedback_text = []
        post_value_list = []
        query_results = get_rows(campaign_option, weights_version, total_score)

        # query_results = pd.DataFrame(query_results)

        # Print All columns Names
        # st.header("Columns from Table: ")
        # for col in query_results.columns:
        #     st.markdown(f"* {col}")

        # st.dataframe(query_results[temp_col_names])

        # Create 4 Columns on Query Response data display
        col1, col2, col3, col4 = st.columns([2,2,5,3])
        # Setting up headers against each column
        with col1:
            st.header("OMG Asset")
        with col2:
            st.header("DAB Asset")
        with col3:
            st.header("Matching Info.")
        with col4:
            st.header("Actions")
        # Iterate over each row and mapped it into columns
        for r in query_results:
            # print(r[0])
            col1, col2, col3, col4 = st.columns([2, 2, 5, 3])
            # URI OMG Image Column
            with col1:
                temp_image = Image.open(requests.get(SignedImage(r['uri_omg']).signed_uri, stream=True).raw)
                # temp_image = temp_image.resize((200,50))
                # st.image(temp_image)
                st.image(temp_image)
            # URI DAB Image Column
            with col2:
                temp_image = Image.open(requests.get(SignedImage(r['uri_dab']).signed_uri, stream=True).raw)
                # temp_image = temp_image.resize((200,50))
                # st.image(temp_image)
                st.image(temp_image)
            # Other Meta Info. Column
            with col3:
                match_info = f"""
                            * orientation_dab: {r['orientation_dab']}
                            * orientation_omg: {r['orientation_omg']}
                            * size_dab: {r['size_dab']}
                            * size_omg: {r['size_omg']}
                            * first_frac_rgb_dab: {r['first_frac_rgb_dab']}
                            * first_frac_rgb_omg: {r['first_frac_rgb_omg']}
                            * second_frac_rgb_dab: {r['second_frac_rgb_dab']}
                            * second_frac_rgb_omg: {r['second_frac_rgb_omg']}
                            * third_frac_rgb_dab:  {r['third_frac_rgb_dab']}
                            * third_frac_rgb_omg: {r['third_frac_rgb_omg']}
                            * property_score: {r['property_score']}
                            * token_match: {r['token_match']}
                            * size_score: {r['size_score']}
                            * property: {r['property']}
                            * label: {r['label']}
                            * size: {r['size']}
                            * total_score: {r['total_score']}
                            * version: {r['version']}
                            * campaign: {r['campaign']}
                            """
                # st.markdown(match_info)
                # st.markdown("---")
                # st.write(r['total_score'])

                temp_df = pd.DataFrame.from_dict(r, orient ='index')
                # temp_df.drop('property', inplace=True, axis=1)
                temp_df = temp_df.iloc[14:24]
                temp_df = temp_df.T
                temp_df = temp_df[['total_score', 'size', 'property', 'label']]
                # temp_df = np.round(temp_df, decimals=3)
                st.write(np.round(temp_df, decimals=2))
                # st.json(r)

            # Feedback Button Column
            with col4:
                btn_key = query_results.index(r)
                # print(btn_key)
                temp_data_dict = {
                    'algorithm_version' : r['version'],
                    'match_uri' : r['uri_dab'],
                    'source_uri' : r['uri_omg'],
                    'date' : datetime.datetime.now(),
                    'total_score': int(r['total_score']),
                    'size_score': int(r['size_score']),
                    'property': int(r['property']),
                    'property_score': int(r['property_score']),
                    'label': int(r['label']),
                    'size': int(r['size']),
                    'token_match': int(r['token_match']),
                    'btn_key': btn_key
                }
                post_value_list.append(temp_data_dict)
                feedback_text.append(btn_key)

                # Expander will display input text field for feedback
                with st.expander("Feedback"):

                    # with st.form(str(btn_key)):
                    #     feedback_text[btn_key] = st.text_input("Reason:", key=btn_key,
                    #                                            placeholder="Unmatched images etc.")
                    #     print(f"feedback: {feedback_text[btn_key]}")
                    #     st.form_submit_button(label="Submit", on_click=submit_feedback, kwargs=(post_value_list[btn_key]))
                    # with st.form(str(btn_key)):
                    # feedback_text[btn_key] = st.text_input("Reason:", key=btn_key, placeholder="Unmatched images etc.")
                    property_weights_feedback.append(st.radio("Property Weights", options, key='property'+str(btn_key)))
                    size_weights_feedback.append(st.radio("Size Weights", options, key='size'+str(btn_key)))
                    label_weights_feedback.append(st.radio("Label Weights", options, key='label'+str(btn_key)))

                    # post_value_list[btn_key]['reason'] = feedback_text
                    submitted = st.button("Submit", key=btn_key, on_click=submit_feedback, kwargs=(post_value_list[btn_key]))


# *********************************************