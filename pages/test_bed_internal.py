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
import matplotlib.pyplot as plt
from streamlit_echarts import st_echarts
from PIL import Image
im = Image.open("./assets/protocol_testbed_icon.png")

st.set_page_config(
    page_title="Adidas Oliver TestBed",
    page_icon=im,
    layout='wide'
)


# def app():
global temp_campaign
temp_campaign = ""

global temp_algo
temp_algo = ""

# --------------------------------------------
st.header("Oliver Internal Testbed")
with st.container():
    # Dummy Dataframe and Bar Chart
    algo_version_1 = [200, 30, 40, 420]
    algo_version_2 = [150, 351, 400, 395]
    index = ['0-25', '26-50', '51-75',
             '76-100']
    df = pd.DataFrame(
        {'Algorithm V1': algo_version_1,
         'Algorithm V2': algo_version_2},
        index=index)
    options = {
        "tooltip": {
            "trigger": 'axis',
            "axisPointer": {
                "type": 'shadow'
            }
        },
        "legend": {
            "data": ['Algorithm V1', 'Algorithm V2']
        },
        "toolbox": {
            "show": True,
            "orient": 'vertical',
            "left": 'right',
            "top": 'center',
            "feature": {
                "mark": {"show": True},
                "dataView": {"show": True,
                             "readOnly": False},
                "magicType": {"show": True,
                              "type": ['line',
                                       'bar',
                                       ]},
                "restore": {"show": True},
                "saveAsImage": {"show": True}
            }
        },
        "xAxis": [
            {
                "type": 'category',
                "axisTick": {"show": False},
                "data": index,
                "axisLabel": {
                    "interval": 0,
                },
                "name": 'Score Range',
                "nameLocation": 'middle',
                "nameGap": 30
            }
        ],
        "yAxis": [
            {
                "type": 'value',
                "name": 'No. of Assets',
                "nameLocation": 'middle',
                "nameGap": 40
            }
        ],
        "series": [
            {
                "name": 'Algorithm V1',
                "type": 'bar',
                "barGap": 0,
                "label": "labelOption",
                "emphasis": {
                    "focus": 'series'
                },
                "data": algo_version_1
            }
            ,
            {
                "name": 'Algorithm V2',
                "type": 'bar',
                "label": "labelOption",
                "emphasis": {
                    "focus": 'series'
                },
                "data": algo_version_2
            }
        ]
    }
    st_echarts(options=options)
    col1,col2 = st.columns([6,6])
    with col1:
        # We cannot add width to year so we create another list
        # indices = np.arange(len(index))
        # width = 0.20
        # # Plotting
        # plt.bar(indices, algo_version_1, width=width,
        #         label="Algorithm V1")
        # xlocs, xlabs = plt.xticks()
        # for i in range(len(algo_version_1)):
        #     plt.annotate(str(algo_version_1[i]), xy=(
        #         indices[i], algo_version_1[i]),
        #                  ha='center', va='bottom')
        # for i in range(len(algo_version_2)):
        #     plt.annotate(str(algo_version_2[i]), xy=(
        #         indices[i] + 0.20, algo_version_2[i]),
        #                  ha='center', va='bottom')
        #
        # # Offsetting by width to shift the bars to the right
        # plt.bar(indices + width, algo_version_2,
        #         width=width,
        #         label="Algorithm V2")
        # # Displaying year on top of indices
        # plt.xticks(ticks=indices, labels=index)
        # plt.xlabel("Total Score Range")
        # plt.ylabel("Assets Count")
        # plt.legend()
        # st.pyplot(plt)
        # Summary Data frame
        algorithms = {
            'Algorithm V1': 'Feature weighted object penalised',
            'Algorithm V2': 'Feature weighted label adjusted',
            'Algorithm V3': 'Feature weighted object added',
            'Algorithm V4': 'Feature weighted'
        }
        algorithms_df = pd.DataFrame(
            [algorithms])
        algorithms_df.reset_index()
        st.table(algorithms_df)
    with col2:
        # Dummy Dataframe and Bar Chart
        algos_version_1 = [200, 30, 40, 420, 690]
        algos_version_2 = [150, 351, 400, 395, 1296]
        indexes = ['0-25', '26-50', '51-75',
                 '76-100', 'Total']
        df_table = pd.DataFrame(
            {'Algorithm V1': algos_version_1,
             'Algorithm V2': algos_version_2},
            index=indexes)
        st.table(df_table)
# --------------------------------------------

st.markdown("---")
# Variables
table_names = ['project-fermi.adidas.dab_omg_match_algo_viewer', 'project-fermi.adidas.feedback']
column_names = []
# users list
users = ["--", "Nick", "Krish", "Dani", "Jane", "Julian", "Hassaan"]
list.sort(users)
user_name = ""

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

# Get ALL Assets Data Frame
def get_df_assets(table_name):
    query = f"""
    SELECT * FROM `{table_name}`
    """
    results = run_query(query=query)
    return results

# Get Distinct values from Columns
def get_distinct_values_frm_cols(table_name,column_name):
    query = f"""
    SELECT  DISTINCT {column_name} FROM `{table_name}`
    """
    results = run_query(query=query)
    return [r[column_name] for r in results]

# Get Distinct values from Columns
def get_distinct_dab(table_name, column_name, campaign_option, algo_option):

    campaign_option = "like '%'" if campaign_option == 'All' else "='" + campaign_option + "'"
    algo_option = "like '%'" if algo_option == 'All' else "='" + algo_option + "'"

    query = f"""
    SELECT  DISTINCT {column_name} FROM `{table_name}`
    WHERE campaign {campaign_option}
    AND version {algo_option}
    """
    results = run_query(query=query)
    return [r[column_name] for r in
            results]

# Get All Rows
def get_all_rows():

    sort_order_data = "ASC" if sort_order == 'Ascending' else "DESC"

    query = f"""
    SELECT  * FROM `{table_names[0]}`
    ORDER BY total_score {sort_order_data}
    LIMIT 10
    """
    results = run_query(query=query)
    return [r for r in results]


# Get Rows based on filter
def get_rows(campaign_option, weights_version, text_label):
    campaign_option = "like '%'"  if campaign_option == 'All' else "='"+campaign_option+"'"
    weights_version = "like '%'" if weights_version == 'All' else "='"+weights_version+"'"
    dab_filename = "like '%'" if DAB_ASSET_NAME == 'All' else "='"+DAB_ASSET_NAME+"'"
    total_score = "BETWEEN 0 AND 100"
    sort_order_data = "ASC" if sort_order == 'Ascending' else "DESC"
    text_label = "like '%'" if text_label == str('0.0') else "=" + str(text_label)

    # Check for selected score range
    if score_selector == 'All':
        total_score = "BETWEEN 0 AND 100000"
    elif score_selector == '0-10':
        total_score = "BETWEEN 0 AND 10"
    elif score_selector == '11-20':
        total_score = "BETWEEN 11 AND 20"
    elif score_selector == '21-30':
        total_score = "BETWEEN 21 AND 30"
    elif score_selector == '31-40':
        total_score = "BETWEEN 31 AND 40"
    elif score_selector == '41-50':
        total_score = "BETWEEN 41 AND 50"
    elif score_selector == '51-60':
        total_score = "BETWEEN 51 AND 60"
    elif score_selector == '61-70':
        total_score = "BETWEEN 61 AND 70"
    elif score_selector == '71-80':
        total_score = "BETWEEN 71 AND 80"
    elif score_selector == '81-90':
        total_score = "BETWEEN 81 AND 90"
    elif score_selector == '91-99':
        total_score = "BETWEEN 91 AND 99"
    elif score_selector == '100':
        total_score = "=100"

    query = f"""
    SELECT  * FROM `{table_names[0]}`
    WHERE 
    campaign {campaign_option}
    AND version {weights_version}
    
    AND uri_dab {dab_filename}
    AND total_score {total_score}

    ORDER BY total_score {sort_order_data}
    LIMIT 15
    """
    # AND
    # property = {weight_property}
    # AND
    # label = {weight_label}
    # AND
    # size = {weight_size}
    # AND text {text_label}
    print(query)
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

def get_weights_max_min_range(column_name, number_of_steps=1, step_units=0.01):
    query = f"""
    SELECT MAX({column_name}), MIN({column_name})
        FROM `{table_names[0]}`
            """
    biggest, smallest = run_query(query=query)[0].values()
    print(biggest)
    step_size = get_step_size(biggest, number_of_steps, smallest, step_units)
    return biggest, smallest, step_size

# Setting page width
# st.set_page_config(layout="wide")

# -----------------------------------
# Creating Form Filters Variables
# Get All Campaigns Names
column_names.append("campaign")
campaigns = get_distinct_values_frm_cols(table_names[0],column_names[0])
campaigns.append('All')
campaigns = ['UB22 March Drop', 'Bra Revolution']
campaigns.reverse()
# Get All Campaigns Names

# Get All Distinct Versions
column_names.append("version")
# print(column_names[-1])
versions = get_distinct_values_frm_cols(table_names[0], column_names[-1])
# versions = [
#     'feature weighted object added top 5 crop all',
#     'feature weighted object added top 5 all omg',
#     'feature weighted object added top 5 all',
#     'feature weighted object added top 10 all',
#     'text0.3 color0.1 label0.3 top 10',
#     'text0.5 color0.1 label0.2 top 10',
#     'text0.3 color0.2 label0.2 top 10',
#     'text0.4 color0.3 label0.2 top 10',
#     'text0.1 color0.5 label0.3 top 10'
# ]
versions.append('All')
versions = ['beblid_4000']
versions.reverse()
# Get All Distinct Versions

# Get All Distinct Asset Name
# column_names.append("file_name_dab")
column_names.append("uri_dab")
# print(column_names[-1])
global dab_asset
dab_asset =  get_distinct_values_frm_cols(table_names[0], column_names[-1])
dab_asset.append('All')
dab_asset.reverse()

# Get All Distinct Size Weights
column_names.append("size")
# print(column_names[-1])
size_min, size_max, size_step_size = get_weights_max_min_range(column_names[-1])

# Get All Distinct label Weights
column_names.append("label")
# print(column_names[-1])
label_min, label_max, label_step_size = get_weights_max_min_range(column_names[-1])

# Get All Distinct Property Weights
column_names.append("property")
# print(column_names[-1])
property_min, property_max, label_step_size = get_weights_max_min_range(column_names[-1])


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

# *********************************************
# ------- Without Form ----------
post_value_list = []
global feedback_text
feedback_text = []

global property_weights_feedback
global size_weights_feedback
global label_weights_feedback
global manual_feedback

global change
change = True;

property_weights_feedback = []
size_weights_feedback = []
label_weights_feedback = []
manual_feedback = []

# -------------------------------
# Submit Feedback button
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600)
def submit_feedback(**data_dict):
    # print(feedback_text)
    # print(feedback_text[btn_key])
    # print(property_weights_feedback[data_dict['btn_key']])
    # For Development and Testing purpose
    # for key, value in data_dict.items():
    #     print(f"{key}: {value}")

    # Building up query to insert data to feedback table
    query = f"""
        INSERT INTO `{table_names[1]}` (
        algorithm_version, 
        match_uri, 
        source_uri, 
        date, 
        total_score, 
        token_match, 
        size_score, 
        property_score, 
        reason, 
        property_feedback, 
        size_feedback, 
        label_feedback,
        user,
        label_weight,
        size_weight,
        property_weight,
        manual_feedback)
        VALUES (
                "{data_dict['algorithm_version']}",
                "{data_dict['match_uri']}",
                "{data_dict['source_uri']}",
                "{data_dict['date']}",
                {data_dict['total_score']},
                {data_dict['token_match']},
                {data_dict['size_score']},
                {data_dict['property_score']},
                "Bad Score due to color mismatch",
                "{property_weights_feedback[data_dict['btn_key']]}",
                "{size_weights_feedback[data_dict['btn_key']]}",
                "{label_weights_feedback[data_dict['btn_key']]}",
                "{user_name}",
                {weight_size},
                {weight_label},
                {weight_property},
                "{manual_feedback[data_dict['btn_key']]}"
                )
        """
    # print(query)
    print(property_weights_feedback[btn_key])
    print(size_weights_feedback[btn_key])
    print(label_weights_feedback[btn_key])

    # RUN SQL
    results = run_query(query=query)
    print(results)

def get_dab_asset(campaign_option, algo_option):
    return get_distinct_dab(table_names[0], column_names[2], campaign_option, algo_option)

# Initialize Session state
if 'load_state' not in st.session_state:
    st.session_state.load_state = False
# Tuple for radio button options
options = ('Matched', 'Partially Matched', 'Unmatched')
options_sizes = ('Matched','Unmatched')
# --------------------------

# Creating Container
with st.container():
    # Filter type for Records
    col1, col2 = st.columns([6,6])
    # Col1 for filter type
    with col1:
        # Campaign Filter
        user_name = st.selectbox(
            'Select your user for feedback',
            users
        )
    with col2:
        filter_type = st.selectbox('Show Record: ', ['All', 'Custom'])

    # Check for selected filter type
    if filter_type == 'Custom':
        col__1, col__2 = st.columns([6, 6])
        with col__1:
            labels = ['All','0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90', '91-99', '100']
            score_selector = st.selectbox('Select Score Range', labels)
        with col__2:
            # Campaign Filter
            campaign_option = st.selectbox(
                'Campaign',
                campaigns
            )

        # Setting up 2 columns for input fields
        col1, col2 = st.columns([6,6])

        # Column 2: For Weights Version
        with col1:
            # Weights Version Filter
            weights_version = st.selectbox(
                'Algorithm Version',
                versions
            )
        if campaign_option != temp_campaign or weights_version != temp_algo:
            temp_campaign = campaign_option
            temp_algo = weights_version
            print(temp_campaign)
            print(temp_algo)
            dab_asset = get_dab_asset(temp_campaign, temp_algo)
            dab_asset.append('All')
            dab_asset.reverse()
            # Column 1: For Campaign Option
            with col2:
                DAB_ASSET_NAME = st.selectbox(
                    'DAB Asset ('+str(len(dab_asset))+')' ,
                    dab_asset,
                )
            print("HERE")
            change = False

        # Slider Filters
        col1, col2, col3, col4 = st.columns([3,3,3,3])
        # Size Slider
        with col1:
            weight_size = st.slider("Size Weight", min_value=0.0, max_value=1.00, step=0.1)
        # Color Slider
        with col2:
            weight_property = st.slider("Color Weight", min_value=0.0, max_value=1.00, step=0.1)
        # Label Slider
        with col3:
            weight_label =  st.slider("Label Weight", min_value=0.0, max_value=1.00, step=0.1)
        # Text slider
        with col4:
            text_label = st.slider(
                "Text Weight", min_value=0.0,
                max_value=1.00, step=0.1)

        text_label = text_label

    # Sort Order Select Box
    sort_order = st.selectbox(
            "Select Score Sort Order",
            ["Descending","Ascending"]
                     )
    # Button to get Data
    load = st.button("Load Data")

    # Check for session to Load data from
    if (load or st.session_state.load_state):
        st.session_state.load_state = True
        feedback_text = []
        post_value_list = []

        if (filter_type == "All"):
            query_results = get_all_rows()
        elif (filter_type == "Custom"):
            print("Here")
            query_results = get_rows(campaign_option, weights_version, text_label)

        # query_results = pd.DataFrame(query_results)

        # Print All columns Names
        # st.header("Columns from Table: ")
        # for col in query_results.columns:
        #     st.markdown(f"* {col}")

        # st.dataframe(query_results[temp_col_names])

        # Create 4 Columns on Query Response data display
        # col1, col2, col3, col4 = st.columns([2,2,6,2])

        # Create 2 Columns on Query Response data display
        col1, col2 = st.columns([6,6])
        # Setting up headers against each column
        with col1:
            st.subheader("DAB Asset")
        with col2:
            st.subheader("OMG Asset")

        # Iterate over each row and mapped it into columns
        print(len(query_results))
        for count in range(0,len(query_results)):
            try:
                r = query_results[count]
                col1, col2 = st.columns([6, 6])
                # URI DAB Image Column
                with col1:
                    temp_image = Image.open(
                        requests.get(SignedImage(
                            r['uri_dab']).signed_uri,stream=True).raw)
                    # temp_image = temp_image.resize((200,50))
                    # st.image(temp_image)
                    st.image(temp_image)

                # URI OMG Image Column
                with col2:
                    temp_image = Image.open(
                        requests.get(SignedImage(
                            r[
                                'uri_omg']).signed_uri,
                                     stream=True).raw)
                    # temp_image = temp_image.resize((200,50))
                    # st.image(temp_image)
                    st.image(temp_image)

                col3, col4 = st.columns([6,6])
                # Other Meta Info. Column
                with col3:
                    st.markdown('**DAB Label**: ' + r[
                        'description_dab'])

                # Feedback Button Column
                with col4:
                    st.markdown('**OMG Label**: ' + r[
                        'description_omg'])
                    # st.markdown("---")
                    btn_key = count
                    print(btn_key)
                    print(f"btn_key: {btn_key}")

                temp_data_dict = {
                    'algorithm_version' : r['version'],
                    'match_uri' : r['uri_dab'],
                    'source_uri' : r['uri_omg'],
                    'date' : datetime.datetime.now(),
                    'total_score': float(r['total_score']),
                    'size_score': float(r['size_score']),
                    'property': float(r['property']),
                    'property_score': float(r['property_score']),
                    'label': float(r['label']),
                    'size': float(r['size']),
                    'token_match': int(r['token_match']),
                    'btn_key': btn_key
                }

                post_value_list.append(temp_data_dict)
                feedback_text.append(btn_key)

                temp2_data_dict = {
                    'size': float(r['size']),
                    'size_score': float(r['size_score']),
                    'property': float(r['property']),
                    'property_score': float(r['property_score']),
                    'label': float(r['label']),
                    'text': float(r['text']),
                    'text_score': float(r['text_match']),
                    'token_match': int(r['token_match']),
                    'total_score': float(r['total_score']),

                }

                temp_df = pd.DataFrame.from_dict(
                    temp2_data_dict, orient='index')
                # temp_df.reset_index(level=0,
                #                inplace=True)
                # temp_df.drop('property', inplace=True, axis=1)
                # print(list(temp_df.columns))
                # temp_df = temp_df.iloc[15::]
                # # temp_df[['property']]

                temp_df = temp_df.T
                temp_df.rename(columns={
                    'property': 'color weight',
                    'label': 'label weight',
                    'size': 'size weight',
                    'property_score': 'color score',
                    'total_score': 'total'
                }, inplace=True)
                # print(temp_df)
                # temp_df = temp_df.T
                test = temp_df.astype(str)
                # test = test.style.set_properties(
                #     **{
                #         'font-size': '9pt',
                #     })
                st.table(test)
                # Expander will display input text field for feedback
                with st.expander("Feedback"):
                    form = st.form(key=str(btn_key))
                    form_col1, form_col2, form_col3 = st.columns([4,4,4])
                    with form_col1:
                        property_radio_btn = form.radio("Color Weights", options, key='property'+str(btn_key))
                        property_weights_feedback.append(property_radio_btn)
                    with form_col2:
                        size_radio_btn = form.radio("Size Weights", options_sizes, key='size'+str(btn_key))
                        size_weights_feedback.append(size_radio_btn)
                    with form_col3:
                        label_radio = form.radio("Label Weights", options, key='label'+str(btn_key))
                        label_weights_feedback.append(label_radio)
                    options_manual = ["Match", "Not Match"]
                    label_manual = form.radio("Manual", options_manual, key='manual'+str(btn_key))
                    manual_feedback.append(label_manual)
                    # post_value_list[btn_key]['reason'] = feedback_text
                    # submitted = form.form_submit_button("Submit", on_click=submit_feedback, kwargs=(post_value_list[btn_key]))
                    submitted = form.form_submit_button("Submit")
                    if submitted:
                        # print(property_radio_btn)
                        # post_value_list[btn_key]['']
                        submit_feedback(**post_value_list[btn_key])

                # cols,cols2 = st.columns(2)
                # with cols:
                st.markdown("---")

            except Exception as e:
                print(f"Exception Raised: {e}")
                print(f"URI OMG: {r['uri_omg']}")
                print(f"URI OMG: {r['uri_dab']}")
    # *********************************************