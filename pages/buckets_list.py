# Author: Muhammad Hassaan Bashir
# Dated: 10th May, 2022

# This is the script that will create a new page and will give the user the option to select the bucket and it will list down all the blobs on it


import streamlit as st
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from PIL import Image

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
# Project Name
project = 'project-fermi'

im = Image.open("./assets/list.png")

st.set_page_config(
    page_title="Adidas Oliver Buckets List",
    page_icon=im,
    layout='wide'
)

# Buckets List function
# List of Storage Buckets
def list_buckets(project_name):
    """Lists all buckets."""
    storage_client = storage.Client(credentials=credentials, project=project_name)
    buckets = storage_client.list_buckets()
    b_list = []
    for bucket in buckets:
        b_list.append(bucket.name)
    print(type(b_list))
    return b_list


# List of Total Blobs and Zip files in Bucket:
def list_blobs(bucket_name):
    storage_client = storage.Client(credentials=credentials, project=project)

    print(f"Bucket Name: {bucket_name}")
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)
    zip_count = 0
    list_blobs = []
    all_blobs = []
    blob_count = 0

    try:
        for blob in blobs:
            if ".zip" in blob.name:
                zip_count+=1
                list_blobs.append(blob.name)

            all_blobs.append(blob.name)

        blob_count = len(all_blobs)

    except Exception as ex:
        print("EXCEPTION: "+str(ex))

    blob_meta_dict = {
        "blob_count": blob_count,
        "zip_count": zip_count,
        "list_blob": list_blobs
    }

    return blob_meta_dict

# # Main App function of the page
# def app():
buckets = list_buckets(project)

# Initialize Session state
if 'load_state' not in st.session_state:
    st.session_state.load_state = False

with st.container():
    col1,col2 = st.columns([6,3])
    with col1:
        buckets_dropdown = st.selectbox("Buckets: ("+str(len(buckets))+")",buckets)

    load = st.button("Load Data")
    if (load or st.session_state.load_state):
        st.session_state.load_state = True

        blob_meta_data = list_blobs(buckets_dropdown)

        col1, col2 = st.columns([6,6])
        with col1:
            st.header("Total Zip Files in Bucket")
        with col2:
            st.header("Total Blobs in Bucket")

        col3, col4 = st.columns([6, 6])
        with col3:
            st.markdown(blob_meta_data['zip_count'])
        with col4:
            st.markdown(blob_meta_data['blob_count'])

        st.dataframe(blob_meta_data['list_blob'])