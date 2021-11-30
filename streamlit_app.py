import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import datetime
import os
from dataclasses import dataclass
from google.cloud import storage

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


def extract_bucket_and_fn(path: str):
    path = path.replace('gs://', '')
    path_elements = path.split('/')
    fn = path_elements.pop()
    bucket = '/'.join(path_elements)
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


query = """
    SELECT  DISTINCT campaign FROM `project-fermi.adidas.dab_omg_match_output`
    LIMIT 50
    """
results = run_query(query=query)
campaigns = [r['campaign'] for r in results]

st.header("Awesome Data viz")
with st.container():
    with st.form("my_form"):
        col1, col2 = st.columns(2)
        st.header("Select resource")
        with col1:
            option = st.selectbox(
                'Campaign',
                campaigns
            )
        submitted = st.form_submit_button("Submit")
        if submitted:
            query = """
                SELECT  * FROM `project-fermi.adidas.dab_omg_match_output`
                LIMIT 10
                """
            rows = run_query(query=query)

            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.header("DAB Sources")
                    for image in [SignedImage(r['uri_dab']) for r in rows]:
                        st.image(image.signed_uri)

                with col2:
                    st.header("OMG Adapted Results")
                    for image in [SignedImage(r['uri_omg']) for r in rows]:
                        st.image(image.signed_uri)
