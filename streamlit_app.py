import datetime
from dataclasses import dataclass
from math import floor, ceil

import streamlit as st
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


def extract_bucket_and_fn(path: str):
    path = path.replace('gs://', '')
    path_elements = path.split('/')
    path_elements = list(map(urllib.parse.quote, path_elements))
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


def get_campaigns():
    query = """
    SELECT  DISTINCT campaign FROM `project-fermi.adidas.dab_omg_match_output`
    LIMIT 50
    """
    results = run_query(query=query)
    return [r['campaign'] for r in results]


campaigns = get_campaigns()
st.header("Awesome Data viz")


def get_rows(campaign_option, total_score):
    query = f"""
            SELECT  * FROM `project-fermi.adidas.dab_omg_match_output`
            WHERE campaign = "{campaign_option}"
            AND total_score >= {total_score}
                ORDER BY total_score DESC
            LIMIT 10
            """
    rows = run_query(query=query)
    return rows


def round_up(num, divisor):
    return ceil(num / divisor) * divisor


def round_down(num, divisor):
    return floor(num / divisor) * divisor


def get_max_min_range():
    query = f"""
    SELECT MAX(total_score), MIN(total_score)
        FROM `project-fermi.adidas.dab_omg_match_output`
            """
    biggest, smallest = run_query(query=query)[0].values()
    steps = round_up(((biggest - smallest) // 10), 5)
    return round_up(biggest, steps), round_down(smallest, steps), steps


with st.form("my_form"):
    st.header("Select resource")
    campaign_option = st.selectbox(
        'Campaign',
        campaigns
    )
    bg, sm, steps = get_max_min_range()
    total_score = st.slider(
        label='Minimum Total Score?',
        min_value=sm,
        max_value=bg,
        step=steps)
    submitted = st.form_submit_button("Submit")
    if submitted:
        rows = get_rows(campaign_option=campaign_option, total_score=total_score)
        st.header(f"DAB Sources | Results: {len(rows)}")
        for r in rows:
            col1, col2 = st.columns(2)
            with col1:
                st.image(SignedImage(r['uri_omg']).signed_uri)
            with col2:
                st.image(SignedImage(r['uri_dab']).signed_uri)
                st.write(r['total_score'])
