# %%

from google.cloud import bigquery
import ipyplot
import os

client = bigquery.Client()
query_job = client.query(
    """
    SELECT  * FROM `project-fermi.adidas.gcp_vision_output_new`
    LIMIT 10
    """
)
results = query_job.result()
# results = [r.uri.replace('gs://', 'https://storage.cloud.google.com/') for r in results]
# ipyplot.plot_images(results, )
#


#%%

import datetime
import os

from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "project-fermi-24f48601c1b4.json"


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
    storage_client = storage.Client()
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
    # print("Generated GET signed URL:")
    # print(url)
    # print("You can use this URL with any user agent, for example:")
    # print("curl '{}'".format(url))
    return url


# %%
from dataclasses import dataclass


@dataclass
class GridImage:
    uri: str = ''
    label: str = 'no label'
    bucket: str = ''
    fn: str = ''
    signed_uri: str = ''

    def __post_init__(self):
        self.bucket, self.fn = extract_bucket_and_fn(self.uri)
        self.label = self.fn
        self.signed_uri = generate_download_signed_url_v4(self.bucket, self.fn)


results = [GridImage(uri=r) for r in results]
results = [r.signed_uri for r in results if r.signed_uri]
# %%
print('ok!')
ipyplot.plot_images(results,
                    labels=range(0, len(results))
                    )
