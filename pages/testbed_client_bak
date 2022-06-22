# Author: Muhammad Hassaan Bashir
# Dated: 13-January-2022

# Testbed app for Client

# Importing Required Libraries
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


algo_version_1 = [200, 30, 40, 420]
algo_version_2 = [150, 351, 400, 395]
index = ['0-25', '26-50', '51-75', '76-100']
df = pd.DataFrame({'speed': algo_version_1,
                   'lifespan': algo_version_2}, index=index)

# We cannot add width to year so we create another list
indices = np.arange(len(index))


width = 0.20

# Plotting
plt.bar(indices, algo_version_1, width=width, label="Featured Version")
xlocs, xlabs = plt.xticks()
for i in range(len(algo_version_1)):
    plt.annotate(str(algo_version_1[i]), xy=(indices[i],algo_version_1[i]), ha='center', va='bottom')

for i in range(len(algo_version_2)):
    plt.annotate(str(algo_version_2[i]), xy=(indices[i]+0.20,algo_version_2[i]), ha='center', va='bottom')

# Offsetting by width to shift the bars to the right
plt.bar(indices + width, algo_version_2, width=width, label="Revised Featured Version")


# Displaying year on top of indices
plt.xticks(ticks=indices, labels=index)


plt.xlabel("Total Score Range")
plt.ylabel("Assets Count")
plt.legend()

st.pyplot(plt)