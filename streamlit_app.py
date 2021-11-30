import streamlit as st
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

df = pd.DataFrame({
    'first column': [1, 2, 3, 4],
    'second column': [10, 20, 30, 40]
})

st.header("Awesome Data viz")
with st.container():
    with st.form("my_form"):
        col1, col2 = st.columns(2)
        st.header("Select resource")
        with col1:
            option = st.selectbox(
                'Campaign Name',
                df['first column'])
            option = st.selectbox(
                'Container',
                df['first column'])
            option = st.selectbox(
                'Asset',
                df['first column'])
        with col2:
            option = st.selectbox(
                'Ad. BU',
                df['first column'])
            option = st.selectbox(
                'Market',
                df['first column'])
            option = st.selectbox(
                'Match Rate',
                df['first column'])
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write("option", option)

            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.header("DAB Sources")
                    st.image("https://static.streamlit.io/examples/cat.jpg")

                with col2:
                    st.header("OMG Adapted Results")
                    st.image("https://static.streamlit.io/examples/dog.jpg")
