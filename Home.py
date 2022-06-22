import streamlit as st

# Custom imports
# from multipage import MultiPage
# from pages import test_bed, feedback, test_bed_internal, buckets_list
st.set_page_config(
    page_title="Hello",
    page_icon="👋",
    layout="wide"
)
# st.set_page_config(layout="wide")
# Create an instance of the app
# app = MultiPage()

# Title of the main page
st.title("Adidas Reporting 2.0")

marks_list = """
            * Buckets List view
            * User Feedback
            * TestBed Internal APP
            """

st.markdown(marks_list)

# Add all your applications (pages) here
# app.add_page("Adidas Testbed", test_bed.app)
# app.add_page("Oliver Testbed", test_bed_internal.app)
# app.add_page("Feedbacks", feedback.app)
# app.add_page("Buckets Info", buckets_list.app)
# The main app
# app.run()