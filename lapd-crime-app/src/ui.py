import streamlit as st

def refresh_button(label="🔄 Refresh data"):
    if st.sidebar.button(label):
        st.cache_data.clear()
        st.rerun()
