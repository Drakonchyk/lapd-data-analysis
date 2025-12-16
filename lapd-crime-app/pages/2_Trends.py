import streamlit as st
import pandas as pd
import plotly.express as px

from src.db import query_df
from src.queries import (
    q_area_list,
    q_top_crime_descriptions,
    q_monthly_trends_fact,
    q_monthly_total_fact,
)
from src.ui import refresh_button

refresh_button()
st.title("Trends (crime types / descriptions)")

date_from, date_to = st.sidebar.date_input(
    "Date range",
    value=(pd.Timestamp("2022-01-01").date(), pd.Timestamp("2022-12-31").date()),
)

violence_filter = st.sidebar.selectbox(
    "Violence filter",
    ["All", "Violent only", "Non-violent only"],
    index=0,
)

areas = query_df(q_area_list())
area_names = ["All areas"] + areas["area_name"].tolist()
selected_area_name = st.sidebar.selectbox("Area (optional)", area_names, index=0)

area_id = None
if selected_area_name != "All areas":
    area_id = areas.loc[areas["area_name"] == selected_area_name, "area_id"].iloc[0]

top = query_df(q_top_crime_descriptions(str(date_from), str(date_to), violence_filter, limit=60))
type_options = top["crime_description"].tolist() if not top.empty else []

mode = st.sidebar.radio("Type selection", ["Top N types", "Manual (checkboxes)"], index=0)

if mode == "Top N types":
    n = st.sidebar.slider("N", 3, 20, 8)
    selected_types = type_options[:n]
else:
    selected_types = st.sidebar.multiselect(
        "Crime types (pick a few)",
        options=type_options,
        default=[],
    )
    st.sidebar.caption("Tip: choose ~5–10 types so the plot stays readable.")

smooth = st.sidebar.checkbox("Smooth (3-month rolling avg)", value=False)

if mode == "Manual (checkboxes)" and len(selected_types) == 0:
    df = query_df(q_monthly_total_fact(str(date_from), str(date_to), violence_filter, [], area_id))
    if df.empty:
        st.warning("No data for these filters.")
        st.stop()

    df["month_start"] = pd.to_datetime(df["month_start"])
    df = df.sort_values("month_start")

    if smooth:
        df["incidents"] = df["incidents"].rolling(3, min_periods=1).mean()
        df["violent_incidents"] = df["violent_incidents"].rolling(3, min_periods=1).mean()

    st.plotly_chart(px.line(df, x="month_start", y="incidents"), use_container_width=True)
    if violence_filter == "All":
        st.plotly_chart(px.line(df, x="month_start", y="violent_incidents"), use_container_width=True)

    st.info("Select crime types to see per-type lines (or use Top N mode).")
    st.stop()

df = query_df(q_monthly_trends_fact(str(date_from), str(date_to), violence_filter, selected_types, area_id))
if df.empty:
    st.warning("No data for these filters. Try widening date range or clearing filters.")
    st.stop()

df["month_start"] = pd.to_datetime(df["month_start"])
df = df.sort_values(["crime_description", "month_start"])

if smooth:
    df["incidents"] = df.groupby("crime_description")["incidents"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    df["violent_incidents"] = df.groupby("crime_description")["violent_incidents"].transform(lambda s: s.rolling(3, min_periods=1).mean())

st.plotly_chart(px.line(df, x="month_start", y="incidents", color="crime_description"), use_container_width=True)

if violence_filter == "All":
    st.plotly_chart(px.line(df, x="month_start", y="violent_incidents", color="crime_description"), use_container_width=True)
