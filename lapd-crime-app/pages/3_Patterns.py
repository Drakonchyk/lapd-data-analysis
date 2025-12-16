import streamlit as st
import pandas as pd
import plotly.express as px

from src.db import query_df
from src.queries import (
    q_area_list,
    q_top_crime_descriptions,
    q_hourly_weekday_fact,
)
from src.ui import refresh_button

refresh_button()
st.title("Hourly / weekday patterns")

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

type_mode = st.sidebar.radio("Crime types", ["All types", "Selected types"], index=0)

if type_mode == "All types":
    selected_types = []
else:
    selected_types = st.sidebar.multiselect(
        "Pick types (checkboxes)",
        options=type_options,
        default=[],
    )
    if len(selected_types) == 0:
        st.info("Pick at least 1 type (or switch back to 'All types').")
        st.stop()

df = query_df(q_hourly_weekday_fact(str(date_from), str(date_to), violence_filter, selected_types, area_id))
if df.empty:
    st.warning("No data for these filters. Try widening date range or clearing type/area filters.")
    st.stop()

weekday_vals = sorted(df["weekday"].dropna().unique().tolist())
weekday_order_full = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
weekday_order_short = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

category_orders = None
if set(weekday_vals).issubset(set(weekday_order_full)):
    category_orders = {"weekday": weekday_order_full}
elif set(weekday_vals).issubset(set(weekday_order_short)):
    category_orders = {"weekday": weekday_order_short}

st.caption("Heatmap is aggregated over the selected crime types (if any).")

fig = px.density_heatmap(
    df,
    x="hour_occ",
    y="weekday",
    z="incidents",
    color_continuous_scale=px.colors.sequential.Blues,
    category_orders=category_orders or {},
)
st.plotly_chart(fig, use_container_width=True)
