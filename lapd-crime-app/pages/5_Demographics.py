import streamlit as st
import pandas as pd
import plotly.express as px

from src.db import query_df
from src.queries import (
    q_area_list,
    q_top_crime_descriptions,
    q_total_incidents_fact,
    q_dist_victim_sex,
    q_dist_victim_age_group,
    q_dist_victim_descent,
    q_weapon_share,
    q_top_weapon_descriptions,
    q_top_premises,
)
from src.ui import refresh_button

refresh_button()
st.title("Demographics & context")

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
selected_types = []
if type_mode == "Selected types":
    selected_types = st.sidebar.multiselect("Pick types (checkboxes)", type_options, default=[])

# Total incidents for the chosen filters
total = query_df(q_total_incidents_fact(str(date_from), str(date_to), violence_filter, selected_types, area_id))
total_n = int(total.loc[0, "incidents"]) if not total.empty else 0

c1, c2 = st.columns(2)
c1.metric("Incidents (filtered)", total_n)
c2.metric("Area", selected_area_name)

tab1, tab2, tab3 = st.tabs(["Victim", "Weapons", "Premises"])

with tab1:
    sex = query_df(q_dist_victim_sex(str(date_from), str(date_to), violence_filter, selected_types, area_id))
    if not sex.empty:
        st.subheader("Victim sex")
        st.plotly_chart(px.bar(sex, x="victim_sex", y="incidents"), use_container_width=True)

    age = query_df(q_dist_victim_age_group(str(date_from), str(date_to), violence_filter, selected_types, area_id))
    if not age.empty:
        st.subheader("Victim age group")
        st.plotly_chart(px.bar(age, x="victim_age_group", y="incidents"), use_container_width=True)

    descent = query_df(q_dist_victim_descent(str(date_from), str(date_to), violence_filter, selected_types, area_id, limit=20))
    if not descent.empty:
        st.subheader("Victim descent (top 20)")
        st.plotly_chart(px.bar(descent, x="victim_descent", y="incidents"), use_container_width=True)

with tab2:
    ws = query_df(q_weapon_share(str(date_from), str(date_to), violence_filter, selected_types, area_id))
    if not ws.empty:
        with_weapon = int(ws.loc[0, "with_weapon"])
        total = int(ws.loc[0, "total"])
        share = (with_weapon / total) if total else 0
        st.metric("Weapon involved share", f"{share:.2%}")

    wd = query_df(q_top_weapon_descriptions(str(date_from), str(date_to), violence_filter, selected_types, area_id, limit=15))
    if not wd.empty:
        st.subheader("Top weapon descriptions (when weapon present)")
        st.plotly_chart(px.bar(wd, x="weapon_description", y="incidents"), use_container_width=True)

with tab3:
    prem = query_df(q_top_premises(str(date_from), str(date_to), violence_filter, selected_types, area_id, limit=15))
    if not prem.empty:
        st.subheader("Top premises")
        st.plotly_chart(px.bar(prem, x="premise_description", y="incidents"), use_container_width=True)
