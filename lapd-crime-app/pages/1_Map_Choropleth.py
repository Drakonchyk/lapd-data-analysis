import pandas as pd
import plotly.express as px
import streamlit as st

from src.db import query_df
from src.geo import load_or_fetch_lapd_geojson, normalize_area_id
from src.queries import (
    q_date_bounds_daily_area,
    q_map_agg_daily_area,
    q_top_crime_descriptions,
    q_map_agg_fact_area,
    q_daily_series_fact_for_area,
    q_top_descriptions_for_area,
)
from src.ui import refresh_button

refresh_button()
st.title("Choropleth map (LAPD Areas/Divisions)")

bounds = query_df(q_date_bounds_daily_area())
min_date = pd.to_datetime(bounds.loc[0, "min_date"]).date()
max_date = pd.to_datetime(bounds.loc[0, "max_date"]).date()

default_start = max(min_date, (pd.Timestamp(max_date) - pd.Timedelta(days=365)).date())
default_end = max_date

date_from, date_to = st.sidebar.date_input(
    "Date range",
    value=(default_start, default_end),
    min_value=min_date,
    max_value=max_date,
)

violence_filter = st.sidebar.selectbox(
    "Violence filter",
    ["All", "Violent only", "Non-violent only"],
    index=0,
)

top_types = query_df(q_top_crime_descriptions(str(date_from), str(date_to), violence_filter, limit=80))
type_options = top_types["crime_description"].tolist() if not top_types.empty else []

selected_types = st.sidebar.multiselect(
    "Crime types (crime_description) — optional",
    options=type_options,
    default=[],
)

metric_choices = ["crime_count", "violent_count"]
if violence_filter == "All":
    metric_choices.append("violent_share")

metric = st.sidebar.selectbox("Metric", metric_choices, index=0)

geojson = load_or_fetch_lapd_geojson()

use_fact = (violence_filter != "All") or (len(selected_types) > 0)

if use_fact:
    df = query_df(q_map_agg_fact_area(str(date_from), str(date_to), violence_filter, selected_types))
else:
    df = query_df(q_map_agg_daily_area(str(date_from), str(date_to)))

df["area_id_norm"] = df["area_id"].apply(normalize_area_id)

if df.empty:
    st.warning("No data returned for this selection.")
    st.stop()

areas_sorted = df.sort_values("area_name")[["area_id_norm", "area_name"]].drop_duplicates()
fallback_area = st.sidebar.selectbox(
    "Selected area (fallback)",
    options=list(areas_sorted["area_id_norm"]),
    format_func=lambda x: areas_sorted.loc[areas_sorted["area_id_norm"] == x, "area_name"].iloc[0],
)

fig = px.choropleth_mapbox(
    df,
    geojson=geojson,
    locations="area_id_norm",
    featureidkey="properties._area_id",
    color=metric,
    color_continuous_scale=px.colors.sequential.Blues,
    hover_name="area_name",
    hover_data={
        "area_id_norm": False,
        "crime_count": True,
        "violent_count": True,
        "violent_share": ":.2%" if "violent_share" in df.columns else False,
    },
    mapbox_style="carto-positron",
    center={"lat": 34.05, "lon": -118.25},
    zoom=9,
    opacity=0.65,
)
fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))

st.caption("Tip: click a polygon (or use box/lasso). If selection doesn’t work, use the fallback dropdown.")
event = st.plotly_chart(fig, key="lapd_choropleth", on_select="rerun", use_container_width=True)

selected_area_id = None
try:
    sel = event.get("selection", None)
    if sel and sel.get("points"):
        selected_area_id = sel["points"][0].get("location")
except Exception:
    selected_area_id = None

if not selected_area_id:
    selected_area_id = fallback_area

selected_name = df.loc[df["area_id_norm"] == selected_area_id, "area_name"].iloc[0]
st.subheader(f"Selected: {selected_name} (area_id={selected_area_id})")

tab1, tab2, tab3 = st.tabs(["Summary", "Daily trend", "Top crime types"])

with tab1:
    row = df[df["area_id_norm"] == selected_area_id].iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Crimes (filtered)", int(row["crime_count"]))
    c2.metric("Violent (filtered)", int(row["violent_count"]))
    if violence_filter == "All":
        c3.metric("Violent share", f"{float(row['violent_share']):.2%}")
    else:
        c3.metric("Violent share", "— (filtered subset)")
    c4.metric("Avg reporting delay (days)", f"{float(row['avg_reporting_delay_days']):.2f}")

with tab2:
    series = query_df(
        q_daily_series_fact_for_area(
            str(date_from), str(date_to), selected_area_id, violence_filter, selected_types
        )
    )
    if series.empty:
        st.info("No daily series for this selection.")
    else:
        series["occurrence_date"] = pd.to_datetime(series["occurrence_date"])
        y_cols = ["crime_count", "violent_count"] if violence_filter == "All" else ["crime_count"]
        fig2 = px.line(series, x="occurrence_date", y=y_cols)
        st.plotly_chart(fig2, use_container_width=True)

with tab3:
    top = query_df(
        q_top_descriptions_for_area(
            str(date_from), str(date_to), selected_area_id, violence_filter, selected_types, limit=15
        )
    )
    if top.empty:
        st.info("No breakdown for this selection.")
    else:
        fig3 = px.bar(top, x="crime_description", y="incidents")
        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(top)
