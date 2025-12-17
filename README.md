# LAPD Crime Data Analysis & Dashboard

This project is an end-to-end data engineering and visualization solution that analyzes crime data from the Los Angeles Police Department (LAPD). It utilizes a **Databricks Medallion Architecture** (Bronze/Silver/Gold) for data processing and a **Streamlit** application for interactive exploration.

## Project Overview

The system ingests raw data from the LA City Open Data API, cleans and enriches it using PySpark, aggregates it into analytic marts, and serves it via a dynamic dashboard.

### Key Features
* **Automated Ingestion:** Fetches incremental data from Socrata API.
* **Data Quality:** Handles missing values, sanitizes strings, and filters invalid geospatial coordinates.
* **Advanced Analytics:** Calculates reporting delays, identifies temporal anomalies, and categorizes crimes (Violent vs. Non-Violent).
* **Interactive Dashboard:** A multi-page Streamlit app featuring:
    * **Choropleth Maps:** Visualizing crime density by LAPD division.
    * **Trend Analysis:** Monthly and daily crime trends.
    * **Pattern Recognition:** Heatmaps for Day-of-Week vs. Hour-of-Day.
    * **Demographics:** Victim age, descent, and sex analysis.
    * **AI Assistant (Genie):** Natural language querying of the dataset using Databricks Genie.

## Architecture & Pipeline

The project is structured into two main components: the Databricks ETL Pipeline and the Streamlit App.

### 1. Data Pipeline (Databricks Notebooks)
The ETL process follows the Medallion architecture:

* **Bronze (`01_bronze_lapd_ingest.py`):**
    * Ingests raw JSON data from the LAPD API.
    * Maintains schema evolution and adds ingestion metadata.
    * **Target:** `lapd.bronze.crime`
* **Silver (`02_silver_lapd_clean.py`):**
    * Type casting (timestamps, integers).
    * Data cleaning (null handling, coordinate validation).
    * Feature engineering (derived columns like `is_violent`, `reporting_delay`).
    * **Target:** `lapd.silver.crime`
* **Gold (`03_gold_lapd_specification.py` & `dashboard_marts.py`):**
    * Business logic application (bucketing age groups, time of day).
    * De-duplication logic.
    * Aggregation into Marts (Daily, Hourly, Type Trends).
    * **Target:** `lapd.gold.fact_crime_incidents_gold` and various `mart_*` tables.
* **Analysis (`04_eda.py`):**
    * Exploratory Data Analysis (EDA) using Pandas and Matplotlib.
    * Anomaly detection using Z-scores.

### 2. Dashboard (Streamlit)
Located in `lapd-crime-app/`, this application connects to the Databricks SQL Warehouse to query the Gold layer.

* **Tech Stack:** Streamlit, Plotly, Pandas, Databricks SQL Connector, Databricks SDK.
* **Genie Integration:** Uses `databricks-sdk` to interface with Databricks Genie spaces for AI-driven analytics.

## Repository Structure

```text
├── 01_bronze_lapd_ingest.py      # Ingestion logic
├── 02_silver_lapd_clean.py       # Cleaning and transformation
├── 03_gold_lapd_specification.py # Dimensional modeling and aggregation
├── 04_eda.py                     # Exploratory analysis notebook
├── dashboard_marts.py            # SQL definitions for dashboard views
├── lapd-crime-app/               # Streamlit Application
│   ├── app.py                    # Main entry point
│   ├── app.yaml                  # Deployment config
│   ├── requirements.txt          # Python dependencies
│   ├── pages/                    # Multi-page app views
│   │   ├── 1_Map_Choropleth.py
│   │   ├── 2_Trends.py
│   │   ├── 3_Patterns.py
│   │   ├── 4_Genie.py            # AI Chat Interface
│   │   └── 5_Demographics.py
│   └── src/                      # Helper modules
│       ├── db.py                 # Database connection
│       ├── geo.py                # GeoJSON handling
│       ├── queries.py            # SQL Query repository
│       └── ui.py                 # UI components
└── README.md

