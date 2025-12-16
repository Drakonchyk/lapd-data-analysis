import os
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

@st.cache_resource
def get_conn():
    cfg = Config()  
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        raise RuntimeError(
            "Missing DATABRICKS_WAREHOUSE_ID. Put it in app.yaml (env) or app settings."
        )

    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def query_df(query: str):
    with get_conn().cursor() as cur:
        cur.execute(query)
        return cur.fetchall_arrow().to_pandas()
