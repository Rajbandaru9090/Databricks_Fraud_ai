import streamlit as st
import pandas as pd
from databricks import sql
import os

# Load secrets from Streamlit's secrets.toml
DATABRICKS_HOST = st.secrets["databricks_host"]
HTTP_PATH = st.secrets["http_path"]
TOKEN = st.secrets["databricks_token"]

st.set_page_config(page_title="Databricks Debugger", layout="wide")
st.title("🔍 Databricks Connection & Table Debugger")

# ✅ Connect to Databricks
try:
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN
    ) as connection:

        st.success("✅ Connection to Databricks established successfully!")

        query = "SELECT * FROM orders_main LIMIT 10"
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)

            st.subheader("✅ Sample Data from orders_main")
            st.dataframe(df)

except Exception as e:
    st.error("❌ Failed to connect or run query:")
    st.exception(e)
