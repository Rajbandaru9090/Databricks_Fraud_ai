import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from openai import OpenAI

# --- Load Secrets ---
DATABRICKS_HOST = st.secrets["databricks_host"]
DATABRICKS_TOKEN = st.secrets["databricks_token"]
HTTP_PATH = st.secrets["http_path"]
openai_api_key = st.secrets["openai_api_key"]

# --- Set up OpenAI Client (openai>=1.0.0 compatible) ---
client = OpenAI(api_key=openai_api_key)

# --- Databricks Query Function ---
def query_databricks(query):
    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"❌ Error querying Databricks: {e}")
        return pd.DataFrame()

# --- GPT Insight Function ---
def ask_gpt(prompt):
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a top fraud detection analyst for a retail company."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=300,
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"❌ GPT error: {e}"

# --- UI Setup ---
st.set_page_config(layout="wide", page_title="RetailX - Fraud Detection AI Dashboard")
st.title("🔥 RetailX: AI-Powered Fraud Detection & Revenue Intelligence")

st.markdown("""
<style>
.big-font { font-size:20px !important; font-weight: 600; }
.metric { font-size: 18px; font-weight: bold; }
.stPlotlyChart { padding-bottom: 20px; }
</style>
""", unsafe_allow_html=True)
st.markdown("---")

# --- Sidebar Filters ---
with st.sidebar:
    st.header("🔍 Smart Filters")
    region = st.selectbox("Select Region", ["All", "North", "South", "East", "West"])
    month = st.selectbox("Select Month", ["All"] + [f"2024-{str(i).zfill(2)}" for i in range(1, 13)])
    category = st.selectbox("Select Department", ["All", "produce", "dairy eggs", "beverages", "snacks", "frozen", "bakery"])

# --- SQL Query Construction ---
query = "SELECT * FROM workspace.retailx.fraud_orders"
filters = []
if region != "All":
    filters.append(f"region_group = '{region}'")
if month != "All":
    filters.append(f"order_month = '{month}'")
if category != "All":
    filters.append(f"LOWER(department) = '{category.lower()}'")
if filters:
    query += " WHERE " + " AND ".join(filters)

# --- Load Data ---
df = query_databricks(query)

# --- Data Validity Check ---
if df.empty:
    st.warning("⚠️ No data found with current filters.")
    st.stop()

# --- KPI Metrics ---
st.markdown("## 🚀 Key Performance Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Orders", f"{df['order_id'].nunique():,}")
col2.metric("Total Revenue", f"${df['revenue'].sum():,.2f}")
col3.metric("Fraud Rate", f"{100 * df['is_fraud'].mean():.2f}%")
st.markdown("---")

# --- GPT Assistant ---
st.subheader("🧠 AI Assistant: Fraud Trend Analysis")
question = st.text_input("Ask anything about fraud patterns, revenue, or risks", "What are the most fraud-prone departments?")
if st.button("🔎 Get GPT Insight"):
    preview_df = df[["region_group", "order_month", "department", "revenue", "is_fraud"]].head(100).to_dict("records")
    prompt = f"""
You are analyzing retail fraud data with columns: region_group, order_month, department, revenue, is_fraud.
Here is a sample:
{preview_df}
User question: {question}
Give data-driven fraud insights and highlight anomalies in regions, time periods, or departments.
"""
    answer = ask_gpt(prompt)
    st.markdown(f"**🤖 GPT Insight:** {answer}")
st.markdown("---")

# --- Visualizations ---
st.subheader("📊 Revenue vs Fraud Overview")
tab1, tab2, tab3 = st.tabs(["Revenue by Department", "Fraud % by Region", "Monthly Trends"])

with tab1:
    dept_df = df.groupby("department")["revenue"].sum().reset_index().sort_values("revenue", ascending=False)
    fig = px.bar(dept_df, x="department", y="revenue", color="department", title="💰 Revenue by Department")
    fig.update_layout(width=1100, height=500, margin=dict(t=60, b=40))
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    fraud_df = df.groupby("region_group")["is_fraud"].mean().reset_index()
    fig = px.bar(fraud_df, x="region_group", y="is_fraud", title="⚠️ Fraud Percentage by Region")
    fig.update_yaxes(tickformat=".1%")
    fig.update_layout(width=1100, height=500, margin=dict(t=60, b=40))
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    month_df = df.groupby("order_month")["revenue"].sum().reset_index()
    fig = px.line(month_df, x="order_month", y="revenue", markers=True, title="📈 Monthly Revenue Trend")
    fig.update_layout(width=1100, height=500, margin=dict(t=60, b=40))
    st.plotly_chart(fig, use_container_width=True)

# --- Heatmap ---
st.subheader("🌡️ Department-wise Fraud Heatmap")
heatmap_df = df.groupby(["department", "region_group"]).agg(fraud_rate=("is_fraud", "mean")).reset_index()
if heatmap_df.empty:
    st.warning("No data available to generate heatmap.")
else:
    heatmap_pivot = heatmap_df.pivot(index="department", columns="region_group", values="fraud_rate").fillna(0)
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_pivot.values,
        x=heatmap_pivot.columns,
        y=heatmap_pivot.index,
        colorscale="Reds",
        colorbar=dict(title="Fraud Rate"),
        hoverongaps=False
    ))
    fig.update_layout(
        title="🧊 Fraud Rate Heatmap (Dept vs Region)",
        width=1100,
        height=500,
        margin=dict(t=60, b=40)
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Data Preview ---
st.subheader("📋 Sample of Filtered Data")
st.dataframe(df.head(10), use_container_width=True)
st.markdown("---")
