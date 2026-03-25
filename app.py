import streamlit as st
import snowflake.connector
import pandas as pd
import re

# ── Config ────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = "NEA23255.east-us-2.azure"
SNOWFLAKE_USER      = "JHUANG"
SNOWFLAKE_PASSWORD  = st.secrets["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = "cortex_wh"
SNOWFLAKE_DATABASE  = "ANALYTIC_DB"
SNOWFLAKE_SCHEMA    = "SALES"
# ─────────────────────────────────────────────────────

def run_query(sql: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        account         = SNOWFLAKE_ACCOUNT,
        user            = SNOWFLAKE_USER,
        password        = SNOWFLAKE_PASSWORD,
        warehouse       = SNOWFLAKE_WAREHOUSE,
        database        = SNOWFLAKE_DATABASE,
        schema          = SNOWFLAKE_SCHEMA,
        login_timeout   = 60,
        network_timeout = 60
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()

def ask_cortex(question: str) -> dict:
    prompt = """You are a Snowflake SQL expert.
Generate a single valid Snowflake SQL query to answer this question:
{question}

Table: ANALYTIC_DB.SALES.VW_FACT_GLOBAL_SALES_ORDERS
Key columns: REVENUE, COGS, PROFIT, REGION, COUNTRY_NAME,
CUST_NAME, PRODUCT_NAME, PRODUCT_CATEGORIES, SALES_REP,
INVOICE_DATE, YEAR, MONTH, QUARTER, QUANTITY_SHIPPED,
SHIPPED_QTY_IN_KG, PRICE_PER_KG_IN_USD, PROFIT_PER_KG_IN_USD

Rules:
- Return ONLY the SQL query
- No explanations, no markdown, no backticks
- Use fully qualified table name ANALYTIC_DB.SALES.VW_FACT_GLOBAL_SALES_ORDERS
- For this year use WHERE YEAR = YEAR(CURRENT_DATE)
- For this month use WHERE YEAR = YEAR(CURRENT_DATE) AND MONTH = MONTH(CURRENT_DATE)
- For this quarter use WHERE YEAR = YEAR(CURRENT_DATE) AND QUARTER = QUARTER(CURRENT_DATE)
- Always add ORDER BY for ranking queries
- Limit to 50 rows max""".format(question=question.replace("'", "\\'"))

    escaped_prompt = prompt.replace("'", "\\'")
    cortex_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            '{escaped_prompt}'
        ) AS generated_sql
    """

    result_df = run_query(cortex_sql)
    raw_sql   = result_df.iloc[0, 0].strip()
    clean_sql = re.sub(r'```sql|```', '', raw_sql).strip()
    data_df   = run_query(clean_sql)

    for col in data_df.columns:
        try:
            data_df[col] = pd.to_numeric(data_df[col])
        except:
            pass

    return {"sql": clean_sql, "df": data_df}

def display_result(df: pd.DataFrame):
    if df.empty:
        st.info("No data found.")
        return

    if df.shape == (1, 1):
        value = df.iloc[0, 0]
        try:
            st.metric(label=df.columns[0], value=f"{float(value):,.2f}")
        except:
            st.metric(label=df.columns[0], value=str(value))

    elif df.shape[1] == 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
        st.bar_chart(df.set_index(df.columns[0]))
        st.dataframe(df, use_container_width=True)

    elif any(c in ["YEAR", "MONTH", "QUARTER", "INVOICE_DATE",
                   "ORDER_DATE", "YEAR_MONTH_DATE"] for c in df.columns):
        date_col = next(
            c for c in df.columns
            if c in ["YEAR", "MONTH", "QUARTER", "INVOICE_DATE",
                     "ORDER_DATE", "YEAR_MONTH_DATE"]
        )
        st.line_chart(df.set_index(date_col))
        st.dataframe(df, use_container_width=True)

    else:
        st.dataframe(df, use_container_width=True)

    st.caption(f"{len(df)} rows returned")
    st.download_button(
        label="⬇️ Download CSV",
        data=df.to_csv(index=False),
        file_name="results.csv",
        mime="text/csv"
    )

# ── Page setup ────────────────────────────────────────
st.set_page_config(
    page_title="Sales Chat",
    page_icon="💬",
    layout="wide"
)

st.title("💬 Chat with Sales Data")
st.caption("Powered by Snowflake Cortex AI + mistral-large2")

if "history" not in st.session_state:
    st.session_state.history = []

st.markdown("**Try asking:**")
col1, col2, col3 = st.columns(3)

suggestions = {
    col1: ["Revenue by region this year",  "Top 10 customers by revenue"],
    col2: ["Monthly revenue trend 2024",   "Profit margin by product category"],
    col3: ["Best sales rep by revenue",    "Revenue by country this quarter"]
}

clicked = None
for col, questions in suggestions.items():
    for q in questions:
        if col.button(q, key=q):
            clicked = q

st.markdown("---")
col_input, col_btn = st.columns([5, 1])

with col_input:
    user_input = st.text_input(
        "question",
        value=clicked if clicked else "",
        placeholder="Ask anything about your sales data...",
        label_visibility="collapsed"
    )
with col_btn:
    submit = st.button("Ask ▶", use_container_width=True)

question = user_input if (submit or clicked) and user_input else None

if question:
    with st.spinner(f"Analyzing: '{question}'..."):
        try:
            result = ask_cortex(question)
            st.session_state.history.append({
                "question": question,
                "sql":      result["sql"],
                "df":       result["df"],
                "error":    None
            })
        except Exception as e:
            st.session_state.history.append({
                "question": question,
                "sql":      None,
                "df":       None,
                "error":    str(e)
            })

if st.session_state.history:
    if st.button("🗑️ Clear history"):
        st.session_state.history = []
        st.experimental_rerun()

for item in reversed(st.session_state.history):
    st.markdown("---")
    st.markdown(f"**🧑 You:** {item['question']}")

    if item["error"]:
        st.error(f"❌ {item['error']}")
        st.info("💡 Try rephrasing your question.")
    else:
        st.markdown("**🤖 Assistant:**")
        if item["sql"]:
            with st.expander("🔍 View generated SQL"):
                st.code(item["sql"], language="sql")
        display_result(item["df"])

else:
    st.info("👆 Click a suggestion or type a question above to get started!")