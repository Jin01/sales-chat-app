import streamlit as st
import snowflake.connector
import pandas as pd
import requests
import re

# ── Config ────────────────────────────────────────────
SNOWFLAKE_ACCOUNT    = "NEA23255.east-us-2.azure"
SNOWFLAKE_USER       = "JHUANG"
SNOWFLAKE_PASSWORD   = st.secrets["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE  = "cortex_wh"
SNOWFLAKE_DATABASE   = "DATA_PLATFORM_DB"
SNOWFLAKE_SCHEMA     = "PROD_MARTS_SALES"
SEMANTIC_MODEL_STAGE = "@ANALYTIC_DB.PUBLIC.CORTEX_STAGE/semantic_model.yaml"
CORTEX_ANALYST_URL   = (
    f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    f"/api/v2/cortex/analyst/message"
)
# ─────────────────────────────────────────────────────


def get_connection():
    return snowflake.connector.connect(
        account         = SNOWFLAKE_ACCOUNT,
        user            = SNOWFLAKE_USER,
        password        = SNOWFLAKE_PASSWORD,
        warehouse       = SNOWFLAKE_WAREHOUSE,
        database        = SNOWFLAKE_DATABASE,
        schema          = SNOWFLAKE_SCHEMA,
        login_timeout   = 60,
        network_timeout = 60,
    )


def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def get_snowflake_token() -> str:
    """Generate a short-lived scoped token using the existing Snowflake session."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT SYSTEM$GENERATE_SCOPED_CREDS() AS token")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def ask_cortex_analyst(question: str) -> dict:
    """
    Call the Cortex Analyst REST API with the semantic model YAML.
    Returns sql, explanation, and dataframe.
    """
    token = get_snowflake_token()

    payload = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": question}]
            }
        ],
        "semantic_model_file": SEMANTIC_MODEL_STAGE,
    }

    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    response = requests.post(
        CORTEX_ANALYST_URL,
        json    = payload,
        headers = headers,
        timeout = 120,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Cortex Analyst API error {response.status_code}: {response.text}"
        )

    data    = response.json()
    content = data["message"]["content"]

    # extract SQL statement
    sql_items = [item for item in content if item["type"] == "sql"]
    if not sql_items:
        # no SQL generated — Cortex may have returned a clarification
        text_items = [item for item in content if item["type"] == "text"]
        explanation = " ".join(item.get("text", "") for item in text_items)
        return {"sql": None, "df": None, "explanation": explanation}

    sql = sql_items[0]["statement"]

    # extract natural language explanation (if present)
    text_items  = [item for item in content if item["type"] == "text"]
    explanation = " ".join(item.get("text", "") for item in text_items) or None

    # run the generated SQL
    df = run_query(sql)

    # coerce numeric columns
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass

    return {"sql": sql, "df": df, "explanation": explanation}


def display_result(item: dict):
    """Render explanation, chart, table, and download button."""
    if item.get("explanation"):
        st.markdown(f"_{item['explanation']}_")

    sql = item.get("sql")
    df  = item.get("df")

    if sql:
        with st.expander("🔍 View generated SQL"):
            st.code(sql, language="sql")

    if df is None:
        # Cortex returned a clarification instead of SQL
        return

    if df.empty:
        st.info("No data found for this query.")
        return

    # ── single value → metric card
    if df.shape == (1, 1):
        value = df.iloc[0, 0]
        try:
            st.metric(label=df.columns[0], value=f"{float(value):,.2f}")
        except Exception:
            st.metric(label=df.columns[0], value=str(value))

    # ── two columns, second numeric → bar chart
    elif df.shape[1] == 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
        st.bar_chart(df.set_index(df.columns[0]))
        st.dataframe(df, use_container_width=True)

    # ── time-series → line chart
    elif any(
        c in df.columns
        for c in ["YEAR", "MONTH", "QUARTER", "INVOICE_DATE",
                  "ORDER_DATE", "YEAR_MONTH_DATE"]
    ):
        date_col = next(
            c for c in df.columns
            if c in ["YEAR", "MONTH", "QUARTER", "INVOICE_DATE",
                     "ORDER_DATE", "YEAR_MONTH_DATE"]
        )
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            st.line_chart(df.set_index(date_col)[numeric_cols])
        st.dataframe(df, use_container_width=True)

    # ── default → table
    else:
        st.dataframe(df, use_container_width=True)

    st.caption(f"{len(df):,} rows returned")
    st.download_button(
        label     = "⬇️ Download CSV",
        data      = df.to_csv(index=False),
        file_name = "results.csv",
        mime      = "text/csv",
    )


# ── Page setup ────────────────────────────────────────
st.set_page_config(
    page_title = "MAFCO Sales Intelligence",
    page_icon  = "💬",
    layout     = "wide",
)

st.title("💬 MAFCO Sales Intelligence")
st.caption("Powered by Snowflake Cortex Analyst · Data: DATA_PLATFORM_DB.PROD_MARTS_SALES")

# ── Session state
if "history" not in st.session_state:
    st.session_state.history = []

# ── Suggestion buttons
st.markdown("**Try asking:**")
col1, col2, col3 = st.columns(3)

suggestions = {
    col1: [
        "Revenue by segment this year",
        "Top 10 customer groups by revenue",
    ],
    col2: [
        "Monthly revenue trend 2024",
        "Profit margin by product category",
    ],
    col3: [
        "Revenue by division this quarter",
        "Which team has the highest revenue this year",
    ],
}

clicked = None
for col, questions in suggestions.items():
    for q in questions:
        if col.button(q, key=q):
            clicked = q

st.markdown("---")

# ── Input row
col_input, col_btn = st.columns([5, 1])
with col_input:
    user_input = st.text_input(
        "question",
        value             = clicked if clicked else "",
        placeholder       = "Ask anything about MAFCO sales data...",
        label_visibility  = "collapsed",
    )
with col_btn:
    submit = st.button("Ask ▶", use_container_width=True)

question = user_input if (submit or clicked) and user_input else None

# ── Ask Cortex Analyst
if question:
    with st.spinner(f"Analyzing: '{question}'..."):
        try:
            result = ask_cortex_analyst(question)
            st.session_state.history.append({
                "question":    question,
                "sql":         result["sql"],
                "df":          result["df"],
                "explanation": result.get("explanation"),
                "error":       None,
            })
        except Exception as e:
            st.session_state.history.append({
                "question":    question,
                "sql":         None,
                "df":          None,
                "explanation": None,
                "error":       str(e),
            })

# ── Clear history button
if st.session_state.history:
    if st.button("🗑️ Clear history"):
        st.session_state.history = []
        st.rerun()

# ── Render history (newest first)
for item in reversed(st.session_state.history):
    st.markdown("---")
    st.markdown(f"**🧑 You:** {item['question']}")

    if item["error"]:
        st.error(f"❌ {item['error']}")
        st.info("💡 Try rephrasing your question or check that the semantic model stage is accessible.")
    else:
        st.markdown("**🤖 Cortex Analyst:**")
        display_result(item)

if not st.session_state.history:
    st.info("👆 Click a suggestion or type a question above to get started!")