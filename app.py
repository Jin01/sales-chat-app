import streamlit as st
import snowflake.connector
import pandas as pd
import requests
import datetime
import hashlib
import base64

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt

# ── Config ────────────────────────────────────────────
SNOWFLAKE_ACCOUNT    = "NEA23255.east-us-2.azure"
SNOWFLAKE_USER       = "JHUANG"
SNOWFLAKE_PASSWORD   = st.secrets["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE  = "cortex_wh"
SNOWFLAKE_DATABASE   = "DATA_PLATFORM_DB"
SNOWFLAKE_SCHEMA     = "PROD_MARTS_SALES"
SEMANTIC_MODEL_STAGE = "@ANALYTIC_DB.PUBLIC.CORTEX_STAGE/semantic_model.yaml"
CORTEX_ANALYST_URL   = (
    "https://NEA23255.east-us-2.azure.snowflakecomputing.com"
    "/api/v2/cortex/analyst/message"
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


def get_jwt_token() -> str:
    """
    Generate a JWT token using the RSA private key stored in Streamlit secrets.

    Setup steps:
    1. Generate a key pair in Snowflake:
       ALTER USER JHUANG SET RSA_PUBLIC_KEY='<your_public_key>';

    2. Add private key to .streamlit/secrets.toml:
       SNOWFLAKE_PRIVATE_KEY = \"\"\"-----BEGIN PRIVATE KEY-----
       MIIEvQ...
       -----END PRIVATE KEY-----\"\"\"
    """
    private_key_str = st.secrets["SNOWFLAKE_PRIVATE_KEY"]
    private_key = serialization.load_pem_private_key(
        private_key_str.encode(),
        password = None,
        backend  = default_backend(),
    )

    # compute public key fingerprint (SHA256)
    public_key     = private_key.public_key()
    public_key_der = public_key.public_bytes(
        encoding = serialization.Encoding.DER,
        format   = serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    sha256_hash   = hashlib.sha256(public_key_der).digest()
    public_key_fp = "SHA256:" + base64.b64encode(sha256_hash).decode("utf-8")

    # Snowflake expects account in format: ACCOUNT.REGION (dots/dashes → underscores)
    account_clean  = SNOWFLAKE_ACCOUNT.upper().replace("-", "_").replace(".", "_")
    qualified_user = f"{account_clean}.{SNOWFLAKE_USER.upper()}"

    now = datetime.datetime.utcnow()
    payload = {
        "iss": f"{qualified_user}.{public_key_fp}",
        "sub": qualified_user,
        "iat": now,
        "exp": now + datetime.timedelta(hours=1),
    }

    token = jwt.encode(payload, private_key, algorithm="RS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def ask_cortex_analyst(question: str) -> dict:
    """
    Call the Cortex Analyst REST API using JWT authentication.
    Reads the semantic model YAML from the Snowflake stage.
    """
    token = get_jwt_token()

    payload = {
        "messages": [
            {
                "role":    "user",
                "content": [{"type": "text", "text": question}],
            }
        ],
        "semantic_model_file": SEMANTIC_MODEL_STAGE,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }

    response = requests.post(
        CORTEX_ANALYST_URL,
        json    = payload,
        headers = headers,
        timeout = 120,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Cortex Analyst API error {response.status_code}:\n{response.text}"
        )

    data    = response.json()
    content = data["message"]["content"]

    # extract natural language explanation
    text_items  = [item for item in content if item["type"] == "text"]
    explanation = " ".join(item.get("text", "") for item in text_items) or None

    # extract SQL — if absent, Cortex returned a clarification instead
    sql_items = [item for item in content if item["type"] == "sql"]
    if not sql_items:
        return {"sql": None, "df": None, "explanation": explanation}

    sql = sql_items[0]["statement"]
    df  = run_query(sql)

    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass

    return {"sql": sql, "df": df, "explanation": explanation}


def display_result(item: dict):
    """Render explanation, SQL expander, chart, table, and download button."""

    if item.get("explanation"):
        st.markdown(f"_{item['explanation']}_")

    sql = item.get("sql")
    df  = item.get("df")

    if sql:
        with st.expander("🔍 View generated SQL"):
            st.code(sql, language="sql")

    if df is None:
        return

    if df.empty:
        st.info("No data found for this query.")
        return

    # single value → metric card
    if df.shape == (1, 1):
        value = df.iloc[0, 0]
        try:
            st.metric(label=df.columns[0], value=f"{float(value):,.2f}")
        except Exception:
            st.metric(label=df.columns[0], value=str(value))

    # two columns, second numeric → bar chart
    elif df.shape[1] == 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
        st.bar_chart(df.set_index(df.columns[0]))
        st.dataframe(df, use_container_width=True)

    # time-series columns → line chart
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

    # default → plain table
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
st.caption("Powered by Snowflake Cortex Analyst · DATA_PLATFORM_DB.PROD_MARTS_SALES")

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
        value            = clicked if clicked else "",
        placeholder      = "Ask anything about MAFCO sales data...",
        label_visibility = "collapsed",
    )
with col_btn:
    submit = st.button("Ask ▶", use_container_width=True)

question = user_input if (submit or clicked) and user_input else None

# ── Call Cortex Analyst
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

# ── Clear history
if st.session_state.history:
    if st.button("🗑️ Clear history"):
        st.session_state.history = []
        st.rerun()

# ── Render history newest first
for item in reversed(st.session_state.history):
    st.markdown("---")
    st.markdown(f"**🧑 You:** {item['question']}")

    if item["error"]:
        st.error(f"❌ {item['error']}")
        st.info("💡 Try rephrasing your question or check the semantic model stage path.")
    else:
        st.markdown("**🤖 Cortex Analyst:**")
        display_result(item)

if not st.session_state.history:
    st.info("👆 Click a suggestion or type a question above to get started!")
