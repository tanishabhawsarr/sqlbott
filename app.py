import json
import struct
import pyodbc
import os

from flask import Flask, request, jsonify
from azure.identity import ClientSecretCredential
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ---------------- DB CONNECTION ----------------
def get_db_connection():
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )

    token = credential.get_token(
        "https://database.windows.net/.default"
    ).token

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(
        f"<I{len(token_bytes)}s",
        len(token_bytes),
        token_bytes
    )

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{os.getenv('DB_SERVER')},1433;"
        f"Database={os.getenv('DB_NAME')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
    )

# ---------------- SCHEMA ----------------
def get_schema_info(cursor):
    schema_info = {}
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
    """)

    for schema, table in cursor.fetchall():
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
        """, (schema, table))

        schema_info[f"{schema}.{table}"] = [
            {"name": c[0], "type": c[1]} for c in cursor.fetchall()
        ]
    return schema_info

# ---------------- SQL GENERATION ----------------
def generate_sql(question, schema_info):
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        api_version="2024-12-01-preview",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
    )

    system_prompt = """
You generate SQL Server queries only.
Rules:
- NO backticks
- Use dbo schema
- Use COALESCE(SUM(x),0)
- Output ONLY SQL
"""

    user_prompt = f"""
Schema: {json.dumps(schema_info)}
Question: {question}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )

    return (
        response.choices[0].message.content
        .replace("```sql", "")
        .replace("```", "")
        .replace("`", "")
        .strip()
    )

# ---------------- EXECUTE SQL ----------------
def execute_sql(sql, cursor):
    cursor.execute(sql)
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

# ---------------- HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}

# ---------------- MAIN API ----------------
@app.route("/query", methods=["POST"])
def query():
    data = request.json
    question = data.get("question")
    email = data.get("emailid")

    if not question or not email:
        return jsonify({"error": "question and emailid required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    # Set RLS context
    cursor.execute("""
        EXEC sys.sp_set_session_context
            @key=N'emailid',
            @value=?
    """, (email,))

    schema = get_schema_info(cursor)
    sql = generate_sql(question, schema)
    result = execute_sql(sql, cursor)

    return jsonify({
        "sql": sql,
        "result": result
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
