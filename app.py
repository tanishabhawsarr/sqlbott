import json
import struct
import pyodbc
import os
from config import DB_CONFIG
from azure.identity import ClientSecretCredential
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

# ---------------- DB CONNECTION ----------------
def get_db_connection():
    try:
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

        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_NAME") 

        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{server},1433;"
            f"Database={database};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )

        conn = pyodbc.connect(
            conn_str,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
        )
        return conn

    except Exception as e:
        raise Exception(f"DB Connection Failed: {e}")


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

    compressed_schema = json.dumps(schema_info, separators=(",", ":"))

    system_prompt = """
You are an expert SQL query generator for a warehouse management system. Your role is to generate accurate SQL queries based on user questions about sales, inventory, and purchases.
 
**DATABASE SCHEMA:**
- Table: `[dbo].[itemledgerentries]` (alias: `ile`)
  - Columns: `itemNumber`, `entryType`, `salesAmountActual`, `quantity`, `postingDate`
- Table: `[dbo].[items]` (alias: `i`)
  - Columns: `number`, `displayName`
 
**QUERY PATTERNS TO FOLLOW:**
 
1. **Total Sales for Specific Item:**
   - JOIN items table
   - Filter by `entryType = 'Sale'`
   - Filter by specific `displayName`
   - Filter by year using `YEAR(postingDate)`
   - Use `SUM(ile.salesAmountActual)` with `GROUP BY i.displayName`
 
2. **Total Overall Sales:**
   - No JOIN needed
   - Filter by `entryType = 'Sale'`
   - Filter by year using `YEAR(postingDate)`
   - Use `SUM(salesAmountActual)`
 
3. **Raw Material Purchases:**
   - No JOIN needed
   - Filter by `entryType = 'Purchase'`
   - Filter by year using `YEAR(postingDate)`
   - Use `SUM(quantity)`
 
4. **Stock Level Calculations:**
   - JOIN items table
   - Use appropriate `entryType IN` combinations:
     - For finished goods stock: `('Output','Sale')`
     - For raw material stock: `('Purchase','Consumption')`
   - Filter by specific `displayName`
   - Filter by year using `YEAR(postingDate)`
   - Use `SUM(ile.quantity)` with `GROUP BY i.displayName`
 
**QUERY RULES:**
1. Always use table aliases: `ile` for itemledgerentries, `i` for items
2. NEVER prefix tables with database name.
   Always use ONLY schema-qualified names:
   Example: dbo.itemledgerentries, dbo.items
3. For item-specific queries, JOIN with items table: `ON i.number = ile.itemNumber`
4. Use `YEAR(ile.postingDate)` for date filtering
5. Include `GROUP BY i.displayName` when using aggregates with item names
6. Return ONLY the SQL query, no explanations or additional text
 
**EXAMPLES:**
- "Sales of [item] for [year]" → JOIN + entryType='Sale' + filter displayName + year filter
- "Total sales for [year]" → No JOIN + entryType='Sale' + year filter
- "Quantity purchased for [year]" → No JOIN + entryType='Purchase' + year filter
- "Stock of [item] for [year]" → JOIN + appropriate entryType IN + filter displayName + year filter
 
Generate precise SQL queries following these exact patterns.
    """

    user_prompt = f"""
Schema: {compressed_schema}
Question: {question}
Return only SQL.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )

    sql = response.choices[0].message.content.strip()
    return sql.replace("```sql", "").replace("```", "")


# ---------------- EXECUTE SQL ----------------
def execute_sql(sql, cursor):
    cursor.execute(sql)
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ---------------- LOCAL TEST ----------------
if __name__ == "__main__":
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
            EXEC sys.sp_set_session_context 
                @key   = N'emailid',
                @value = ?
        """, ("tanisha.b@addendanalytics.com",))

    cursor.execute("SELECT SESSION_CONTEXT(N'emailid')")
    print(cursor.fetchone())
    cursor.execute("SELECT * FROM UserRLS")
    print(cursor.fetchone())

    schema = get_schema_info(cursor)
    question = "Total sales for year 2024"

    sql = generate_sql(question, schema)
    print("Generated SQL:\n", sql)

    result = execute_sql(sql, cursor)
    print("Result:\n", result)
