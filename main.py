from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import psycopg2
import pandas as pd
import os
import traceback
import datetime  # ✅ required for datetime.date

app = FastAPI()

@app.get("/")
def read_root():
    return {
        "message": "✅ KPI API is live!",
        "endpoints": ["/customers", "/sales", "/inventory"]
    }


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT", 5432)
    )

@app.get("/sales")
def get_sales():
    try:
        conn = get_connection()
        df = pd.read_sql("SELECT * FROM sales", conn)
        conn.close()
        df["sale_date"] = df["sale_date"].astype(str)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        print("❌ ERROR:", traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/inventory")
def get_inventory():
    try:
        conn = get_connection()
        df = pd.read_sql("SELECT * FROM inventory", conn)
        conn.close()

        # ✅ Proper datetime and object column handling
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == "object":
                try:
                    df[col] = df[col].apply(
                        lambda x: str(x) if isinstance(x, (pd.Timestamp, pd.NaT, datetime.date)) else x
                    )
                except Exception:
                    pass

        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        print("❌ ERROR:", traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/customers")
def get_customers():
    try:
        conn = get_connection()
        df = pd.read_sql("SELECT * FROM customer", conn)
        conn.close()
        print("✅ Loaded customer table with", len(df), "rows")

        for col in df.columns:
            print(f"🔍 Converting column: {col} (type: {df[col].dtype})")
            df[col] = df[col].apply(
                lambda x: str(x) if isinstance(x, (pd.Timestamp, datetime.date, datetime.datetime))else x
            )

        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        print("❌ ERROR:", traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=500)


