from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import psycopg2
import pandas as pd
import os
import traceback
import datetime

app = FastAPI()

@app.get("/")
def read_root():
    return {
        "message": "✅ KPI API is live!",
        "endpoints": ["/customers", "/sales", "/inventory", "/public_customers", "/public_sales"]
    }

def get_connection():
    print("🔐 Connecting with:")
    print("HOST:", os.getenv("DB_HOST"))
    print("USER:", os.getenv("DB_USER"))
    print("PASS:", os.getenv("DB_PASS"))

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT", 5432),
        sslmode=os.getenv("DB_SSLMODE", "require")
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
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == "object":
                try:
                    df[col] = df[col].apply(
                        lambda x: str(x) if isinstance(x, (pd.Timestamp, datetime.datetime, datetime.date)) else x
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
                lambda x: str(x) if isinstance(x, (pd.Timestamp, datetime.date, datetime.datetime)) else x
            )
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        print("❌ ERROR:", traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/public_customers")
def get_public_customers():
    try:
        conn = get_connection()
        df = pd.read_sql('SELECT * FROM public."public customer"', conn)
        conn.close()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == "object":
                df[col] = df[col].astype(str)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/public_sales")
def get_public_sales():
    try:
        conn = get_connection()
        df = pd.read_sql('SELECT * FROM public."public sales"', conn)
        conn.close()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == "object":
                df[col] = df[col].astype(str)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
