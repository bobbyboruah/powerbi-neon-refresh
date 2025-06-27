from fastapi import FastAPI
from fastapi.responses import JSONResponse
import psycopg2
import pandas as pd

app = FastAPI()

def get_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="dashboard_poc_db",
        user="postgres",
        password="admin123",
        port=5432
    )

@app.get("/sales")
def get_sales():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM sales", conn)
    conn.close()
    return JSONResponse(content=df.to_dict(orient="records"))

@app.get("/inventory")
def get_inventory():
    conn = psycopg2.connect(
        host="localhost",
        database="dashboard_poc_db",
        user="postgres",           
        password="admin123"    
    )
    df = pd.read_sql("SELECT * FROM inventory", conn)
    conn.close()
    return df.to_dict(orient="records")

@app.get("/customers")
def get_customers():
    conn = psycopg2.connect(
        host="localhost",
        database="dashboard_poc_db",
        user="postgres",           
        password="admin123"    
    )
    df = pd.read_sql("SELECT * FROM customer", conn)
    conn.close()
    return df.to_dict(orient="records")
