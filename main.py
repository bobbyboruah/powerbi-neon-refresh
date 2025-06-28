from fastapi import FastAPI
import psycopg2
from faker import Faker
import uuid
import random
from datetime import datetime, timedelta

app = FastAPI()  # For Render: ASGI app must be named "app"

@app.get("/")
def root():
    return {"status": "FastAPI is live ✅", "endpoint": "/seed-data"}

@app.post("/seed-data")
def seed_data():
    # --- Connect to DB ---
    conn = psycopg2.connect(
        host="ep-royal-sunset-a849z8dn-pooler.eastus2.azure.neon.tech",
        dbname="pocdb",
        user="pocdb_owner",
        password="npg_F7UzML9irYPH",
        port="5432",
        sslmode="require"
    )

    cur = conn.cursor()
    fake = Faker()

    # --- Step 1: Clean old data ---
    print("🧹 Cleaning old data...")
    cur.execute("DELETE FROM poc_sales")
    cur.execute("DELETE FROM poc_customer")
    cur.execute("DELETE FROM poc_inventory")
    conn.commit()

    # --- Step 2: Insert Customers ---
    print("👥 Inserting customers...")
    customers = []
    regions = ['East', 'West', 'North', 'South']
    for _ in range(150):
        cid = str(uuid.uuid4())
        name = fake.name()
        region = random.choice(regions)
        signup_date = fake.date_between(start_date='-2y', end_date='today')
        customers.append((cid, name, region, signup_date))

    cur.executemany(
        "INSERT INTO poc_customer (customer_id, customer_name, region, signup_date) VALUES (%s, %s, %s, %s)",
        customers
    )
    conn.commit()

    # --- Step 3: Insert Inventory ---
    print("📦 Inserting inventory...")
    skus = []
    inventory = []
    for i in range(150):
        sku = f"SKU-{1000 + i}"
        name = fake.word().capitalize() + " Part"
        stock_qty = random.randint(0, 5)
        reorder_level = random.randint(10, 50)
        last_updated = datetime.now() - timedelta(days=random.randint(0, 30))
        skus.append(sku)
        inventory.append((sku, name, stock_qty, reorder_level, last_updated))

    cur.executemany(
        "INSERT INTO poc_inventory (sku, product_name, stock_qty, reorder_level, last_updated) VALUES (%s, %s, %s, %s, %s)",
        inventory
    )
    conn.commit()

    # --- Step 4: Insert Sales ---
    print("💸 Inserting sales...")
    sales = []
    for _ in range(150):
        sid = str(uuid.uuid4())
        cust_id = random.choice(customers)[0]
        sku = random.choice(skus)
        quantity = random.randint(1, 10)
        sale_date = fake.date_between(start_date='-1y', end_date='today')
        revenue = round(random.uniform(50, 500), 2)
        sales.append((sid, cust_id, sku, quantity, sale_date, revenue))

    cur.executemany(
        "INSERT INTO poc_sales (sale_id, customer_id, product_sku, quantity, sale_date, revenue) VALUES (%s, %s, %s, %s, %s, %s)",
        sales
    )
    conn.commit()

    cur.close()
    conn.close()
    print("✅ Data inserted successfully!")

    return {"message": "✅ Seed complete", "customers": len(customers), "sales": len(sales), "inventory": len(inventory)}

# --- Optional: Run locally ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
