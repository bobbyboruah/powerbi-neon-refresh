import os
from dotenv import load_dotenv

# Load local .env only if not running in GitHub Actions
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import uuid
import random
from datetime import datetime, timedelta
import time
import signal
import sys

# === DB Connection ===
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT"),
    "sslmode": os.getenv("DB_SSLMODE", "require")  # fallback to "require"
}

# CLI command for Python main.py 
# DB_HOST="ep-royal-sunset-a849z8dn-pooler.eastus2.azure.neon.tech" \
# DB_NAME="pocdb" \
# DB_USER="pocdb_owner" \
# DB_PASS="npg_F7UzML9irYPH" \
# DB_PORT="5432" \

# === Constants ===
BATCH_SIZE = 15
INTERVAL_SECONDS = 60

fake = Faker()
regions = ['East', 'West', 'North', 'South']
car_parts = ['Brake Pad', 'Oil Filter', 'Clutch Kit', 'Spark Plug', 'Fuel Pump', 'Radiator Hose', 'Alternator', 'Timing Belt']
car_brands = ['Toyota', 'Ford', 'Hyundai', 'Mazda', 'Nissan', 'Honda', 'Jeep', 'Kia']
auto_suffixes = ['Motors', 'Auto Parts', 'Car Traders', 'Vehicle Supplies', 'Spare Mart']

# === Graceful Shutdown Flag ===
running = True
def handle_exit(sig, frame):
    global running
    print("\n🛑 Stopping data generator...")
    running = False
signal.signal(signal.SIGINT, handle_exit)

# === Create Tables (if not exist) ===
def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poc_customer (
                customer_id UUID PRIMARY KEY,
                customer_name TEXT,
                region TEXT,
                signup_date DATE
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poc_inventory (
                sku TEXT PRIMARY KEY,
                product_name TEXT,
                stock_qty INT,
                reorder_level INT,
                last_updated TIMESTAMP
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poc_sales (
                sale_id UUID PRIMARY KEY,
                customer_id UUID REFERENCES poc_customer(customer_id),
                product_sku TEXT REFERENCES poc_inventory(sku),
                quantity INT,
                sale_date DATE,
                revenue NUMERIC
            )""")
        conn.commit()

# === Initial Cleanup ===
def reset_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM poc_sales")
        cur.execute("DELETE FROM poc_inventory")
        cur.execute("DELETE FROM poc_customer")
        conn.commit()
    print("🧹 All tables cleared (only once at startup).\n")

# === Main Data Generator ===
def generate_and_insert(conn):
    with conn.cursor() as cur:
        print(f"\n🔄 Generating new batch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # --- Customers ---
        customers = []
        for _ in range(BATCH_SIZE):
            cid = str(uuid.uuid4())
            name = f"{random.choice(car_brands)} {random.choice(auto_suffixes)}"
            region = random.choice(regions)
            signup_date = fake.date_between(start_date='-2y', end_date='today')
            customers.append((cid, name, region, signup_date))

        execute_values(
            cur,
            "INSERT INTO poc_customer (customer_id, customer_name, region, signup_date) VALUES %s ON CONFLICT DO NOTHING",
            customers
        )

        # --- Inventory ---
        inventory = []
        skus = []
        for _ in range(BATCH_SIZE):
            sku = f"PART-{random.randint(1000, 9999)}"
            product_name = f"{random.choice(car_parts)} for {random.choice(car_brands)}"
            stock_qty = random.randint(0, 5)
            reorder_level = random.randint(10, 30)
            last_updated = datetime.now() - timedelta(days=random.randint(0, 15))
            inventory.append((sku, product_name, stock_qty, reorder_level, last_updated))
            skus.append(sku)

        execute_values(
            cur,
            "INSERT INTO poc_inventory (sku, product_name, stock_qty, reorder_level, last_updated) VALUES %s ON CONFLICT (sku) DO NOTHING",
            inventory
        )

        # --- Sales ---
        sales = []
        for _ in range(BATCH_SIZE):
            sid = str(uuid.uuid4())
            cust_id = random.choice(customers)[0]
            sku = random.choice(skus)
            quantity = random.randint(1, 4)
            sale_date = fake.date_between(start_date='-6mo', end_date='today')
            revenue = round(random.uniform(80, 1200), 2)
            sales.append((sid, cust_id, sku, quantity, sale_date, revenue))

        execute_values(
            cur,
            "INSERT INTO poc_sales (sale_id, customer_id, product_sku, quantity, sale_date, revenue) VALUES %s ON CONFLICT DO NOTHING",
            sales
        )

        conn.commit()

        # Show Totals
        cur.execute("SELECT COUNT(*) FROM poc_customer")
        customer_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM poc_inventory")
        inventory_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM poc_sales")
        sales_count = cur.fetchone()[0]

        print(f"✅ Totals → Customers: {customer_count}, Inventory: {inventory_count}, Sales: {sales_count}")
        print("⏳ Waiting for next batch...\n")

# === Main Loop ===
def main():
    conn = psycopg2.connect(**DB_PARAMS)
    ensure_tables(conn)
    reset_tables(conn)

    while running:
        try:
            generate_and_insert(conn)
            time.sleep(INTERVAL_SECONDS)
        except Exception as e:
            print(f"❌ Error during insert: {e}")
            conn.rollback()

    conn.close()
    print("✅ Disconnected from DB.")

# === Entry ===
if __name__ == "__main__":
    main()
