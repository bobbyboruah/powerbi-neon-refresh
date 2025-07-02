import requests
import psycopg2
from psycopg2.extras import execute_values
import time
import sys
import signal
from datetime import datetime

# === Configuration ===
RESET_ON_START = True
API_BASE_URL = "http://localhost:5000"
POLL_INTERVAL = 60  # in seconds

# === Neon PostgreSQL Connection ===
DB_PARAMS = {
    "host": "ep-royal-sunset-a849z8dn-pooler.eastus2.azure.neon.tech",
    "dbname": "pocdb",
    "user": "pocdb_owner",
    "password": "npg_F7UzML9irYPH",
    "port": "5432",
    "sslmode": "require"
}

# === Graceful Shutdown ===
running = True
def handle_sigint(sig, frame):
    global running
    print("\n🛑 Stopping API sync gracefully...")
    running = False
signal.signal(signal.SIGINT, handle_sigint)

# === Helper to Create Tables (Optional) ===
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

# === Helper to Reset Tables ===
def reset_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM poc_sales")
        cur.execute("DELETE FROM poc_inventory")
        cur.execute("DELETE FROM poc_customer")
        conn.commit()
    print("🧹 Reset: All tables cleaned.\n")

# === Helper to Fetch JSON Safely ===
def fetch_data(endpoint):
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"❌ Failed to fetch {endpoint}: {e}")
        return []

# === Main Sync Loop ===
def main_loop():
    conn = psycopg2.connect(**DB_PARAMS)
    ensure_tables(conn)
    if RESET_ON_START:
        reset_tables(conn)

    print(f"🚀 Starting Data Sync to Neon @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    while running:
        try:
            with conn.cursor() as cur:
                print(f"🔄 Syncing at {datetime.now().strftime('%H:%M:%S')}...")

                # === Fetch Full Data in One Go ===
                customers = fetch_data("customers")
                inventory = fetch_data("inventory")
                sales = fetch_data("sales")

                # === Insert Customers First ===
                if customers:
                    customer_tuples = [(c["customer_id"], c["customer_name"], c["region"], c["signup_date"]) for c in customers]
                    execute_values(cur,
                        "INSERT INTO poc_customer (customer_id, customer_name, region, signup_date) VALUES %s ON CONFLICT DO NOTHING",
                        customer_tuples)

                # === Insert Inventory Second ===
                if inventory:
                    inventory_tuples = [(i["sku"], i["product_name"], i["stock_qty"], i["reorder_level"], i["last_updated"]) for i in inventory]
                    execute_values(cur,
                        "INSERT INTO poc_inventory (sku, product_name, stock_qty, reorder_level, last_updated) VALUES %s ON CONFLICT (sku) DO NOTHING",
                        inventory_tuples)

                # === Insert Sales Last ===
                if sales:
                    sales_tuples = [(s["sale_id"], s["customer_id"], s["product_sku"], s["quantity"], s["sale_date"], s["revenue"]) for s in sales]
                    execute_values(cur,
                        "INSERT INTO poc_sales (sale_id, customer_id, product_sku, quantity, sale_date, revenue) VALUES %s ON CONFLICT DO NOTHING",
                        sales_tuples)

                conn.commit()
                print(f"✅ Synced: {len(customers)} customers, {len(inventory)} inventory, {len(sales)} sales.")
                print("⏳ Waiting before next sync...\n")

        except Exception as e:
            print(f"❌ Error during sync: {e}")
            conn.rollback()

        time.sleep(POLL_INTERVAL)

    conn.close()
    print("✅ Disconnected from DB.")

# === Entry Point ===
if __name__ == "__main__":
    main_loop()
