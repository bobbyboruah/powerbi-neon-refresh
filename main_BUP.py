import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random

# ----- 1. Connection String -----
conn = psycopg2.connect(
    host="ep-small-cell-a89t5ohq-pooler.eastus2.azure.neon.tech",
    dbname="pocdb",
    user="pocdb_owner",
    password="npg_fJ8UG1Yijope",  
    port="5432",
    sslmode="require"
)


fake = Faker()
cur = conn.cursor()

# ----- 2. Insert Customers -----
for _ in range(10):
    name = fake.name()
    region = random.choice(['North', 'South', 'East', 'West'])
    join_date = fake.date_between(start_date='-1y', end_date='today')
    cur.execute(
        "INSERT INTO customers (customer_name, region, join_date) VALUES (%s, %s, %s)",
        (name, region, join_date)
    )

# ----- 3. Insert Inventory -----
for i in range(5):
    sku = f'SKU-{1000 + i}'
    name = fake.word().capitalize() + " Part"
    qty = random.randint(10, 100)
    reorder = random.randint(5, 20)
    last_updated = datetime.now()
    cur.execute(
        "INSERT INTO inventory (product_sku, product_name, quantity_on_hand, reorder_level, last_updated) VALUES (%s, %s, %s, %s, %s)",
        (sku, name, qty, reorder, last_updated)
    )

# ----- 4. Insert Sales -----
cur.execute("SELECT customer_id FROM customers")
customer_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT product_sku FROM inventory")
skus = [row[0] for row in cur.fetchall()]

for _ in range(20):
    cust = random.choice(customer_ids)
    sku = random.choice(skus)
    sale_date = fake.date_between(start_date='-30d', end_date='today')
    qty = random.randint(1, 5)
    amount = qty * random.uniform(20, 150)
    cur.execute(
        "INSERT INTO sales (customer_id, product_sku, sale_date, quantity, sale_amount) VALUES (%s, %s, %s, %s, %s)",
        (cust, sku, sale_date, qty, round(amount, 2))
    )

# ----- 5. Commit and Close -----
conn.commit()
cur.close()
conn.close()

print("✅ Data inserted successfully into Neon DB.")
