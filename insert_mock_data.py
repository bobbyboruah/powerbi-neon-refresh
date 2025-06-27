import psycopg2
from datetime import date, timedelta
import uuid
import random

try:
    # ✅ Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="dashboard_poc_db",
        user="postgres",
        password="admin123",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL database.")

    # ✅ Insert customers (50 with segmentation fields)
    regions = ['North', 'South', 'East', 'West', 'Central']
    types = ['Retail', 'Wholesale', 'Online']
    base_signup = date(2023, 1, 1)

    customer_data = []
    for i in range(1, 51):
        cust_id = f'CUST{str(i).zfill(3)}'
        cust_name = f'Customer {i}'
        region = random.choice(regions)
        signup = base_signup + timedelta(days=random.randint(0, 500))
        cust_type = random.choice(types)
        customer_data.append((cust_id, cust_name, region, signup, cust_type))

    cursor.executemany("""
        INSERT INTO customer (customer_id, customer_name, region, signup_date, customer_type)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """, customer_data)

    print("✅ Inserted 50 customers with region/type/date.")

    # ✅ Insert inventory data (10 products)
    inventory_data = [
        ('PROD001', 'Oil Filter', 100, 30, 'Warehouse A'),
        ('PROD002', 'Brake Pads', 120, 25, 'Warehouse B'),
        ('PROD003', 'Air Filter', 90, 20, 'Warehouse A'),
        ('PROD004', 'Spark Plug', 200, 50, 'Warehouse C'),
        ('PROD005', 'Fuel Pump', 60, 15, 'Warehouse A'),
        ('PROD006', 'Clutch Kit', 45, 10, 'Warehouse B'),
        ('PROD007', 'Timing Belt', 80, 18, 'Warehouse C'),
        ('PROD008', 'Headlight', 55, 20, 'Warehouse B'),
        ('PROD009', 'Radiator', 70, 25, 'Warehouse A'),
        ('PROD010', 'Battery', 110, 40, 'Warehouse C'),
    ]

    cursor.executemany("""
        INSERT INTO inventory (item_id, item_name, quantity_on_hand, reorder_level, warehouse)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (item_id) DO NOTHING;
    """, inventory_data)

    print("✅ Inventory data inserted.")

    # ✅ Insert sales (100 random entries)
    products = [item[0] for item in inventory_data]
    customers = [c[0] for c in customer_data]
    base_date = date(2024, 6, 1)

    sales_data = []
    for _ in range(100):
        sale_id = str(uuid.uuid4())
        item_id = random.choice(products)
        customer_id = random.choice(customers)
        quantity = random.randint(1, 5)
        sale_date = base_date + timedelta(days=random.randint(0, 29))  # Spread across June 2024
        total_amount = round(random.uniform(50, 600), 2)

        sales_data.append((sale_id, item_id, customer_id, quantity, sale_date, total_amount))

    cursor.executemany("""
        INSERT INTO sales (sale_id, item_id, customer_id, quantity_sold, sale_date, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, sales_data)

    conn.commit()
    print("✅ Inserted 100 sales records.")
    
except Exception as e:
    print("❌ Error:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("🔒 Connection closed.")
