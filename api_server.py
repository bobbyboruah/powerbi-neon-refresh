# api_server.py

from flask import Flask, jsonify
from faker import Faker
import uuid
import random
from datetime import datetime, timedelta
import threading
import time

app = Flask(__name__)
fake = Faker()

# In-memory data stores
customer_data = []
inventory_data = []
sales_data = []

# Constants
BATCH_SIZE = 15
INTERVAL_SECONDS = 60
regions = ['East', 'West', 'North', 'South']
car_parts = ['Brake Pad', 'Oil Filter', 'Clutch Kit', 'Spark Plug', 'Fuel Pump', 'Radiator Hose', 'Alternator', 'Timing Belt']
car_brands = ['Toyota', 'Ford', 'Hyundai', 'Mazda', 'Nissan', 'Honda', 'Jeep', 'Kia']
auto_suffixes = ['Motors', 'Auto Parts', 'Car Traders', 'Vehicle Supplies', 'Spare Mart']


def generate_mock_data():
    global customer_data, inventory_data, sales_data

    while True:
        print(f"🔄 Generating new batch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        customers = []
        inventory = []
        sales = []

        # Generate customers
        for _ in range(BATCH_SIZE):
            cid = str(uuid.uuid4())
            name = f"{random.choice(car_brands)} {random.choice(auto_suffixes)}"
            region = random.choice(regions)
            signup_date = fake.date_between(start_date='-2y', end_date='today')
            customers.append({
                "customer_id": cid,
                "customer_name": name,
                "region": region,
                "signup_date": signup_date.isoformat()
            })

        # Generate inventory
        skus = []
        for _ in range(BATCH_SIZE):
            sku = f"PART-{random.randint(1000, 9999)}"
            product_name = f"{random.choice(car_parts)} for {random.choice(car_brands)}"
            stock_qty = random.randint(0, 5)
            reorder_level = random.randint(10, 30)
            last_updated = (datetime.now() - timedelta(days=random.randint(0, 15))).isoformat()
            inventory.append({
                "sku": sku,
                "product_name": product_name,
                "stock_qty": stock_qty,
                "reorder_level": reorder_level,
                "last_updated": last_updated
            })
            skus.append(sku)

        # Generate sales
        for _ in range(BATCH_SIZE):
            sale_id = str(uuid.uuid4())
            cust_id = random.choice(customers)["customer_id"]
            sku = random.choice(skus)
            quantity = random.randint(1, 4)
            sale_date = fake.date_between(start_date='-6mo', end_date='today').isoformat()
            revenue = round(random.uniform(80, 1200), 2)
            sales.append({
                "sale_id": sale_id,
                "customer_id": cust_id,
                "product_sku": sku,
                "quantity": quantity,
                "sale_date": sale_date,
                "revenue": revenue
            })

        # Overwrite the in-memory batches
        customer_data = customers
        inventory_data = inventory
        sales_data = sales

        time.sleep(INTERVAL_SECONDS)


# API Endpoints
@app.route('/customers', methods=['GET'])
def get_customers():
    return jsonify(customer_data)

@app.route('/inventory', methods=['GET'])
def get_inventory():
    return jsonify(inventory_data)

@app.route('/sales', methods=['GET'])
def get_sales():
    return jsonify(sales_data)


if __name__ == '__main__':
    print("🚀 Starting API server on http://localhost:5000 ...")
    threading.Thread(target=generate_mock_data, daemon=True).start()
    app.run(debug=True)
