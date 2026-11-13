"""Generate a realistic retail Superstore sales dataset with intentional anomalies for pipeline testing.

Produces:
- data/sample/superstore_sales_sample.csv (tracked in Git, ~50 rows)
- data/raw/superstore_sales_raw.csv (ignored by Git, ~25,000+ rows)
"""

import csv
import os
import random
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

REGIONS_AND_STATES = {
    "South": [("Kentucky", "Henderson", "42420"), ("Florida", "Miami", "33180"), ("Georgia", "Atlanta", "30301"), ("North Carolina", "Charlotte", "28202")],
    "West": [("California", "Los Angeles", "90036"), ("California", "San Francisco", "94102"), ("Washington", "Seattle", "98101"), ("Arizona", "Phoenix", "85001")],
    "Central": [("Illinois", "Chicago", "60653"), ("Texas", "Houston", "77002"), ("Texas", "Dallas", "75201"), ("Michigan", "Detroit", "48201")],
    "East": [("New York", "New York City", "10024"), ("Pennsylvania", "Philadelphia", "19140"), ("Massachusetts", "Boston", "02116"), ("Ohio", "Columbus", "43215")]
}

SEGMENTS = ["Consumer", "Corporate", "Home Office"]
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]

CATEGORIES = {
    "Furniture": {
        "Bookcases": ["Bush Somerset Collection Bookcase", "Riverside Palais Royal Lawyers Bookcase", "Sauder Camden County Bookcase"],
        "Chairs": ["HON 5400 Series Task Chair", "Novimex Swivel Mesh Chair", "Global Troy Executive Leather Chair"],
        "Furnishings": ["Eldon Expressions Desk Tray", "Deflect-o DuraMat Clear Desk Pad", "Howard Miller Wall Clock"],
        "Tables": ["Chromcraft Round Conference Table", "Bevis Rectangular Conference Table", "Balt Training Table"]
    },
    "Office Supplies": {
        "Appliances": ["Kensington Orbit Wireless Trackball", "Fellowes Powershred Shredder", "Hoover Commercial Canister Vacuum"],
        "Art": ["Newell 322 Ballpoint Pen", "Boston Heavy-Duty Pencil Sharpener", "Dixon Ticonderoga Pencils"],
        "Binders": ["Avery Heavy Duty Binders", "GBC VeloBind System", "Wilson Jones Hanging Folders"],
        "Envelopes": ["#10 Self-Seal White Envelopes", "Kraft Clasp Envelopes", "Tyvek Tear-Proof Envelopes"],
        "Fasteners": ["Advantus Map Tacks", "Acco Perma 4-Prong Fasteners", "Staples Push Pins"],
        "Paper": ["Xerox 196 Multi-Use Paper", "Hammermill Copy Plus", "Southworth Fine Paper"],
        "Storage": ["Sterilite Modular Storage Drawers", "Fellowes Stor/Drawer Steel Plus", "Tennsco Lockers"]
    },
    "Technology": {
        "Accessories": ["Logitech Wireless Mouse M325", "SanDisk 64GB Flash Drive", "Belkin 7-Outlet Surge Protector"],
        "Copiers": ["Canon imageCLASS D530 Copier", "Hewlett Packard LaserJet Pro", "Sharp AL-1530CS Copier"],
        "Machines": ["Star Micronics Receipt Printer", "Epson TM-T88V Thermal Printer", "Zebra Thermal Label Printer"],
        "Phones": ["Apple iPhone 13 Pro 128GB", "Cisco SPA 504G IP Phone", "Samsung Galaxy S22 256GB"]
    }
}

FIRST_NAMES = ["Claire", "Brosina", "Andrew", "Harold", "Pete", "Alejandro", "Zuschuss", "Ken", "Sandra", "Emily", "Eric", "Linda", "John", "Maria", "David", "Sarah", "Michael", "James", "Rachel", "Robert"]
LAST_NAMES = ["Gute", "Hoffman", "Allen", "Pawlan", "Kroll", "Ballentine", "Donatelli", "Black", "Flanagan", "Phan", "Hoffmann", "Cacic", "Miller", "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Wilson"]

# Pre-generate 100 fixed customers
CUSTOMERS = []
for i in range(1, 151):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    cid = f"{fn[:2].upper()}-{10000 + i}"
    CUSTOMERS.append({"customer_id": cid, "customer_name": f"{fn} {ln}", "segment": random.choice(SEGMENTS)})

# Pre-generate catalog of products
PRODUCTS = []
prod_id_counter = 1000
for cat, subcats in CATEGORIES.items():
    prefix = cat[:3].upper()
    for subcat, names in subcats.items():
        sub_prefix = subcat[:2].upper()
        for name in names:
            prod_id_counter += 1
            PRODUCTS.append({
                "product_id": f"{prefix}-{sub_prefix}-{prod_id_counter}",
                "category": cat,
                "sub_category": subcat,
                "product_name": name,
                "base_price": round(random.uniform(5.0, 750.0), 2)
            })

def generate_row(row_id: int, start_date: datetime, end_date: datetime, inject_anomalies: bool = True):
    cust = random.choice(CUSTOMERS)
    prod = random.choice(PRODUCTS)
    region = random.choice(list(REGIONS_AND_STATES.keys()))
    state, city, postal_code = random.choice(REGIONS_AND_STATES[region])
    
    order_dt = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    ship_delay = random.choice([1, 2, 3, 4, 5, 7])
    ship_dt = order_dt + timedelta(days=ship_delay)
    
    order_id = f"CA-{order_dt.year}-{random.randint(100000, 199999)}"
    ship_mode = random.choice(SHIP_MODES)
    
    quantity = random.randint(1, 12)
    discount = random.choice([0.0, 0.0, 0.1, 0.15, 0.2, 0.3, 0.5])
    sales = round(prod["base_price"] * quantity * (1.0 - discount), 2)
    margin_rate = random.uniform(-0.15, 0.35)
    profit = round(sales * margin_rate, 2)
    
    # Date formatting variations
    date_format_choice = random.random()
    if date_format_choice < 0.6:
        order_date_str = order_dt.strftime("%Y-%m-%d")
        ship_date_str = ship_dt.strftime("%Y-%m-%d")
    elif date_format_choice < 0.9:
        order_date_str = order_dt.strftime("%m/%d/%Y")
        ship_date_str = ship_dt.strftime("%m/%d/%Y")
    else:
        order_date_str = order_dt.strftime("%d-%m-%Y")
        ship_date_str = ship_dt.strftime("%d-%m-%Y")
        
    cust_name = cust["customer_name"]
    postal = postal_code
    sales_str = f"{sales:.2f}"
    
    # Injected anomalies for data quality & cleaning exercises
    if inject_anomalies and random.random() < 0.03:
        anomaly_type = random.choice(["whitespace", "null_cust", "null_postal", "dollar_sign", "zero_qty", "negative_sales"])
        if anomaly_type == "whitespace":
            cust_name = f"  {cust_name}  "
        elif anomaly_type == "null_cust":
            cust_name = ""
        elif anomaly_type == "null_postal":
            postal = ""
        elif anomaly_type == "dollar_sign":
            sales_str = f"${sales:.2f}"
        elif anomaly_type == "zero_qty":
            quantity = 0
        elif anomaly_type == "negative_sales":
            sales_str = f"-{sales:.2f}"
            
    return {
        "row_id": row_id,
        "order_id": order_id,
        "order_date": order_date_str,
        "ship_date": ship_date_str,
        "ship_mode": ship_mode,
        "customer_id": cust["customer_id"],
        "customer_name": cust_name,
        "segment": cust["segment"],
        "country": "United States",
        "city": city,
        "state": state,
        "postal_code": postal,
        "region": region,
        "product_id": prod["product_id"],
        "category": prod["category"],
        "sub_category": prod["sub_category"],
        "product_name": prod["product_name"],
        "sales": sales_str,
        "quantity": quantity,
        "discount": discount,
        "profit": profit
    }

def main():
    fieldnames = [
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "customer_name", "segment", "country", "city",
        "state", "postal_code", "region", "product_id", "category",
        "sub_category", "product_name", "sales", "quantity", "discount", "profit"
    ]
    
    os.makedirs("data/sample", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)
    
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    # 1. Generate sample dataset (50 rows, clean and minimal anomalies)
    sample_rows = [generate_row(i, start_date, end_date, inject_anomalies=False) for i in range(1, 51)]
    sample_path = os.path.join("data", "sample", "superstore_sales_sample.csv")
    with open(sample_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_rows)
    print(f"Generated sample dataset: {sample_path} ({len(sample_rows)} rows)")
    
    # 2. Generate full raw dataset (~30,000 rows with duplicates & anomalies)
    raw_rows = []
    num_orders = 30000
    for i in range(1, num_orders + 1):
        row = generate_row(i, start_date, end_date, inject_anomalies=True)
        raw_rows.append(row)
        # Inject occasional duplicate rows (~1.5% chance)
        if random.random() < 0.015:
            raw_rows.append(dict(row))
            
    raw_path = os.path.join("data", "raw", "superstore_sales_raw.csv")
    with open(raw_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(raw_rows)
    print(f"Generated full raw dataset: {raw_path} ({len(raw_rows)} rows)")

if __name__ == "__main__":
    main()
