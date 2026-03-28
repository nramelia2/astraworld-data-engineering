import pandas as pd
from config import engine

# Customers
def clean_customers():
    print("\n[CLEAN] customers")

    df = pd.read_sql("SELECT * FROM customers_raw", engine)

    df['name'] = df['name'].astype(str).str.strip()

    df['dob'] = pd.to_datetime(df['dob'], errors='coerce')

    def check_quality(row):
        if pd.isna(row['dob']):
            return 'MISSING_DOB'
        elif row['dob'] < pd.Timestamp('1920-01-01'):
            return 'INVALID_DOB'
        elif row['name'] == '' or pd.isna(row['name']):
            return 'MISSING_NAME'
        elif 'PT' in row['name'].upper():
            return 'COMPANY'
        else:
            return 'VALID'

    df['data_quality'] = df.apply(check_quality, axis=1)

    df.loc[df['dob'] < '1920-01-01', 'dob'] = None

    df['customer_type'] = df['name'].apply(
        lambda x: 1 if 'PT' in str(x).upper() else 0
    )

    df['is_active'] = df['data_quality'].apply(
        lambda x: 1 if x in ['VALID', 'COMPANY'] else 0
    )

    df.to_sql('customers_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] customers_clean created")

# Sales
def clean_sales():
    print("\n[CLEAN] sales")

    df = pd.read_sql("SELECT * FROM sales_raw", engine)

    df['price'] = df['price'].astype(str).str.replace('.', '', regex=False)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    df = df.dropna(subset=['price'])

    df['is_active'] = 1

    df.to_sql('sales_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] sales_clean created")

# After Sales
def clean_after_sales():
    print("\n[CLEAN] after_sales")

    df = pd.read_sql("SELECT * FROM after_sales_raw", engine)

    df = df.dropna(subset=['vin', 'customer_id'])

    df['is_active'] = 1

    df.to_sql('after_sales_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] after_sales_clean created")

# Customer Addresses
def clean_customer_addresses():
    print("\n[CLEAN] customer_addresses")

    df = pd.read_sql("SELECT * FROM customer_addresses_raw", engine)

    df['city'] = df['city'].astype(str).str.title()
    df['province'] = df['province'].astype(str).str.title()
    df['address'] = df['address'].astype(str).str.strip()

    df['is_active'] = 1

    df.to_sql('customer_addresses_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] customer_addresses_clean created")