import pandas as pd
from config import engine

# Customers
def clean_customers():
    print("\n[CLEAN] customers")

    df = pd.read_sql("SELECT * FROM customers_raw", engine)

    df['name'] = df['name'].str.strip()
    df['dob'] = pd.to_datetime(df['dob'], errors='coerce')

    def check_quality(row):
        if pd.isna(row['name']):
            return 'MISSING_NAME'

        if str(row['name']).startswith('PT'):
            return 'COMPANY'

        if pd.isna(row['dob']):
            return 'MISSING_DOB'

        if row['dob'] == pd.Timestamp('1900-01-01'):
            return 'INVALID_DOB'

        return 'VALID'

    df['data_quality'] = df.apply(check_quality, axis=1)

    df.loc[df['dob'] == pd.Timestamp('1900-01-01'), 'dob'] = None

    df.to_sql('customers_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] customers_clean created")

# Sales
def clean_sales():
    print("\n[CLEAN] sales")

    df = pd.read_sql("SELECT * FROM sales_raw", engine)

    df['price'] = df['price'].str.replace('.', '', regex=False)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    df = df.dropna(subset=['price'])

    df.to_sql('sales_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] sales_clean created")

# After Sales
def clean_after_sales():
    print("\n[CLEAN] after_sales")

    df = pd.read_sql("SELECT * FROM after_sales_raw", engine)

    df = df.dropna(subset=['vin', 'customer_id'])

    df.to_sql('after_sales_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] after_sales_clean created")

# Customer Addresses
def clean_customer_addresses():
    print("\n[CLEAN] customer_addresses")

    df = pd.read_sql("SELECT * FROM customer_addresses_raw", engine)

    df['city'] = df['city'].str.title()
    df['province'] = df['province'].str.title()
    df['address'] = df['address'].str.strip()

    df.to_sql('customer_addresses_clean', engine, if_exists='replace', index=False)

    print("[SUCCESS] customer_addresses_clean created")