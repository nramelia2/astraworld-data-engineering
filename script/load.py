import pandas as pd
import os
from config import engine
from utils import clean_date_column, clean_datetime_column

# Customers
def load_customer():
    print("\n[LOAD] customers_raw.csv")

    df = pd.read_csv('data/customers_raw.csv', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()

    print("[BEFORE]")
    print(df.head())

    # Clean date
    df['dob'] = clean_date_column(df['dob'])
    df['created_at'] = clean_datetime_column(df['created_at'])

    df['is_company'] = df['name'].str.startswith('PT', na=False)

    df.loc[df['dob'] == '1900-01-01', 'dob'] = pd.NaT

    print("[AFTER]")
    print(df.head())

    # Format ke string
    df['dob'] = df['dob'].dt.strftime('%Y-%m-%d')
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.where(pd.notnull(df), None)

    df.drop(columns=['is_company'], inplace=True)

    df.to_sql('customers_raw', engine, if_exists='replace', index=False)

    print(f"[SUCCESS] customers_raw inserted! total rows: {len(df)}")


# Sales
def load_sales():
    print("\n[LOAD] sales_raw.csv")

    df = pd.read_csv('data/sales_raw.csv', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()

    print("[BEFORE]")
    print(df.head())

    df['invoice_date'] = clean_date_column(df['invoice_date'])
    df['created_at'] = clean_datetime_column(df['created_at'])

    print("[AFTER]")
    print(df.head())

    df['invoice_date'] = df['invoice_date'].dt.strftime('%Y-%m-%d')
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.where(pd.notnull(df), None)

    df.to_sql('sales_raw', engine, if_exists='replace', index=False)

    print(f"[SUCCESS] sales_raw inserted! total rows: {len(df)}")

# After Sales
def load_after_sales():
    print("\n[LOAD] after_sales_raw.csv")

    df = pd.read_csv('data/after_sales_raw.csv', encoding='utf-8-sig')
    df.columns = df.columns.str.strip()

    print("[BEFORE]")
    print(df.head())

    df['service_date'] = clean_date_column(df['service_date'])
    df['created_at'] = clean_datetime_column(df['created_at'])

    print("[AFTER]")
    print(df.head())

    df['service_date'] = df['service_date'].dt.strftime('%Y-%m-%d')
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.where(pd.notnull(df), None)

    df.to_sql('after_sales_raw', engine, if_exists='replace', index=False)

    print(f"[SUCCESS] after_sales_raw inserted! total rows: {len(df)}")


# Customer Addresses
def load_customer_addresses():
    print("\n[LOAD] customer_addresses (daily files)")

    folder = "data/customer_addresses/"
    files = sorted([f for f in os.listdir(folder) if f.startswith("customer_addresses_")])

    if not files:
        print("[WARNING] No files found!")
        return

    all_data = []

    for file in files:
        print(f"\n[PROCESS] {file}")

        file_path = os.path.join(folder, file)
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        df.columns = df.columns.str.strip()

        print("[BEFORE]")
        print(df.head())

        df['created_at'] = clean_datetime_column(df['created_at'])

        df = df.dropna(subset=['customer_id'])

        print("[AFTER]")
        print(df.head())

        df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    final_df = final_df.drop_duplicates()

    final_df.to_sql('customer_addresses_raw', engine, if_exists='replace', index=False)

    print(f"\n[SUCCESS] customer_addresses_raw replaced! total rows: {len(final_df)}")