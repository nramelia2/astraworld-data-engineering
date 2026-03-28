from config import engine
import pandas as pd

def create_datamart_sales():
    print("\n[DATAMART] sales")

    query = """
    SELECT
        DATE_FORMAT(invoice_date, '%%Y-%%m') AS periode,
        CASE
            WHEN price >= 100000000 AND price <= 250000000 THEN 'LOW'
            WHEN price > 250000000 AND price <= 400000000 THEN 'MEDIUM'
            WHEN price > 400000000 THEN 'HIGH'
            ELSE 'UNKNOWN'
        END AS class,
        model,
        SUM(price) AS total
    FROM sales_clean
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    """

    df = pd.read_sql(query, engine)

    df.to_sql('datamart_sales', engine, if_exists='replace', index=False)

    print(f"[SUCCESS] datamart_sales created! total rows: {len(df)}")