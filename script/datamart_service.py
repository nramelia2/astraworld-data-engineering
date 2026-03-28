from sqlalchemy import text
from config import engine

def create_datamart_service():
    print("\n[DATAMART] service")

    query = text("""
    DROP TABLE IF EXISTS datamart_service;

    CREATE TABLE datamart_service AS
    SELECT 
        YEAR(a.service_date) as periode,
        a.vin,

        c.name as customer_name,

        MAX(ca.address) as address,

        COUNT(DISTINCT a.service_ticket) as count_service,

        CASE 
            WHEN COUNT(DISTINCT a.service_ticket) > 10 THEN 'HIGH'
            WHEN COUNT(DISTINCT a.service_ticket) BETWEEN 5 AND 10 THEN 'MED'
            ELSE 'LOW'
        END as priority

    FROM after_sales_clean a

    LEFT JOIN customers_clean c 
        ON a.customer_id = c.id

    LEFT JOIN customer_addresses_clean ca 
        ON a.customer_id = ca.customer_id

    WHERE 
        c.is_active = 1 

    GROUP BY 
        YEAR(a.service_date), a.vin, c.name;
    """)

    with engine.connect() as conn:
        for stmt in query.text.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))

    print("[SUCCESS] datamart_service created")