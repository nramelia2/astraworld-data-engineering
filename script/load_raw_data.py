from load import (
    load_customer,
    load_sales,
    load_after_sales,
    load_customer_addresses
)

from clean import (
    clean_customers,
    clean_sales,
    clean_after_sales,
    clean_customer_addresses
)

from datamart_sales import create_datamart_sales
from datamart_service import create_datamart_service


def main():
    print("===== START ETL =====")

    load_customer()
    load_sales()
    load_after_sales()
    load_customer_addresses()

    clean_customers()
    clean_sales()
    clean_after_sales()
    clean_customer_addresses()

    create_datamart_sales()
    create_datamart_service()

    print("===== END ETL =====")


if __name__ == "__main__":
    main()