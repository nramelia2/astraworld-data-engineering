import pandas as pd
import glob
import os
import shutil
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:root@mysql:3306/dwh")

FILES_PATH = "/data/customer_addresses/*.csv"
ARCHIVE_PATH = "/data/archive/"

def ingest():
    files = glob.glob(FILES_PATH)

    if not files:
        print("No CSV files found!")
        return
        
    os.makedirs(ARCHIVE_PATH, exist_ok=True)

    for file in files:
        print(f"Processing {file}")

        df = pd.read_csv(file)

        df.to_sql(
            "customer_addresses_stg",
            con=engine,
            if_exists="append",
            index=False
        )

        dest = os.path.join(ARCHIVE_PATH, os.path.basename(file))
        shutil.move(file, dest)

        print(f"Moved to archive: {dest}")

if __name__ == "__main__":
    ingest()