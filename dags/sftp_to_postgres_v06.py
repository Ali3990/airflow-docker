from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import re
import zipfile
import pandas as pd
from io import StringIO

from home_sales_schema import nationwide_home_sales_schema
from fcl_activity_schema import fcl_activity_schema

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

LOCAL_DOWNLOAD_DIR = "/tmp/sftp_downloads"

# 🔹 Predefined schema mapping
PREDEFINED_SCHEMAS = {
    "nationwide_home_sales": nationwide_home_sales_schema,
    "fcl_activity": fcl_activity_schema
    }

@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sftp", "postgres", "taskflow"],
)
def sftp_to_postgres_v06():

    @task()
    def ensure_table_exists():
        hook = PostgresHook(postgres_conn_id="DBeaver_conn")
        result = hook.get_first("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'sftp_imports'
            );
        """)
        if not result[0]:
            print("Table does not exist, creating...")
        else:
            print("Table already exists, proceeding...")

    @task()
    def sftp_connection_check():
        hook = SFTPHook(ssh_conn_id="sftp_conn")
        conn = hook.get_conn()
        conn.listdir(".")
        print("✅ SFTP connection successful")

    @task()
    def list_files_in_root():
        hook = SFTPHook(ssh_conn_id="sftp_conn")
        files = hook.get_conn().listdir(".")
        print("📂 Files in SFTP root directory:", files)
        return files

    @task()
    def download_and_upload(files: list):
        if not files:
            print("⚠️ No files found in SFTP root")
            return

        month_pattern = re.compile(r".*_(\d{8})\.zip$")  # Capture YYYYMMDD
        available_dates = [month_pattern.match(f).group(1) for f in files if month_pattern.match(f)]
        if not available_dates:
            print("⚠️ No files matching daily pattern found")
            return

        latest_date = max(available_dates)
        print(f"🎯 Latest date detected: {latest_date}")
        target_pattern = re.compile(rf".*_{latest_date}\.zip$")

        hook = SFTPHook(ssh_conn_id="sftp_conn")
        os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
        pg_hook = PostgresHook(postgres_conn_id="DBeaver_conn")

        for f in files:
            if not target_pattern.match(f):
                continue

            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, f)
            hook.retrieve_file(remote_full_path=f, local_full_path=local_path)
            print(f"✅ Downloaded {f} → {local_path}")

            try:
                with zipfile.ZipFile(local_path, "r") as zip_ref:
                    zip_ref.extractall(LOCAL_DOWNLOAD_DIR)
                    for txt_file in zip_ref.namelist():
                        if txt_file.endswith(".txt"):
                            txt_path = os.path.join(LOCAL_DOWNLOAD_DIR, txt_file)

                            # Always read as strings to preserve values
                            chunk_iter = pd.read_csv(txt_path, sep="|", dtype=str, chunksize=5000)
                            for i, chunk in enumerate(chunk_iter):
                                if chunk.empty:
                                    print(f"⚠️ Chunk {i} in {txt_file} is empty, skipping")
                                    continue

                                raw_name = os.path.basename(txt_file).lower()

                                # Custom naming logic
                                if "nationwidehomesales" in raw_name:
                                    base_name = "nationwide_home_sales"
                                elif "fcl" in raw_name:
                                    base_name = "fcl_activity"
                                else:
                                    base_name = re.sub(r"\.txt$", "", raw_name)
                                    base_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name)

                                # Extract date from filename
                                date_match = re.search(r"(\d{8})", raw_name)
                                date_str = date_match.group(1) if date_match else latest_date

                                # Combine base name and date
                                table_name = f"{base_name}_{date_str}"
                                table_name = table_name[:63]  # Postgres limit

                                print(f"📌 Loading chunk {i} into table: {table_name}")

                                # Connect to Postgres
                                conn = pg_hook.get_conn()
                                cur = conn.cursor()

                                # Create table if first chunk
                                if i == 0:
                                    cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')

                                    if base_name in PREDEFINED_SCHEMAS:
                                        schema = PREDEFINED_SCHEMAS[base_name]
                                        columns_with_types = ", ".join([
                                            f'"{col}" {schema.get(col, "VARCHAR(50)")}' 
                                            for col in chunk.columns
                                        ])
                                    else:
                                        # fallback if no schema defined
                                        columns_with_types = ", ".join([f'"{col}" VARCHAR(50)' for col in chunk.columns])

                                    cur.execute(f'CREATE TABLE "{table_name}" ({columns_with_types});')
                                    conn.commit()

                                # 🔹 Enforce schema-based type cleaning before upload
                                    if base_name in PREDEFINED_SCHEMAS:
                                        schema = PREDEFINED_SCHEMAS[base_name]

                                        for col, dtype in schema.items():
                                            if col not in chunk.columns:
                                                continue

                                            # --- Numeric Types ---
                                            if any(x in dtype.upper() for x in ["REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"]):
                                                # Remove commas, coerce to float safely
                                                chunk[col] = (
                                                    chunk[col]
                                                    .astype(str)
                                                    .str.replace(",", "", regex=False)
                                                    .str.replace("$", "", regex=False)
                                                    .str.strip()
                                                )
                                                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

                                            # --- Integer Types ---
                                            elif any(x in dtype.upper() for x in ["INT", "BIGINT", "SMALLINT"]):
                                                chunk[col] = (
                                                    chunk[col]
                                                    .astype(str)
                                                    .str.replace(",", "", regex=False)
                                                    .str.strip()
                                                )
                                                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")

                                            # --- Boolean Types ---
                                            elif "BOOL" in dtype.upper():
                                                chunk[col] = (
                                                    chunk[col]
                                                    .astype(str)
                                                    .str.strip()
                                                    .str.lower()
                                                    .replace({"true": True, "false": False, "1": True, "0": False, "nan": None})
                                                )

                                            # --- Text Types ---
                                            else:
                                                chunk[col] = chunk[col].astype(str).str.strip().replace({"nan": None})

                                # Convert DataFrame → CSV in memory
                                csv_buffer = StringIO()
                                chunk.to_csv(csv_buffer, index=False, header=False)
                                csv_buffer.seek(0)

                                # Bulk load with COPY
                                cur.copy_expert(f'COPY "{table_name}" FROM STDIN WITH CSV', csv_buffer)
                                conn.commit()
                                cur.close()
                                conn.close()
                                print(f"✅ Uploaded chunk {i} with {len(chunk)} rows into {table_name}")

            except zipfile.BadZipFile as e:
                print(f"❌ Failed to extract {f}: {e}")

    # DAG task flow
    table_check = ensure_table_exists()
    conn_check = sftp_connection_check()
    files = list_files_in_root()
    download_upload = download_and_upload(files)

    table_check >> conn_check >> files >> download_upload

dag = sftp_to_postgres_v06()
