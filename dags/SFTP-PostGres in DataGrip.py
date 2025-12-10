from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import re
import zipfile
import pandas as pd
from io import StringIO

# Regex pattern for the date tailing the file name
REPORT_1_PATTERN = r"Marcus&Millichap_0000757_NationwideHomeSalesReport_Delivery_(\d{8})"
REPORT_2_PATTERN = r"MarcusMillichap_0000757_FCL_Activity_Report_(\d{8})"

@dag(
    default_args={
        "owner": "airflow",
        "retries": 0,
        "catchup": False
    },
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sftp", "postgres", "CoreLogic"],
)
def sftp_corelogic():

    # Check and print latest table in the SFTP transfer
    @task()
    def list_files():
        hook = SFTPHook(ftp_conn_id="sftp_conn")
        sftp = hook.get_conn()

        files_attr = sftp.listdir_attr(".")
        serializable = []

        for f in files_attr:
            # f is SFTPAttributes object here
            print(f"File: {f.filename}, last modified: {datetime.fromtimestamp(f.st_mtime)}")

            serializable.append({
                "filename": f.filename,
                "mtime": f.st_mtime
            })

        return serializable

    @task()
    def download_latest_files(file_list):
        latest = {"nationwide": None, "fcl": None}

        nationwide_candidates = []
        fcl_candidates = []

        for f in file_list:
            fname = f["filename"]
            mtime = f["mtime"]

            if re.search(REPORT_1_PATTERN, fname):
                nationwide_candidates.append(f)

            if re.search(REPORT_2_PATTERN, fname):
                fcl_candidates.append(f)

        if nationwide_candidates:
            latest["nationwide"] = max(nationwide_candidates, key=lambda x: x["mtime"])["filename"]

        if fcl_candidates:
            latest["fcl"] = max(fcl_candidates, key=lambda x: x["mtime"])["filename"]

        print("📌 Latest Nationwide:", latest["nationwide"])
        print("📌 Latest FCL:", latest["fcl"])

        return latest



    file_list_attr = list_files()
    download_latest = download_latest_files(file_list_attr)

    file_list_attr >> download_latest

sftp_corelogic() 

    
    