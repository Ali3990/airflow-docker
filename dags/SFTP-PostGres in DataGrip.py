from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import re
import zipfile
import pandas as pd
from io import StringIO


@dag(
    default_args={
        "owner": "airflow",
        "retries": 0,
        "catchup": False
    },
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sftp", "postgres", "taskflow"],
)
def sftp_corelogic_v2():

    # Check and print latest table in the SFTP transfer
    @task()
    def list_files():
        hook = SFTPHook(ftp_conn_id="sftp_conn")
        files = hook.get_conn().listdir(".")
        print("Files in SFTP root directory:", files)
        for f in files:
            print(" -", f)
        return files

    @task()
    def download_latest_files():
        





    list = list_files()
    download_latest = download_latest_files(list)

sftp_corelogic_v2() 

    
    