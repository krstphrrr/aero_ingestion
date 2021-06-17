import os, os.path
import time, datetime
import pandas as pd
import numpy as np
from src.utility_functions.db_conn import db
from src.utility_functions.tabletools import table_create, sql_command, tablecheck
from src.utility_functions.ingestion_script import ingester
from src.aero_model_metadata import update_model

"""
usage example:
path = r"path/to/NRI_FLUX_OUTPUT"

to check dataframe before ingesting:

df = txt_read(path)

to ingest data and also create metadata entry in ModelRuns
(requires metadata file):

model_run_updater(path, "A20200831", "NRI")

"""

def txt_read(path) -> pd.DataFrame:
    df_dict = {}
    testset = ["20184145384203B2_flux","20184145384203B1_flux","20184145374203B2_flux"]
    count = 1
    for i in os.listdir(path):
        #if file is not an excelfile
        if os.path.splitext(i)[1]!=".xlsx":
        # debug block
        if os.path.splitext(i)[0] in [i for i in testset]:
            file = os.path.join(path,i)
            created_time = os.path.getctime(file)
            parsed_ctime = time.ctime(created_time)
            date_ctime = datetime.datetime.strptime(parsed_ctime, "%a %b %d %H:%M:%S %Y")
            # print(date_ctime)
            complete = os.path.join(path,i)
            temp = pd.read_table(complete, sep="\t", low_memory=False)
            df_dict.update({f"df{count}":temp})
            count+=1

        # get date/time for modelrun
            file = os.path.join(path,i)
            created_time = os.path.getctime(file)
            parsed_ctime = time.ctime(created_time)
            date_ctime = datetime.datetime.strptime(parsed_ctime, "%a %b %d %H:%M:%S %Y")
            # get plotid
            plotid = i.split('_')[0]
            complete = os.path.join(path,i)
            temp = pd.read_table(complete, sep="\t", low_memory=False)
            temp['PlotId'] = plotid
            df_dict.update({f"df{count}":temp})
            # print(f"{count} added")
            count+=1
        else:
            pass
    return pd.concat([d[1] for d in df_dict.items()],ignore_index=True)


def model_run_updater(batchpath, modelrunkey, source = None) -> None:
    """
    arguments:
    batchpath -> directory that holds aero runs
    modelrunkey -> specify modelrunkey to designate current batch of runs
    source -> specify source (ie. NRI etc. ) to describe current batch of runs

    1. creates a table in postgres with supplied dataframe
    2. appends data to postgres table
    """
    d = db("aero")
    df = txt_read(batchpath)
    if source!=None:
        df['Source'] = source
    else:
        pass
    df['ModelRunKey'] = modelrunkey

    if tablecheck('aero_runs'):
        print('aero_runs exists, skipping table creation')
        update_model(batchpath,modelrunkey)

        ingester.main_ingest(df, "aero_runs", d.str,100000)
    else:
        print('creating aero_runs table..')
        table_create(df, "aero_runs", "aero")
        update_model(batchpath,modelrunkey)
        ingester.main_ingest(df, "aero_runs", d.str,100000)
