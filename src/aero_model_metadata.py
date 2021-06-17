import os, os.path
import pandas as pd
import numpy as np
from sqlalchemy import *
from src.utility_functions.db_conn import db
from src.utility_functions.tabletools import table_create, sql_command, tablecheck
from src.utility_functions.aero_interfaces import fields_dict

"""
script responsible for creating and updating the metadata table for aero up in
postgres (db:aero_data, schema:public, tables: ModelRuns, Aero_runs)

ModelRuns: metadata on each aero modelling run ingested.

aero_runs: actual data from each aero modelling run.

"""


""" EXPORTED FUNCTION """

def update_model(path_in_batch,modelrunkey):
    """ ingests a modelrun metadata file if project key does not exist.
    1. create template
    2. check table existence
    3. check modelrun_key existence
    4. read_template = create new dataframe with the metadata excel file supplied
    5. send_model = create connection to db and ingest.

    """
    # create empty modelruns table in pandas
    tempdf = template()

    # check if table exists
    if tablecheck("ModelRuns", "aero"):
        if modelrun_key_check(modelrunkey):
            print(f"modelrunkey exists, aborting 'ModelRuns' update with ModelRunKey = {modelrunkey}.")
            print("continuing without update..")
            pass
        else:
            #
            update = read_template(path_in_batch,tempdf)
            # update['ModelRunKey'] = modelrunkey
            send_model(update)

    # if no, create table and update pg
    else:
        table_create(tempdf,"ModelRuns","aero")
        add_modelrunkey_to_pg()
        update = read_template(path_in_batch, tempdf)
        # tempdf = read_template(path_in_batch,tempdf)
        # update['ModelRunKey'] = modelrunkey
        send_model(update)

""" UTILITY FUNCTIONS """

def engine_conn_string(string):
    """ engine setup for sqlalchemy's db connection
    string argument is for choosing which user to connect as. refer to dotenv file
    for options.
    """
    d = db(string)
    return f'postgresql://{d.params["user"]}:{d.params["password"]}@{d.params["host"]}:{d.params["port"]}/{d.params["dbname"]}'

def send_model(df):
    """ sends completed dataframe to postgres
    """
    eng = create_engine(engine_conn_string("aero"))
    df.to_sql(con=eng, name="ModelRuns", if_exists="append", index=False)

def template():
    """ creating an empty dataframe with a specific
    set of fields and field types
    """
    df = pd.DataFrame(fields_dict)
    return df


def read_template(dir, maindf):
    """ creates a new dataframe with the data on the metadata excel file,
    appending it to an empty dataframe be uploaded to the projects table in pg.
    """
    maindfcopy = maindf.copy()
    maindf.drop(maindf.index,inplace=True)
    for path in os.listdir(dir):
        if os.path.splitext(path)[1]==".xlsx":
            df = pd.read_excel(os.path.join(dir,path))
            data = df.iloc[0,:]

        elif os.path.splitext(path)[1]!=".xlsx":
            pass
        else:
            print("No metadata '.xlsx'(excel) file found within directory. Please provide project metadata file.")
    maindf.loc[len(maindf),:] = data
    return maindf




def modelrun_key_check(modelrunkey):
    d = db("aero")
    if tablecheck("ModelRuns", "aero"):
        try:
            con = d.str
            cur = con.cursor()
            exists_query = '''
            select exists (
                select 1
                from "ModelRuns"
                where "ModelRunKey" = %s
            )'''
            cur.execute (exists_query, (modelrunkey,))
            return cur.fetchone()[0]

        except Exception as e:
            print(e, "error selecting modelruns table.")
            con = d.str
            cur = con.cursor()
    else:
        print("ModelRun table does not exist.")


def add_modelrunkey_to_pg():
    d = db("aero")
    add_query = '''
        ALTER TABLE IF EXISTS "ModelRuns"
        ADD COLUMN "ModelRunKey" TEXT;
        '''
    try:
        con = d.str
        cur = con.cursor()
        cur.execute(add_query)
        con.commit()

    except Exception as e:
        print(e, 'error adding column to modelruns table')
        con = d.str
        cur = con.cursor()


def modelrunkey_extract(df):
    if "ModelRunKey" in df.columns:
        return df.ModelRunKey[0]
