import os
from psycopg2 import sql
import numpy as np
import pandas as pd
from src.utility_functions.db_conn import db
from src.utility_functions.aero_interfaces import type_translate,aero_translate
# from src.utils.arcnah import arcno


""" EXPORTED FUNCTIONS """

def summary_update(summary_path):
    """
    summary table ingest, just needs the path to the .txt
    """
    d = db('aero')
    tablename = 'aero_summary'
    tempdf = pd.read_table(summary_path, sep="\t", low_memory=False)
    if tablecheck(tablename):
        ingester.main_ingest(tempdf, tablename,d.str)
    else:
        table_create(tempdf, tablename, 'aero')
        ingester.main_ingest(tempdf, tablename,d.str)




def table_create(df: pd.DataFrame, tablename: str, conn:str=None):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.

    external dependencies:
    - pandas
    - psycopg2

    local dependencies:
    - aero_translate interface
    - db connection factory
    """

    table_fields = {}

    try:
        for i in df.columns:
            if tablename!='aero_runs':
                pass
                # table_fields.update({f'{i}':f'{tablefields[possible_tables[tablename]][i]}'})
            elif tablename=='aero_summary':
                table_fields.update({f'{i}':f'{aero_translate[df.dtypes[i].name]}'})
            else:
                print("aero")
                table_fields.update({f'{i}':f'{aero_translate[df.dtypes[i].name]}'})


        if table_fields:
            print("checking fields")
            comm = sql_command(table_fields, tablename, conn) if conn!='nri' else sql_command(table_fields, tablename, 'nritest')
            d = db(f'{conn}')
            con = d.str
            cur = con.cursor()
            # return comm
            cur.execute(comm)
            con.commit()

    except Exception as e:
        print(e)
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()

def sql_command(typedict:{}, name:str, db:str=None):
    """
    create a string for a psycopg2 cursor execute command to create a new table.
    it receives a dictionary with fields and fieldtypes, and builds the string
    using them.

    modified to be used exclusively with aero data

    """
    db_choice={
    "aero":"gisdb",
    }
    schema_choice={
    "aero":"aero_data",
    }
    inner_list = [f"\"{k}\" {v}" for k,v in typedict.items()]
    part_1 = f""" CREATE TABLE {db_choice[db]}.{schema_choice[db]}.\"{name}\" \
     (""" if db==None else f""" CREATE TABLE {db_choice[db]}.{schema_choice[db]}.\"{name}\" ("""
    try:
        for i,x in enumerate(inner_list):
            if i==len(inner_list)-1:
                part_1+=f"{x}"
            else:
                part_1+=f"{x},"
    except Exception as e:
        print(e)
    finally:
        part_1+=");"
        return part_1




def tablecheck(tablename, conn="aero"):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    tableschema = "dimadev" if conn=="dimadev" else "aero_data"
    try:
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()
