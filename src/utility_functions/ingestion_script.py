
import datetime as dt
from psycopg2 import sql
from tqdm import tqdm
from io import StringIO
import psycopg2, re, os, os.path, pandas as pd
from sqlalchemy import *
from sqlalchemy import TEXT, INTEGER, NUMERIC, VARCHAR, DATE
from pandas import read_sql_query
from geoalchemy2 import Geometry, WKTElement, WKBElement
from shapely.geometry import Point
import geopandas as gpd
from src.utility_functions.db_conn import db

class ingester:

    con = None
    cur = None
    # data pull on init
    __tablenames = []
    __seen = set()

    def __init__(self, con):
        """ clearing old instances """
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.__tablenames = []
        self.__seen = set()

        """ init connection objects """
        self.con = con
        self.cur = self.con.cursor()
        """ populate properties """
        self.pull_tablenames()
    def clear(self,var):
        var = None
        return var

    def pull_tablenames(self):

        if self.__tablenames is not None:
            if self.con is not None:

                try:
                    self.cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name;""")
                    query_results = self.cur.fetchall()

                    for table in query_results:
                        if table not in self.__seen:
                            self.__seen.add(re.search(r"\(\'(.*?)\'\,\)",
                            str(table)).group(1))
                            self.__tablenames.append(re.search(r"\(\'(.*?)\'\,\)",
                            str(table)).group(1))
                except Exception as e:
                    print(e)
                    self.con = self.con
                    self.cursor = self.con.cursor
            else:
                print("connection object not initialized")

    @staticmethod
    def drop_fk(self, table):
        conn = self.con
        cur = conn.cursor()
        key_str = "{}_PrimaryKey_fkey".format(str(table))
        print('try: dropping keys...')
        try:
            # print(table)
            cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   DROP CONSTRAINT IF EXISTS {1}""").format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            conn.commit()
        except Exception as e:
            print(e)
            conn = self.con
            cur = conn.cursor()
        print(f"Foreign keys on {table} dropped")

    def drop_table(self, table):
        conn = self.con
        cur = conn.cursor()
        try:
            cur.execute(
            sql.SQL("DROP TABLE IF EXISTS gisdb.public.{} CASCADE;").format(
            sql.Identifier(table))
            )
            conn.commit()
            print(table +' dropped')
        except Exception as e:
            print(e)
            conn = self.con
            cur = conn.cursor()

    def reestablish_fk(self,table):
        conn = self.con
        cur = conn.cursor()
        key_str = "{}_PrimaryKey_fkey".format(str(table))

        try:

            cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   ADD CONSTRAINT {1}
                   FOREIGN KEY ("PrimaryKey")
                   REFERENCES "dataHeader"("PrimaryKey");
                   """).format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            conn.commit()
        except Exception as e:
            print(e)
            conn = self.con
            cur = conn.cursor()

    @staticmethod
    def main_ingest( df: pd.DataFrame, table:str,
                    connection: psycopg2.extensions.connection,
                    chunk_size:int = 10000):
        """needs a table first"""
        print(connection)

        df = df.copy()

        escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
        for col in df.columns:
            if df.dtypes[col] == 'object':
                for v, e in escaped.items():
                    df[col] = df[col].apply(lambda x: x.replace(v, '') if (x is not None) and (isinstance(x,str)) else x)
        try:
            conn = connection
            cursor = conn.cursor()
            for i in tqdm(range(0, df.shape[0], chunk_size)):
                f = StringIO()
                chunk = df.iloc[i:(i + chunk_size)]

                chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                f.seek(0)
                cursor.copy_from(f, f'"{table}"', columns=[f'"{i}"' for i in df.columns])
                connection.commit()
        except psycopg2.Error as e:
            print(e)
            conn = connection
            cursor = conn.cursor()
            conn.rollback()
        cursor.close()

    @staticmethod
    def composite_pk(*field,con,maintable):
        """ Creates composite primary keys in postgres for a given table
        """
        conn = con
        cur = conn.cursor()
        key_str = "{}_PrimaryKey_fkey".format(str(maintable))
        fields = [f'{i}' for i in field]
        fields_str = ', '.join(fields)
        fields_str2 = f'{fields_str}'



        try:

            cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   ADD CONSTRAINT {1}
                   PRIMARY KEY ({2})
                   """).format(
                   sql.Identifier(maintable),
                   sql.Identifier(key_str),
                   sql.Identifier(fields_str2))
            )

            conn.commit()
        except Exception as e:
            print(e)
            conn = con
            cur = conn.cursor()

    @staticmethod
    def drop_rows(con, maintable, field, result):
        """ removing rows that fit a specific value from a given table

        - need to implement graceful closing of pg session
        """
        conn = con
        cur = conn.cursor()
        try:

            cur.execute(
            sql.SQL("""DELETE from gisdb.public.{0}
                  WHERE {1} = '%s'
                   """ % result).format(
                   sql.Identifier(maintable),
                   sql.Identifier(field))
            )

            conn.commit()
        except Exception as e:
            print(e)
            conn = con
            cur = conn.cursor()
