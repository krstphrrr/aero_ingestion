import pyodbc
from psycopg2 import connect, sql
from os import chdir, getcwd
from os.path import abspath, join
from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool

import jaydebeapi
import platform

ucanaccess_jars = [
r"/usr/src/UCanAccess-5.0.0-bin/ucanaccess-5.0.0.jar",
r"/usr/src/UCanAccess-5.0.0-bin/lib/commons-lang3-3.8.1.jar",
r"/usr/src/UCanAccess-5.0.0-bin/lib/commons-logging-1.2.jar",
r"/usr/src/UCanAccess-5.0.0-bin/lib/hsqldb-2.5.0.jar",
r"/usr/src/UCanAccess-5.0.0-bin/lib/jackcess-3.0.1.jar"]

classpath = ":".join(ucanaccess_jars)

drv_str1 = r"net.ucanaccess.jdbc.UcanaccessDriver"

def jdbc_path(path):
    return f'jdbc:ucanaccess://"{path}";newDatabaseVersion=V2010'

def jaycon(path):
    con = jaydebeapi.connect(drv_str1, jdbc_path(path), ["",""],classpath)
    return con

class Acc2:
    con=None
    def __init__(self, whichdima):
        self.whichdima=whichdima
        MDB = self.whichdima
        drv_str1 = "net.ucanaccess.jdbc.UcanaccessDriver"
        DRV = '{/out/lib/libmdbodbc.so}' if platform.system()=='Linux' else '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = f'jdbc:ucanaccess://{MDB};newDatabaseVersion=V2010'

        self.con = jaydebeapi.connect(drv_str1, mdb_string,["",""],classpath)


    def db(self):
        try:
            return self.con
        except Exception as e:
            print(e)

class Acc:
    con=None
    def __init__(self, whichdima):
        self.whichdima=whichdima
        MDB = self.whichdima

        DRV = '{/out/lib/libmdbodbc.so}' if platform.system()=='Linux' else '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = f"DRIVER={DRV};DBQ=\"{MDB}\";" if platform.system()=='Linux' else f"DRIVER={DRV};DBQ={MDB};"

        self.con = pyodbc.connect(mdb_string)


    def db(self):
        try:
            return self.con
        except Exception as e:
            print(e)


def config(filename='src/utils/database.ini', section='postgresql'):
    """
    Uses the configpaser module to read .ini and return a dictionary of
    credentials
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))

    return db

def dimaconfig(filename='src/utils/database.ini', section='dima'):
    """
    Same as config but reads another section.
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))
    return db


class db:
    def __init__(self, keyword = None):
        if keyword == None:
            self.params = config()
            self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
            self.str = self.str_1.getconn()
        else:
            if "dimadev" in keyword:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=dimadev"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()


            else:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=public"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()


def searchpath_test(keyword):
    d = db(keyword)

    try:
        con = d.str
        cur = con.cursor()
        sql = 'show search_path'
        cur.execute(sql)
        print(cur.fetchone())
    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()


# searchpath_test("dima")

maintablelist = [
      'tblPlots',
      'tblLines',
      'tblLPIDetail',
      'tblLPIHeader',
      'tblGapDetail',
      'tblGapHeader',
      'tblQualHeader',
      'tblQualDetail',
      'tblSoilStabHeader',
      'tblSoilStabDetail',
      'tblSoilPitHorizons',
      'tblSoilPits',
      'tblSpecRichHeader',
      'tblSpecRichDetail',
      'tblPlantProdHeader',
      'tblPlantProdDetail',
      'tblPlotNotes',
      'tblPlantDenHeader',
      'tblPlantDenDetail',
      'tblSpecies',
      'tblSpeciesGeneric',
      'tblSites',
      'tblBSNE_Box',
      'tblBSNE_BoxCollection',
      'tblBSNE_Stack',
      'tblBSNE_TrapCollection'
      ]
correct = {
        'TBLPLOTS':'tblPlots',
        'TBLLINES':'tblLines',
        'TBLLPIDETAIL':'tblLPIDetail',
        'TBLLPIHEADER':'tblLPIHeader',
        'TBLGAPDETAIL':'tblGapDetail',
        'TBLGAPHEADER':'tblGapHeader',
        'TBLQUALHEADER':'tblQualHeader',
        'TBLQUALDETAIL':'tblQualDetail',
        'TBLSOILSTABHEADER':'tblSoilStabHeader',
        'TBLSOILSTABDETAIL':'tblSoilStabDetail',
        'TBLSOILPITHORIZONS':'tblSoilPitHorizons',
        'TBLSOILPITS':'tblSoilPits',
        'TBLSPECRICHHEADER':'tblSpecRichHeader',
        'TBLSPECRICHDETAIL':'tblSpecRichDetail',
        'TBLPLANTPRODHEADER':'tblPlantProdHeader',
        'TBLPLANTPRODDETAIL':'tblPlantProdDetail',
        'TBLPLOTNOTES':'tblPlotNotes',
        'TBLPLANTDENHEADER':'tblPlantDenDetail',
        'TBLPLANTDENDETAIL':'tblPlantDenDetail',
        'TBLSPECIES':'tblSpecies',
        'TBLSPECIESGENERIC':'tblSpeciesGeneric',
        'TBLSITES':'tblSites',
        'TBLBSNE_BOX':'tblBSNE_Box',
        'TBLBSNE_BOXCOLLECTION':'tblBSNE_BoxCollection',
        'TBLBSNE_STACK':'tblBSNE_Stack',
        'TBLBSNE_TRAPCOLLECTION':'tblBSNE_TrapCollection'
        }
