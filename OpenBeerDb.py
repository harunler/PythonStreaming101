import os
import pandas as pd
import asyncio as a_io
import requests as req
import sqlite3 as sql_3
import logging as log
import matplotlib.pyplot as plt
from zipfile import ZipFile as zipF
from io import BytesIO as byIo

# const
CMD_REPLACE: str = "replace"
COL_UNNAMED_PREFIX: str = "Unnamed: "
DBS_IN_MEMORY: str = ":memory:"
ERR_UNEXPECTED: str = "unexpected error!"
ERR_REQ_CNN: str = "request connection error!"
ERR_REQ_TIMEOUT: str = "request timeout error!"
EXT_CSV: str = ".csv"
EXT_DB: str = ".db"
EXT_LOG: str = ".log"
LOG_FORMAT: str = "%(asctime)s %(levelname)s: %(funcName)s -> %(message)s"
MSG_IMPORT: str ="importing data"
MSG_STATUS_READY: str = "status ready!"
MOD_NAME = "OpenBeerDb"
SQL_DROP_EXIST_TBL: str = "DROP TABLE IF EXISTS "
SQL_TABLES: str = "SELECT name FROM sqlite_master WHERE type='table'"
URL_DOWNLOAD: str = "https://openbeerdb.com/files/openbeerdb_csv.zip"

# initialize logging
log.basicConfig(filename=f"{MOD_NAME}{EXT_LOG}", format=LOG_FORMAT, filemode="w")
lg = log.getLogger()
c_handler = log.StreamHandler()
c_handler.setLevel(log.DEBUG)
lg.addHandler(c_handler)

#The database connection
_cnn: sql_3

class DataFrameValidator:
    # Observe the column's info of a data frame and return a list of valid column names
    @staticmethod
    @a_io.coroutine
    async def validate_csv_cols(csv):
        df = pd.read_csv(csv, header=0, delimiter=",", nrows=0)
        return [str(c) for c in df.columns if not str(c).startswith(COL_UNNAMED_PREFIX)]

class Data:
    @staticmethod
    def table_list():
        global _cnn
        if not _cnn: return []
        cur = _cnn.cursor()
        cur.execute(SQL_TABLES)
        res = cur.fetchall()
        cur.close()
        return [t[0] for t in res]

    @staticmethod
    @a_io.coroutine
    async def query_async(sql): return Data.query(sql)

    @staticmethod
    def query(sql):
        global _cnn
        return {} if not _cnn else pd.read_sql_query(sql, _cnn)

    @staticmethod
    def create_or_alter_table(name, df):
        global _cnn
        if _cnn and df: df.to_sql(name, df, _cnn)

# Open beer db data class
class OpenBeerDb:
    def __init__(self, in_memory=True, show_import_info=False, url=URL_DOWNLOAD):
        global _cnn
        self._loop = a_io.get_event_loop()
        self._show_imp_info = show_import_info
        self.url = url
        if in_memory:
            _cnn = sql_3.connect(DBS_IN_MEMORY)
            self.import_zip(url)
        else:
            _cnn = sql_3.connect(f"{MOD_NAME}{EXT_DB}")
            print(f"{MOD_NAME}: {MSG_STATUS_READY}") if len(Data.table_list()) else self.import_zip(url)

    # imports csv data to db table
    async def _import_csv(self, csv, cols):
        global _cnn
        df = pd.read_csv(csv, header=0, delimiter=",", index_col=0, usecols=cols, low_memory=False)
        tbl_n = str(csv.name).split("/")[1].replace(EXT_CSV, "")
        df.to_sql(tbl_n, _cnn, if_exists=CMD_REPLACE)
        if self._show_imp_info: print(f"{'_'*80}\n{tbl_n}\n{df.describe()}\n")

    # Imports data into the db
    def import_zip(self, url):
        try:
            print(f"{MOD_NAME}: {MSG_IMPORT}...")
            resp = req.get(self.url, stream=True)
            z = zipF(byIo(resp.content))
            csv_n = [c for c in z.namelist() if str(c).endswith(EXT_CSV)]
            # validate headers in csv files
            t1 = [self._loop.create_task(DataFrameValidator.validate_csv_cols(z.open(c))) for c in csv_n]
            self._loop.run_until_complete(a_io.wait(t1))
            # fill data frames and write to db
            t2 = [self._loop.create_task(self._import_csv(z.open(c), t.result())) for c, t in zip(csv_n, t1)]
            self._loop.run_until_complete(a_io.wait(t2))
            if self._show_imp_info: print(f"{'_'*80}\n")
            print(f"{MOD_NAME}: {MSG_STATUS_READY}")

        except a_ioh.ClientConnectionError: lg.error(ERR_REQ_CNN)
        except TimeoutError: lg.error(ERR_REQ_TIMEOUT)
        except: lg.critical(ERR_UNEXPECTED, exc_info=True)

    def query_async(self, sql_list):
        global _cnn
        try:
            if _cnn and Data.table_list():
                tasks = [self._loop.create_task(Data.query_async(s)) for s in sql_list]
                self._loop.run_until_complete(a_io.wait(tasks))
                return [t.result() for t in tasks]
        except:
            lg.critical(ERR_UNEXPECTED, exc_info=True)

"""
if __name__ == '__main__':
    ob = OpenBeerDb()
    #dfs = ob.query_async(["select * from styles;"])
    #print(dfs[0].head())
    df = Data.query("select * from styles;")
    pd.set_option('display.max_columns', 3)
    pd.set_option('display.max_rows', 1000)
    print(df.head(3))
    print(df.describe())
    df["cat_id"].plot(kind="hist", bins=100)
    plt.xlabel('category')
"""    
