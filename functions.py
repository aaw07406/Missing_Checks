import pandas as pd
import pyodbc as od
import time
import logging
import os.path
from datetime import datetime, timedelta
import os
import customtkinter as ctk
import tkinter as tk
from PIL import Image
from tkinter import messagebox
import os
from tabulate import tabulate
from customtkinter import CTkFont
from customtkinter import CTkTextbox
import tkinter.filedialog as fd
from customtkinter import CTkImage
from PIL import Image
from tkinter import PhotoImage
import re
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import matplotlib.pyplot as plt


ecprod = 'DSN=ECPROD'
edwprod = 'DSN=EDWPROD'
arrprod = 'DSN=ARRPROD'
logger = logging.getLogger()
this_folder = os.path.dirname(os.path.abspath(__file__))


def get_sql(sql: str, conn: str = edwprod) -> pd.DataFrame:
    """ Returns pd.DataFrame results from query string with pyodbc connection string."""
    connection = od.connect(conn, autocommit=True, unicode_results=True)
    res = pd.read_sql(sql, connection)
    connection.close()
    return res


def execute_sql(sql: str, conn: str = edwprod) -> None:
    """Executes a SQL statement that does not return a result (e.g., CREATE, DROP, INSERT)."""
    connection = od.connect(conn, autocommit=True, unicode_results=True)
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()
    connection.close()


def sql_insert(df, conn: str = None):
    """Executes a SQL statement that does not return a result (e.g., CREATE, DROP, INSERT)."""
    connection = od.connect(conn, autocommit=True, unicode_results=True)
    cursor = connection.cursor()
    for row in df.itertuples(index=False):
        cursor.execute("""
            MERGE INTO DL_MARSHALL.MISSING_CHECKS AS tgt
            USING (SELECT ? AS RECORD_TYPE_RF, ? AS CARRIER_CD, ? AS CARRIER_NM, ? AS CHECK_NUM, ? AS CHECK_AMT) AS src
            ON tgt.CHECK_NUM = src.CHECK_NUM AND tgt.CARRIER_CD = src.CARRIER_CD AND tgt.CHECK_AMT = src.CHECK_AMT
            WHEN NOT MATCHED THEN
            INSERT (RECORD_TYPE_RF, CARRIER_CD, CARRIER_NM, CHECK_NUM, CHECK_AMT)
            VALUES (src.RECORD_TYPE_RF, src.CARRIER_CD, src.CARRIER_NM, src.CHECK_NUM, src.CHECK_AMT)
        """, row)


    connection.commit()
    cursor.close()
    connection.close()


def get_db2_sql(sql: str, conn: str = None) -> pd.DataFrame:
    """ Returns pd.DataFrame results from query string with pyodbc connection string."""
    connection = od.connect(conn, autocommit=True, unicode_results=True)
    res = pd.read_sql(sql, connection)
    connection.close()
    return res

