import pandas as pd
from pandasql import *
import pandas as pd
import sqlite3

# Create a SQL connection to our SQLite database
con = sqlite3.connect("orders.db")

cur = con.cursor()
# pysqldf = lambda q: sqldf(q, globals())
# d.read_csv("bindat.csv", parse_dates=[["date", "time"]],
# date_parser=lambda x: pd.to_datetime(x, format="%Y%m%d %H%M"),
# index_col="date_time")

order_df = pd.read_csv('orders.csv', usecols=['date', 'quantity', 'SKU'], parse_dates=['date'])
po_df = pd.read_csv('pos.csv', usecols=['SKU', 'stock_quantity', 'moq', 'lead_time'])

order_df.to_sql('orders', con=con, if_exists='append')

