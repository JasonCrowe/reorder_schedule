import datetime
import pandas as pd
import sqlite3

con = sqlite3.connect(':memory:')
con.text_factory = str
cur = con.cursor()


def fix_sku(row):
    x = str(row['SKU']).replace(' ', '')
    return str(x).replace('\r\n', '')


order_df = pd.read_csv('orders.csv', usecols=['date', 'quantity', 'SKU'], parse_dates=['date'])
po_df = pd.read_csv('pos.csv', usecols=['SKU', 'stock_quantity', 'moq', 'lead_time'])

# order_df['SKU'] = order_df.apply(fix_sku, axis=1)
# po_df['SKU'] = po_df.apply(fix_sku, axis=1)

order_df.to_sql('orders', con=con, if_exists='replace')
po_df.to_sql('pos', con=con, if_exists='replace')

q = """
select
    order_data.SKU as order_sku,
    pos_data.SKU as po_sku,
    julianday(max_date) - julianday(min_date) as total_days,
    total_sales,
    pos_data.moq,
    pos_data.lead_time,
    pos_data.stock_qty as on_hand_inventory 
    from (
        select 
            SKU,
            max(date) as max_date,
            min(date) as min_date,
            sum(quantity) as total_sales
        from orders
        group by SKU
        ) as order_data
    left outer join (
        select 
            SKU,
            max(moq) as moq,
            max(lead_time) as lead_time,
            sum(stock_quantity) as stock_qty
        from pos
        group by SKU
    )pos_data on pos_data.SKU = order_data.SKU
"""
results = pd.read_sql(q, con=con)


def days_between_sales(row):
    days = row['total_days']/row['total_sales']
    return int(days)

# def days_between_sales(row):
#     try:
#         days = int(row['total_sales'])/int(row['total_days'])
#     except ZeroDivisionError:
#         days = 999
#     return int(days)


def reorder_days(row):
    days = row['days_between_sales'] * row['on_hand_inventory'] + row['lead_time']
    return days


def reorder_date(row):
    now = datetime.datetime.now().date()
    try:
        end_date = now + datetime.timedelta(days=row['reorder_days'])
    except ValueError:
        end_date = now
    return end_date

results['days_between_sales'] = results.apply(days_between_sales, axis=1)
results['reorder_days'] = results.apply(reorder_days, axis=1)
results['reorder_day'] = results.apply(reorder_date, axis=1)
# results['days_between_sales'] = results.apply(days_between_sales, axis=1)

# del results['total_days']
# del results['reorder_days']

results.to_csv('reorder_schedule.csv', index=False)
print results

