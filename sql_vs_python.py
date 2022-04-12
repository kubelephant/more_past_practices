# SQL vs Python

# 1. Get Row_number
SQL: row_number() over(partition by customer_id order by date asc)
Python: orders['rn'] = orders.groupby('customer_id')['date'].rank(method='first', ascending=True)

# 2. Get Rank
SQL: rank() over(partition by customer_id order by date asc)
Python: orders['rk'] = orders.groupby('customer_id')['date'].rank(method='min', ascending=True)

# 3. Get Dense Rank
SQL: dense_rank() over(partition by customer_id order by date asc)
Python: orders['dense_rk'] = orders.groupby('customer_id')['date'].rank(method='dense', ascending=True)

# 4. Get Cumulative Sum
SQL: sum(amount) over(partition by customer_id, order_month order by date rows unbounded preceding)
Python: orders['cumsum'] = orders.sort_values(by='date', ascending=True).groupby(['customer_id', 'order_month'])['amount_paid'].cumsum().round(1)

# 5. Get Average
SQL: avg(amount) over(partition by customer_id, order_month)
Python: orders['avg_amount'] = orders.groupby(['customer_id', 'order_month'])['amount_paid'].transform('mean').round(1)
### Using transform to keep the length of the original dataframe

# 6. Lead/lag, offset
SQL: lead(adj_close, 1) over(partition by symbol order by date)
Python: stocks.sort_values(by='date', ascending=True).groupby('symbol').shift(1)

SQL: lag(adj_close, 7) over(partition by symbol order by date)
Python: stocks.sort_values(by='date', ascending=True).groupby('symbol').shift(-7)

# 6.1 Calculate percentage changed
Python: sotcks.sort_values(by='date', ascending=True).groupby('symbol').pct_change(1).round(2)

 