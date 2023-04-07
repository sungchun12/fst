# Optional: Set logging to display the progress of the diff
import os
import logging
logging.basicConfig(level=logging.INFO)

from data_diff import connect_to_table, diff_tables

SNOWFLAKE_CONN_INFO = {
    "driver": "snowflake",
    "user": "sung_c",
    "account": "zna84829",
    "database": "ANALYTICS",
    "warehouse": "TRANSFORMING",
    "role": "TRANSFORMER",
    "schema": "DBT_PROD_SUNG",
    "password": os.environ["SNOWFLAKE_PASSWORD"],
}

# connect(SNOWFLAKE_CONN_INFO)
# capitalization matters for these table names

snowflake_table = connect_to_table(SNOWFLAKE_CONN_INFO, "table_name")  # Uses id by default

table1 = connect_to_table(SNOWFLAKE_CONN_INFO, "FCT_ORDER_ITEMS", "ORDER_ITEM_KEY")
table2 = connect_to_table(SNOWFLAKE_CONN_INFO, "FCT_ORDERS", "ORDER_KEY")

# for different_row in diff_tables(table1, table2):
#     plus_or_minus, columns = different_row
#     print(plus_or_minus, columns)

diffed_tables = list(diff_tables(table1, table2, extra_columns=("ORDER_DATE","GROSS_ITEM_SALES_AMOUNT")))
print(diffed_tables[0:5])

# materialize the table
# copy and paste the SQL logic from this video:https://www.loom.com/share/682e4b7d74e84eb4824b983311f0a3b2?t=191
# materialize that table