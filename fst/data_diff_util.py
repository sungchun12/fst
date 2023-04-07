# Optional: Set logging to display the progress of the diff
import os
import logging

logging.basicConfig(level=logging.INFO)

from data_diff import connect_to_table, diff_tables, JoinDiffer, connect

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

materialized_table = connect(SNOWFLAKE_CONN_INFO).parse_table_name("test_datadiff")
# capitalization matters for these table names

table1 = connect_to_table(SNOWFLAKE_CONN_INFO, "FCT_ORDER_ITEMS", "ORDER_ITEM_KEY")
table2 = connect_to_table(SNOWFLAKE_CONN_INFO, "FCT_ORDERS", "ORDER_KEY")

# diffed_tables = list(diff_tables(table1, table2, extra_columns=("ORDER_DATE","GROSS_ITEM_SALES_AMOUNT")))
# https://github.com/datafold/data-diff/blob/master/tests/test_joindiff.py#L116
diff_tables_by_join = JoinDiffer(
    threaded=True, validate_unique_key=True, materialize_to_table=materialized_table
)
diffed_tables_joined = list(
    diff_tables_by_join.diff_tables(table1=table1, table2=table2)
)
print(diffed_tables_joined[0:5])
print(len(diffed_tables_joined))

# materialize the table

# copy and paste the SQL logic from this video:https://www.loom.com/share/682e4b7d74e84eb4824b983311f0a3b2?t=191
# materialize that table
