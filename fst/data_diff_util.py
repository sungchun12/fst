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

DUCKDB_CONN_INFO = {"driver": "duckdb", "filepath": "jaffle_shop.duckdb"}
connect(DUCKDB_CONN_INFO)

table_a = connect_to_table(DUCKDB_CONN_INFO, "new_file", "customer_id")
table_b = connect_to_table(DUCKDB_CONN_INFO, "customers", "customer_id")

diffed_tables = list(diff_tables(table_a, table_b))


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

# diff duckdb on the left and snowflake on the right
table3 = connect_to_table(SNOWFLAKE_CONN_INFO, "dim_customers", "names")
duckdb_to_snowflake = diff_tables(table1=table_a, table2=table3)
print(duckdb_to_snowflake)
# print(len(duckdb_to_snowflake))

# DiffResultWrapper(diff=<generator object TableDiffer._diff_tables_wrapper at 0x1206f8660>, info_tree=InfoTree(info=SegmentInfo(tables=[TableSegment(database=<data_diff.databases.duckdb.DuckDB object at 0x105ddca00>, table_path=('customers',), key_columns=('customer_id',), update_column=None, extra_columns=(), min_key=None, max_key=None, min_update=None, max_update=None, where=None, case_sensitive=True, _schema=None), TableSegment(database=<data_diff.databases.snowflake.Snowflake object at 0x106a17af0>, table_path=('DIM_CUSTOMERS',), key_columns=('CUSTOMER_KEY',), update_column=None, extra_columns=(), min_key=None, max_key=None, min_update=None, max_update=None, where=None, case_sensitive=True, _schema=None)], diff=None, is_diff=None, diff_count=None, rowcounts={}, max_rows=None), children=[]), stats={'validated_unique_keys': [['ORDER_ITEM_KEY'], ['ORDER_KEY']], 'table1_count': 60175, 'table2_count': 15000, 'exclusive_count': 75175, 'diff_counts': {'ORDER_ITEM_KEY_a': 75175}}, result_list=[])
# Traceback (most recent call last):