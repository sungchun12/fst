# fst

**fst: flow state tool**

Let's make it the overwhelming normal that these questions are answered in seconds or less when engineering data.

- Who else is touching this precious file of mine? I’m tired of pull request clashing

- What’s historical performance on this and am I beating it?

- How often does this fail in production?

- Who uses this model and how often?

- What dashboards will this help vs. hurt?

- What’s a data preview based on my updates look like?

- How many scheduled data pipelines are tied to this model?

- How much does this cost to run in production and am I helping vs. hurting?

- What are existing database permissions on this model?

- Anyone working on pull requests in real time that rely on my work?

- What’s a data diff compared to current production data?


```bash
# my command to run this tool in an infinite loop in a split terminal
python -m venv venv
source venv/bin/activate     
pip install -r requirements.txt
cd jaffle_shop_duckdb
python ../fst_query.py /Users/sung/Desktop/fst/jaffle_shop_duckdb/models/customers.sql
```

```shell
# example of running this tool on each modification to the sql file
Detected modification: /Users/sung/Desktop/fst/jaffle_shop_duckdb/models/new_file.sql
Received query:
select * from {{ ref("customers") }} where customer_lifetime_value > 0 order by customer_lifetime_value desc limit 10


Compiling dbt with the modified SQL file (new_file)...
Detected modification: /Users/sung/Desktop/fst/jaffle_shop_duckdb/target/compiled/jaffle_shop/models/new_file.sql
dbt compile was successful.
compiled_file_path: /Users/sung/Desktop/fst/jaffle_shop_duckdb/target/compiled/jaffle_shop/models/new_file.sql
Executing compiled query:
select * from "jaffle_shop"."main"."customers" where customer_lifetime_value > 0 order by customer_lifetime_value desc limit 10

Using DuckDB file: jaffle_shop.duckdb
Compilation time: 3.25 seconds
Query time: 0.03 seconds
Result:
+---------------+-------------+---------------+---------------------+--------------------+---------------------------+
|   customer_id | last_name   | first_order   | most_recent_order   |   number_of_orders |   customer_lifetime_value |
+===============+=============+===============+=====================+====================+===========================+
|            51 | R.          | 2018-01-28    | 2018-02-23          |                  3 |                        99 |
+---------------+-------------+---------------+---------------------+--------------------+---------------------------+
|             3 | P.          | 2018-01-02    | 2018-03-11          |                  3 |                        65 |
+---------------+-------------+---------------+---------------------+--------------------+---------------------------+
|            46 | C.          | 2018-03-24    | 2018-03-27          |                  2 |                        64 |
+---------------+-------------+---------------+---------------------+--------------------+---------------------------+
|            30 | W.          | 2018-03-02    | 2018-03-14          |                  2 |                        57 |
+---------------+-------------+---------------+---------------------+--------------------+---------------------------+
|            54 | M.          | 2018-01-07    | 2018-03-24          |                  5 |                        57 | 1 |                        26 |
```