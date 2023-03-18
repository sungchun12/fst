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
Using DuckDB file: jaffle_shop.duckdb
Compilation time: 3.20 seconds
Query time: 0.01 seconds
Result Preview:
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|   customer_id | first_name   | last_name   | first_order   | most_recent_order   |   number_of_orders |   customer_lifetime_value |
+===============+==============+=============+===============+=====================+====================+===========================+
|             1 | Michael      | P.          | 2018-01-01    | 2018-02-10          |                  2 |                        33 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|             2 | Shawn        | M.          | 2018-01-11    | 2018-01-11          |                  1 |                        23 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|             3 | Kathleen     | P.          | 2018-01-02    | 2018-03-11          |                  3 |                        65 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|             6 | Sarah        | R.          | 2018-02-19    | 2018-02-19          |                  1 |                         8 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|             7 | Martin       | M.          | 2018-01-14    | 2018-01-14          |                  1 |                        26 |
```