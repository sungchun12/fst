<p align="center">
  <img src="./images/logo.jpeg" alt="fst: flow state tool]">
</p>


# fst

**fst: flow state tool**

Let's make it the overwhelming normal that these questions are answered in seconds or less when engineering data.

- Who else is touching this precious file of mine? I’m tired of pull request clashing

- What’s historical performance on this and am I beating it?

- How often does this fail in production?

- Who uses this model and how often?

- What dashboards will this help vs. hurt?

- What’s a data preview based on my updates look like? (e.g. 5 rows) FIRST THING TO DO

- How many scheduled data pipelines are tied to this model?

- How much does this cost to run in production and am I helping vs. hurting?

- What are existing database permissions on this model?

- Anyone working on pull requests in real time that rely on my work?

- What’s a data diff compared to current production data?


```bash
# my command to run this tool in an infinite loop in a split terminal
git clone https://github.com/dbt-labs/jaffle_shop_duckdb.git
cd jaffle_shop_duckdb
python -m venv venv
source venv/bin/activate   
pip install -e ../ # installing the fst package locally
# fst start --file-path <file path>
fst start --file-path /Users/sung/Desktop/fst/jaffle_shop_duckdb/models/new_file.sql
```

```shell
# example of running this tool on each modification to the sql file
 ~/De/fst/jaffle_shop_duckdb python ../fst_query.py /Users/sung/Desktop/fst/jaffle_shop_duckdb/models/new_file.sql
2023-03-18 18:39:15 - INFO - Watching directory: /Users/sung/Desktop/fst/jaffle_shop_duckdb
2023-03-18 18:39:34 - INFO - Detected modification: /Users/sung/Desktop/fst/jaffle_shop_duckdb/models/new_file.sql
2023-03-18 18:39:34 - INFO - Received query:
select * from {{ ref("customers") }} where customer_lifetime_value > 30 order by customer_lifetime_value desc limit 10
2023-03-18 18:39:34 - INFO - Running `dbt build` with the modified SQL file (new_file)...
2023-03-18 18:39:37 - INFO - `dbt build` was successful.
2023-03-18 18:39:37 - INFO - project_name: jaffle_shop
2023-03-18 18:39:37 - WARNING - Warning: No tests were run with the `dbt build` command. Consider adding tests to your project.
2023-03-18 18:39:37 - WARNING - Generated test YAML file: /Users/sung/Desktop/fst/jaffle_shop_duckdb/models/new_file.yml
2023-03-18 18:39:37 - INFO - Executing compiled query from: /Users/sung/Desktop/fst/jaffle_shop_duckdb/target/compiled/jaffle_shop/models/new_file.sql
select * from "jaffle_shop"."main"."customers" where customer_lifetime_value > 30 order by customer_lifetime_value desc limit 10

2023-03-18 18:39:37 - INFO - Using DuckDB file: jaffle_shop.duckdb
2023-03-18 18:39:37 - INFO - `dbt build` time: 3.38 seconds
2023-03-18 18:39:37 - INFO - Query time: 0.00 seconds
2023-03-18 18:39:37 - INFO - Result Preview
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|   customer_id | first_name   | last_name   | first_order   | most_recent_order   |   number_of_orders |   customer_lifetime_value |
+===============+==============+=============+===============+=====================+====================+===========================+
|            51 | Howard       | R.          | 2018-01-28    | 2018-02-23          |                  3 |                        99 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|             3 | Kathleen     | P.          | 2018-01-02    | 2018-03-11          |                  3 |                        65 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|            46 | Norma        | C.          | 2018-03-24    | 2018-03-27          |                  2 |                        64 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|            30 | Christina    | W.          | 2018-03-02    | 2018-03-14          |                  2 |                        57 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
|            54 | Rose         | M.          | 2018-01-07    | 2018-03-24          |                  5 |                        57 |
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
```