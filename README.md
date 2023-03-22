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

> **Note:** This tool is still in development. Please feel free to contribute to this project.

## Description

This is a file watcher that runs `dbt build` and a duckdb query preview on a SQL file when it detects a modification. It also generates a test file for the modified SQL file if tests are not detected. The compiled SQL file is then executed to get the query result.

It works with any SQL file within the current working directory of the dbt project. It also works with any SQL file within the `models` directory of the project.

It works with any SQL file within the `models/` directory of the dbt project. You must run this tool from the root directory of the dbt project.

You'll notice for the sake of MVP, I am running nested git clones to get this working. I'll release to pypi soon.

```bash
# my command to run this tool in an infinite loop in a split terminal
git clone https://github.com/sungchun12/fst.git
cd fst
git clone https://github.com/dbt-labs/jaffle_shop_duckdb.git
cd jaffle_shop_duckdb
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
source venv/bin/activate
pip3 install -r requirements.txt
pip3 install -e ../ # installing the fst package locally
code .
fst start
```

```shell
# example of running this tool on each modification to any SQL file within models/
# pro tip: open up the compiled query in a split IDE window for hot reloading as you develope
2023-03-22 11:05:29 - INFO - Running `dbt build` with the modified SQL file (/Users/sung/fst/jaffle_shop_duckdb/models/new_file.sql)...
2023-03-22 11:05:33 - INFO - `dbt build` was successful.
2023-03-22 11:05:33 - INFO - 18:05:32  Running with dbt=1.4.5
18:05:32  Found 6 models, 20 tests, 0 snapshots, 0 analyses, 297 macros, 0 operations, 3 seed files, 0 sources, 0 exposures, 0 metrics
18:05:32  
18:05:32  Concurrency: 24 threads (target='dev')
18:05:32  
18:05:32  1 of 1 START sql table model main.new_file ..................................... [RUN]
18:05:33  1 of 1 OK created sql table model main.new_file ................................ [OK in 0.12s]
18:05:33  
18:05:33  Finished running 1 table model in 0 hours 0 minutes and 0.25 seconds (0.25s).
18:05:33  
18:05:33  Completed successfully
18:05:33  
18:05:33  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1

2023-03-22 11:05:33 - WARNING - Warning: No tests were run with the `dbt build` command. Consider adding tests to your project.
2023-03-22 11:05:33 - WARNING - Generated test YAML file: /Users/sung/fst/jaffle_shop_duckdb/models/new_file.yml
2023-03-22 11:05:33 - WARNING - Running `dbt test` with the generated test YAML file...
2023-03-22 11:05:37 - INFO - `dbt test` with generated tests was successful.
2023-03-22 11:05:37 - INFO - 18:05:36  Running with dbt=1.4.5
18:05:36  Found 6 models, 22 tests, 0 snapshots, 0 analyses, 297 macros, 0 operations, 3 seed files, 0 sources, 0 exposures, 0 metrics
18:05:36  
18:05:36  Concurrency: 24 threads (target='dev')
18:05:36  
18:05:36  1 of 2 START test not_null_new_file_customer_id ................................ [RUN]
18:05:36  2 of 2 START test unique_new_file_customer_id .................................. [RUN]
18:05:36  1 of 2 PASS not_null_new_file_customer_id ...................................... [PASS in 0.07s]
18:05:36  2 of 2 PASS unique_new_file_customer_id ........................................ [PASS in 0.07s]
18:05:36  
18:05:36  Finished running 2 tests in 0 hours 0 minutes and 0.17 seconds (0.17s).
18:05:36  
18:05:36  Completed successfully
18:05:36  
18:05:36  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2

2023-03-22 11:05:37 - INFO - Executing compiled query from: /Users/sung/fst/jaffle_shop_duckdb/target/compiled/jaffle_shop/models/new_file.sql
2023-03-22 11:05:37 - INFO - Using DuckDB file: jaffle_shop.duckdb
2023-03-22 11:05:37 - INFO - `dbt build` time: 4.28 seconds
2023-03-22 11:05:37 - INFO - Query time: 0.00 seconds
2023-03-22 11:05:37 - INFO - Result Preview
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
```
