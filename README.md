<p align="center">
  <img src="./images/fst_logo.png" alt="fst: flow state tool]">
</p>

fst(flow state tool): A tool to help you stay in flow state while developing dbt models.

Let's make it the overwhelming normal that these questions are answered in seconds or less when engineering data(think: you don't need 10+ tabs and 40+ mouse clicks to do your jobs)

<details>
  <summary>Questions to Answer</summary>
  
- Who else is touching this precious file of mine? I’m tired of pull request clashing

- What’s historical performance on this and am I beating it?

- How often does this fail in production?

- Who uses this model and how often?

- What dashboards will this help vs. hurt?

- What’s a data preview based on my updates look like? (e.g. 5 rows)

- How many scheduled data pipelines are tied to this model?

- How much does this cost to run in production and am I helping vs. hurting?

- What are existing database permissions on this model?

- Anyone working on pull requests in real time that rely on my work?

- What’s a data diff compared to current production data?
  
</details>

> **Note:** This tool is still in development. Please feel free to contribute to this project.

## Description

This is a file watcher for a dbt project using duckdb as the database. It runs `dbt build` and a query preview on a SQL file when it detects a modification. It also generates a test file for the modified SQL file if tests are not detected.

It works with any SQL file within the `models/` directory of the dbt project. You must run this tool from the root directory of the dbt project.

You'll notice for the sake of MVP, I am running nested git clones to get this working. I'll release to pypi soon.


```bash
# my command to run this tool in an infinite loop in a split terminal
git clone https://github.com/sungchun12/fst.git
cd fst
git clone https://github.com/dbt-labs/jaffle_shop_duckdb.git
cd jaffle_shop_duckdb
python -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
source venv/bin/activate
pip install -r requirements.txt # installs the dbt dependencies
pip install -e ../ # installs the fst package locally
dbt build # Create the duckb database file and get commands working
fst start
```

```shell
# example of running this tool on each modification to any SQL file within the `models/` directory
# pro tip: open up the compiled query in a split IDE window for hot reloading as you develop
2023-04-12 11:30:15 - INFO - Running `dbt build` with the modified SQL file (/Users/sung/fst/jaffle_shop_duckdb/models/new_file.sql)...
2023-04-12 11:30:21 - INFO - `dbt build` was successful.
2023-04-12 11:30:21 - INFO - 18:30:20  Running with dbt=1.4.5
18:30:20  Found 7 models, 22 tests, 0 snapshots, 0 analyses, 297 macros, 0 operations, 3 seed files, 0 sources, 0 exposures, 0 metrics
18:30:20  
18:30:20  Concurrency: 24 threads (target='dev')
18:30:20  
18:30:20  1 of 1 START sql table model main.new_file ..................................... [RUN]
18:30:21  1 of 1 OK created sql table model main.new_file ................................ [OK in 0.26s]
18:30:21  
18:30:21  Finished running 1 table model in 0 hours 0 minutes and 0.53 seconds (0.53s).
18:30:21  
18:30:21  Completed successfully
18:30:21  
18:30:21  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1

2023-04-12 11:30:21 - WARNING - Warning: No tests were run with the `dbt build` command. Consider adding tests to your project.
2023-04-12 11:30:21 - WARNING - Generated test YAML file: /Users/sung/fst/jaffle_shop_duckdb/models/new_file.yml
2023-04-12 11:30:21 - WARNING - Running `dbt test` with the generated test YAML file...
2023-04-12 11:30:28 - INFO - `dbt test` with generated tests was successful.
2023-04-12 11:30:28 - INFO - 18:30:26  Running with dbt=1.4.5
18:30:27  Found 7 models, 24 tests, 0 snapshots, 0 analyses, 297 macros, 0 operations, 3 seed files, 0 sources, 0 exposures, 0 metrics
18:30:27  
18:30:27  Concurrency: 24 threads (target='dev')
18:30:27  
18:30:27  1 of 2 START test not_null_new_file_customer_id ................................ [RUN]
18:30:27  2 of 2 START test unique_new_file_customer_id .................................. [RUN]
18:30:27  1 of 2 PASS not_null_new_file_customer_id ...................................... [PASS in 0.20s]
18:30:27  2 of 2 PASS unique_new_file_customer_id ........................................ [PASS in 0.21s]
18:30:27  
18:30:27  Finished running 2 tests in 0 hours 0 minutes and 0.44 seconds (0.44s).
18:30:27  
18:30:27  Completed successfully
18:30:27  
18:30:27  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2

2023-04-12 11:30:28 - INFO - Executing compiled query from: /Users/sung/fst/jaffle_shop_duckdb/target/compiled/jaffle_shop/models/new_file.sql
2023-04-12 11:30:28 - INFO - Using DuckDB file: jaffle_shop.duckdb
2023-04-12 11:30:28 - INFO - `dbt build` time: 6.76 seconds
2023-04-12 11:30:28 - INFO - Query time: 0.00 seconds
2023-04-12 11:30:28 - INFO - Result Preview
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
+---------------+--------------+-------------+---------------+---------------------+--------------------+---------------------------+
2023-04-12 11:30:28 - INFO - fst metrics saved to the database: fst_metrics.duckdb
```

> Note: Tested with python version: 3.8.9 on MacOs Intel