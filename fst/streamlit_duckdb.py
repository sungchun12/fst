import streamlit as st
import pandas as pd
import duckdb

# Function to fetch metrics data from DuckDB
def fetch_metrics_data():
    duckdb_conn = duckdb.connect("fst_metrics.duckdb")
    metrics_df = duckdb_conn.execute("SELECT * FROM metrics").fetchdf()
    duckdb_conn.close()
    return metrics_df

# Fetch the metrics data
metrics_df = fetch_metrics_data()

# Streamlit app
st.title("fst Performance and Productivity")

# Show the fst_metrics data
st.write("## Result Preview")
st.write(metrics_df)

# Calculate average dbt build time and average query time
average_dbt_build_time = metrics_df["dbt_build_time"].mean()
average_query_time = metrics_df["query_time"].mean()

# Performance Metrics
st.write("## Performance Metrics")
st.write(f"Average `dbt build` time: {average_dbt_build_time:.2f} seconds")
st.write(f"Average query time: {average_query_time:.2f} seconds")

# Visualizations
st.write("## Visualizations")
st.bar_chart(metrics_df["dbt_build_time"])
