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
index_options = [
    f"{row.timestamp} - {row.modified_sql_file}" for _, row in metrics_df.iterrows()
]
selected_option = st.selectbox(
    "Select a row to display the result preview:",
    options=index_options,
    index=len(index_options) - 1,
)
selected_index = index_options.index(selected_option)
selected_row = metrics_df.iloc[selected_index]
result_preview_df = pd.read_json(selected_row["result_preview_json"])
st.write(result_preview_df)

# Filter metrics_df based on the selected option
filtered_metrics_df = metrics_df[
    metrics_df["modified_sql_file"] == selected_row["modified_sql_file"]
]

# Calculate average dbt build time and average query time for the selected option
average_dbt_build_time = filtered_metrics_df["dbt_build_time"].mean()
average_query_time = filtered_metrics_df["query_time"].mean()

# Performance Metrics
st.write("## Performance Metrics")
st.write(
    f"Average `dbt build` time for the selected option: {average_dbt_build_time:.2f} seconds"
)
st.write(
    f"Average query time for the selected option: {average_query_time:.2f} seconds"
)

# Calculate rolling average
rolling_average = (
    filtered_metrics_df["dbt_build_time"]
    .rolling(window=len(filtered_metrics_df), min_periods=1)
    .mean()
)

# Create a bar chart for dbt build times and a line chart for the rolling average
dbt_build_times_chart = pd.DataFrame(
    {"dbt_build_time": filtered_metrics_df["dbt_build_time"]}
)
rolling_average_chart = pd.DataFrame({"rolling_average": rolling_average})
combined_chart = pd.concat([dbt_build_times_chart, rolling_average_chart], axis=1)

# Plot the combined chart
st.bar_chart(combined_chart)

# Calculate the number of modifications per file and average performance stats related to each file
modifications_per_file = (
    metrics_df.groupby("modified_sql_file").size().reset_index(name="num_modifications")
)
average_performance_stats = (
    metrics_df.groupby("modified_sql_file")["dbt_build_time", "query_time"]
    .mean()
    .reset_index()
)
average_performance_stats = average_performance_stats.rename(
    columns={"dbt_build_time": "avg_dbt_build_time", "query_time": "avg_query_time"}
)
file_modifications_and_performance = pd.merge(
    modifications_per_file, average_performance_stats, on="modified_sql_file"
)

# Display the number of modifications per file and average performance stats in a simple dataframe
st.write("## File Modifications and Average Performance Stats")
st.write(file_modifications_and_performance)

# Display contents of the compiled_sql_file
st.write("## Compiled SQL File")
# Aesthetically pleasing toggle button
query_params = st.experimental_get_query_params()
show_code = query_params.get("show_code", ["False"])[0].lower() == "true"

if st.button("Toggle code snippet"):
    show_code = not show_code
    st.experimental_set_query_params(show_code=show_code)

if show_code:
    with open(selected_row["compiled_sql_file"], "r") as f:
        compiled_sql_file_contents = f.read()
    st.code(compiled_sql_file_contents, language="sql")
