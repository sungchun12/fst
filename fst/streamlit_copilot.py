import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

# Function to fetch metrics data from DuckDB
def fetch_metrics_data():
    with duckdb.connect("fst_metrics.duckdb") as duckdb_conn:
        metrics_df = duckdb_conn.execute("SELECT * FROM metrics").fetchdf()

    return metrics_df


# Fetch the metrics data
metrics_df = fetch_metrics_data()

# Streamlit app
st.title("fst Performance and Productivity")

# Show the fst_metrics data
st.write("### Result Preview")

# Sort metrics_df by timestamp in descending order
sorted_metrics_df = metrics_df.sort_values(by="timestamp", ascending=False)

index_options = [
    f"{row.timestamp} - {row.modified_sql_file}"
    for _, row in sorted_metrics_df.iterrows()
]
selected_option = st.selectbox(
    "Select a row to display the result preview:",
    options=index_options,
    index=0,  # Set the default index to 0 (the first option) as the options are sorted from newest to oldest
)
selected_index = index_options.index(selected_option)
selected_row = sorted_metrics_df.iloc[selected_index]
result_preview_df = pd.read_json(selected_row["result_preview_json"])
st.write(result_preview_df)

# Filter metrics_df based on the selected option
filtered_metrics_df = metrics_df.loc[
    metrics_df["modified_sql_file"] == selected_row["modified_sql_file"]
].copy()  # Create a copy of the DataFrame to avoid SettingWithCopyWarning

# Calculate average dbt build time and average query time for the selected option
average_dbt_build_time = filtered_metrics_df["dbt_build_time"].mean()
average_query_time = filtered_metrics_df["query_time"].mean()

# Performance Metrics
st.write("### Performance Metrics")
st.write(
    f"Average `dbt build` time for the selected option: {average_dbt_build_time:.2f} seconds"
)
st.write(
    f"Average query time for the selected option: {average_query_time:.2f} seconds"
)

# Calculate rolling average
filtered_metrics_df.loc[:, "rolling_average"] = (
    filtered_metrics_df["dbt_build_time"]
    .rolling(window=len(filtered_metrics_df), min_periods=1)
    .mean()
)

# Reset the index of the filtered_metrics_df
filtered_metrics_df = filtered_metrics_df.reset_index()

# Create a line chart for the rolling average
fig = px.line(
    filtered_metrics_df,
    x="index",
    y="rolling_average",
    labels={"index": "# of Modifications", "rolling_average": "Time in Seconds"},
)

# Add a custom legend for the rolling average line
fig.update_traces(
    line=dict(color="orange", width=2), name="Rolling Average", showlegend=True
)


# Add a bar chart for dbt build times
fig.add_bar(
    x=filtered_metrics_df["index"],
    y=filtered_metrics_df["dbt_build_time"],
    name="Unique builds",
)

# Plot the combined chart
st.write(fig)

# Calculate the number of modifications per file and average performance stats related to each file
modifications_per_file = (
    metrics_df.groupby("modified_sql_file").size().reset_index(name="num_modifications")
)
average_performance_stats = (
    metrics_df.groupby("modified_sql_file")[["dbt_build_time", "query_time"]]
    .mean()
    .reset_index()
)
average_performance_stats = average_performance_stats.rename(
    columns={"dbt_build_time": "avg_dbt_build_time", "query_time": "avg_query_time"}
)
file_modifications_and_performance = pd.merge(
    modifications_per_file, average_performance_stats
)
# Display the number of modifications per file and average performance stats in a simple dataframe
st.write("### File Modifications and Average Performance Stats")
st.write(file_modifications_and_performance)

# Display contents of the compiled_sql_file
# Aesthetically pleasing toggle button
query_params = st.experimental_get_query_params()
show_code = query_params.get("show_code", ["False"])[0].lower() == "true"

if st.button("Toggle **latest** compiled code snippet for selected option"):
    show_code = not show_code
    st.experimental_set_query_params(show_code=show_code)

if show_code:
    with open(selected_row["compiled_sql_file"], "r") as f:
        compiled_sql_file_contents = f.read()
    st.code(compiled_sql_file_contents, language="sql")
