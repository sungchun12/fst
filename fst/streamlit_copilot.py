import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import os

# Function to fetch metrics data from DuckDB
def fetch_metrics_data():
    with duckdb.connect("fst_metrics.duckdb") as duckdb_conn:
        metrics_df = duckdb_conn.execute("SELECT * FROM metrics").fetchdf()

    return metrics_df


# Fetch the metrics data
metrics_df = fetch_metrics_data()

# Streamlit app
st.title("fst Copilot")

@st.cache_resource
def get_duckdb_conn():
    return duckdb.connect('jaffle_shop.duckdb')

def run_query(query):
    with get_duckdb_conn() as conn:
        result = conn.execute(query).fetchdf()
    return result

query = st.text_area("Enter your SQL query here:")

if st.button("Run Query"):
    if query.strip():
        try:
            # Cache the query results
            df = run_query(query)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error running query: {e}")
    else:
        st.error("Query is empty.")

# Sort metrics_df by timestamp in descending order
sorted_metrics_df = metrics_df.sort_values(by="timestamp", ascending=False)

index_options = [
    f"{row.timestamp} - {os.path.basename(row.modified_sql_file)}"
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
st.code(f"{selected_row['modified_sql_file']}", language="text")

# Filter metrics_df based on the selected option
filtered_metrics_df = metrics_df.loc[
    metrics_df["modified_sql_file"] == selected_row["modified_sql_file"]
].copy()  # Create a copy of the DataFrame to avoid SettingWithCopyWarning

# Calculate average dbt build time and average query time for the selected option
average_dbt_build_time = filtered_metrics_df["dbt_build_time"].mean()
average_query_time = filtered_metrics_df["query_time"].mean()

# Performance Metrics
st.write(
    f"Average `dbt build` time: **{average_dbt_build_time:.2f} seconds** | Average query time: **{average_query_time:.2f} seconds**"
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


# Add a bar chart for dbt build times, with colors based on the dbt_build_status
# Define the color mapping
status_colors = {"success": "#AEC6CF", "failure": "#FF6961"}

# Add separate bars for success and failure unique builds
for status, color in status_colors.items():
    mask = filtered_metrics_df["dbt_build_status"] == status
    fig.add_bar(
        x=filtered_metrics_df.loc[mask, "index"],
        y=filtered_metrics_df.loc[mask, "dbt_build_time"],
        marker=dict(color=color),
        name=status.capitalize(),
    )


# Plot the combined chart
st.write(fig)

# Calculate the number of modifications per file and average performance stats related to each file
# Create a new column with the base file name
metrics_df["base_modified_sql_file"] = metrics_df["modified_sql_file"].apply(
    os.path.basename
)

# Group by the base_modified_sql_file column
modifications_per_file = (
    metrics_df.groupby("base_modified_sql_file")
    .size()
    .reset_index(name="num_modifications")
)
average_performance_stats = (
    metrics_df.groupby("base_modified_sql_file")[["dbt_build_time", "query_time"]]
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
st.code(f"{selected_row['compiled_sql_file']}", language="text")

if show_code:
    with open(selected_row["compiled_sql_file"], "r") as f:
        compiled_sql_file_contents = f.read()
    st.code(compiled_sql_file_contents, language="sql")
