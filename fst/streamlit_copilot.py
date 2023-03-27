import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import os
from functools import lru_cache
import numpy as np
import streamlit_ace
from fst.db_utils import get_duckdb_file_path


# Function to fetch metrics data from DuckDB
@st.cache_data
def fetch_metrics_data():
    with duckdb.connect("fst_metrics.duckdb") as duckdb_conn:
        metrics_df = duckdb_conn.execute("SELECT * FROM metrics").fetchdf()

    return metrics_df


# Fetch the metrics data
metrics_df = fetch_metrics_data()

# Streamlit app
st.title("fst Copilot")


@lru_cache(maxsize=1)
def get_duckdb_conn():
    return duckdb.connect(get_duckdb_file_path())


@st.cache_data
def run_query(query):
    with get_duckdb_conn() as conn:
        result = conn.execute(query).fetchdf()
    return result


sql_placeholder = """-- Write your SQL query here for ad hoc investigations
-- pink background indicates duplicate values
-- yellow background indicates null values
select 1 as id
"""

query = streamlit_ace.st_ace(
    value=sql_placeholder,
    theme="tomorrow",
    height=150,
    language="sql",
    key="sql_input",
    auto_update=True,
)

class DataFrameHighlighter:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def highlight(self):
        def column_style(column):
            is_duplicate = column.duplicated(keep=False)
            is_null = column.isna()
            styles = []

            for dup, null in zip(is_duplicate, is_null):
                if null:
                    styles.append("background-color: lightyellow")
                elif dup:
                    styles.append("background-color: lightpink")
                else:
                    styles.append("")
            return styles

        return self.dataframe.style.apply(column_style, axis=0)


if query.strip():
    try:
        df = run_query(query)
        highlighted_df = DataFrameHighlighter(df).highlight()
        st.dataframe(highlighted_df)
    except Exception as e:
        st.error(f"Error running query: {e}")
else:
    st.error("Query is empty.")

sorted_metrics_df = metrics_df.sort_values(by="timestamp", ascending=False)

index_options = [
    f"{row.timestamp} - {os.path.basename(row.modified_sql_file)}"
    for row in sorted_metrics_df.itertuples()
]
selected_option = st.selectbox(
    "Select a row to display the result preview:",
    options=index_options,
    index=0,
)
selected_index = index_options.index(selected_option)
selected_row = sorted_metrics_df.iloc[selected_index]
result_preview_df = pd.read_json(selected_row["result_preview_json"])
st.write(result_preview_df)
st.code(f"{selected_row['modified_sql_file']}", language="text")

filtered_metrics_df = metrics_df.loc[
    metrics_df["modified_sql_file"] == selected_row["modified_sql_file"]
].copy()

average_dbt_build_time = filtered_metrics_df["dbt_build_time"].mean()
average_query_time = filtered_metrics_df["query_time"].mean()

st.write(
    f"Average `dbt build` time: **{average_dbt_build_time:.2f} seconds** | Average query time: **{average_query_time:.2f} seconds**"
)


@st.cache_data
def calculate_rolling_average(df, column):
    return df[column].rolling(window=len(df), min_periods=1).mean()


filtered_metrics_df.loc[:, "rolling_average"] = calculate_rolling_average(
    filtered_metrics_df, "dbt_build_time"
)

filtered_metrics_df = filtered_metrics_df.reset_index()

fig = px.line(
    filtered_metrics_df,
    x="index",
    y="rolling_average",
    labels={"index": "# of Modifications", "rolling_average": "Time in Seconds"},
)

fig.update_traces(
    line=dict(color="orange", width=2), name="Rolling Average", showlegend=True
)

status_colors = {"success": "#AEC6CF", "failure": "#FF6961"}

for status, color in status_colors.items():
    mask = filtered_metrics_df["dbt_build_status"] == status
    fig.add_bar(
        x=filtered_metrics_df.loc[mask, "index"],
        y=filtered_metrics_df.loc[mask, "dbt_build_time"],
        marker=dict(color=color),
        name=status.capitalize(),
    )

st.write(fig)

metrics_df["base_modified_sql_file"] = metrics_df["modified_sql_file"].apply(
    os.path.basename
)

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

st.write("### File Modifications and Average Performance Stats")
st.write(file_modifications_and_performance)

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
