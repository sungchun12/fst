import os
from functools import cached_property, lru_cache
from typing import List
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit_ace
from fst.db_utils import get_duckdb_file_path
import diff_viewer
import pytz


# TODO: show a datadiff of the data that changed between current iteration and production
# TODO: make a selection box for the model name and a select slider for the iteration based on the timestamp, have it highlight the iteration in the chart with a dotted line bar
# TODO: fix build vs. compile time for more accurate stats
@lru_cache(maxsize=1)
def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(get_duckdb_file_path())


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    with get_duckdb_conn() as conn:
        result = conn.execute(query).fetchdf()
    return result


class DataFrameHighlighter:
    def __init__(self, dataframe: pd.DataFrame, highlight_options: List[str]):
        self.dataframe = dataframe
        self.highlight_options = highlight_options

    @cached_property
    def highlight(self) -> pd.io.formats.style.Styler:
        def column_style(column: pd.Series) -> List[str]:
            is_duplicate = column.duplicated(keep=False)
            is_null = column.isna()
            styles = []

            for dup, null in zip(is_duplicate, is_null):
                style = ""
                if null and "nulls" in self.highlight_options:
                    style = "background-color: lightyellow"
                if dup and "duplicates" in self.highlight_options:
                    style = "background-color: lightpink"
                styles.append(style)
            return styles

        return self.dataframe.style.apply(column_style, axis=0)


def main() -> None:
    st.title("fst Copilot")

    metrics_df = fetch_metrics_data()
    display_query_section()
    show_metrics(metrics_df)


def fetch_metrics_data() -> pd.DataFrame:
    with duckdb.connect("fst_metrics.duckdb") as duckdb_conn:
        create_metrics_table = """
            CREATE TABLE IF NOT EXISTS metrics (
                timestamp TIMESTAMP,
                modified_sql_file TEXT,
                compiled_sql_file TEXT,
                compiled_query VARCHAR,
                dbt_build_status TEXT,
                duckdb_file_name TEXT,
                dbt_build_time REAL,
                query_time REAL,
                result_preview_json TEXT
            )
        """
        duckdb_conn.execute(create_metrics_table)
        metrics_df = duckdb_conn.execute("SELECT * FROM metrics").fetchdf()
    return metrics_df


def display_query_section() -> None:
    sql_placeholder = (
        "-- Write your exploratory SQL query here\n"
        "-- pink == duplicate values\n"
        "-- yellow == null values\n"
        "-- show all tables command below\n"
        "show\n"
    )

    query = streamlit_ace.st_ace(
        value=sql_placeholder,
        theme="tomorrow",
        height=150,
        language="sql",
        key="sql_input",
        auto_update=True,
    )

    highlight_options = st.multiselect(
        "Highlight options:",
        options=["nulls", "duplicates"],
        default=["nulls", "duplicates"],
        help="Highlight nulls and/or duplicate values per column in the table below",
    )

    if query.strip():
        try:
            df = run_query(query)
            highlighted_df = DataFrameHighlighter(df, highlight_options).highlight
            st.dataframe(highlighted_df)
        except Exception as e:
            st.error(f"Error running query: {e}")
    else:
        st.error("Query is empty.")


def show_metrics(metrics_df: pd.DataFrame) -> None:
    sorted_metrics_df = metrics_df.sort_values(by="timestamp", ascending=True)

    model_options = sorted_metrics_df["modified_sql_file"].unique()
    selected_model = st.selectbox(
        "Select a dbt model:",
        options=model_options,
        index=0,
    )

    filtered_metrics_df = sorted_metrics_df.loc[sorted_metrics_df["modified_sql_file"] == selected_model].copy()
    filtered_metrics_df = filtered_metrics_df.reset_index()

    iteration_options = filtered_metrics_df["timestamp"].tolist()
    num_iterations = len(iteration_options)
    if len(iteration_options) > 0:
        if num_iterations > 1:
            min_iteration_index = 0
            max_iteration_index = num_iterations - 1
            slider_label = "Select an iteration (starts at 0):"

            selected_iteration_index = st.slider(
                slider_label,
                min_value=min_iteration_index,
                max_value=max_iteration_index,
                value=max_iteration_index,
                format="%d"
            )

            selected_iteration = iteration_options[selected_iteration_index]
        else:
            selected_iteration = iteration_options[0]
            st.write("There is only one iteration available.")

        selected_row = filtered_metrics_df.loc[filtered_metrics_df["timestamp"] == selected_iteration].iloc[0]

        selected_timestamp(selected_iteration)
        show_selected_row(selected_row)
        view_code_diffs(selected_row)
        show_performance_metrics(selected_row, sorted_metrics_df)
        show_compiled_code(selected_row)
        show_compiled_query(selected_row)
    else:
        st.warning("No iterations found for any dbt models. Modify a dbt model to see results here.")

def selected_timestamp(selected_iteration: pd.Series) -> None:
    utc_timestamp = selected_iteration.strftime('%Y-%m-%d %H:%M:%S')
    pacific = pytz.timezone('US/Pacific')
    pacific_timestamp = selected_iteration.replace(tzinfo=pytz.utc).astimezone(pacific).strftime('%Y-%m-%d %I:%M:%S %p')

    st.write(f"Selected iteration timestamp (UTC): {utc_timestamp} | (Pacific Time): {pacific_timestamp}")


def show_selected_row(selected_row: pd.Series) -> None:
    result_preview_df = pd.read_json(selected_row["result_preview_json"])
    st.write(result_preview_df)


def show_performance_metrics(selected_row: pd.Series, metrics_df: pd.DataFrame) -> None:
    filtered_metrics_df = metrics_df.loc[
        metrics_df["modified_sql_file"] == selected_row["modified_sql_file"]
    ].copy()

    average_dbt_build_time = filtered_metrics_df["dbt_build_time"].mean()
    average_query_time = filtered_metrics_df["query_time"].mean()

    st.write(
        f"Average `dbt build` time: **{average_dbt_build_time:.2f} seconds** | "
        f"Average query time: **{average_query_time:.2f} seconds**"
    )

    filtered_metrics_df.loc[:, "rolling_average"] = calculate_rolling_average(
        filtered_metrics_df, "dbt_build_time"
    )

    filtered_metrics_df = filtered_metrics_df.reset_index()

    fig = create_line_chart(filtered_metrics_df)
    st.write(fig)

    show_file_modifications_and_performance_metrics(metrics_df)


@st.cache_data
def calculate_rolling_average(df: pd.DataFrame, column: str) -> pd.Series:
    return df[column].rolling(window=len(df), min_periods=1).mean()


def create_line_chart(df: pd.DataFrame) -> px.line:
    fig = px.line(
        df,
        x="index",
        y="rolling_average",
        labels={
            "index": "# of Iterations over Time",
            "rolling_average": "Time in Seconds",
        },
    )

    fig.update_traces(
        line=dict(color="orange", width=2), name="Rolling Average", showlegend=True
    )

    status_colors = {"success": "#AEC6CF", "failure": "#FF6961"}

    for status, color in status_colors.items():
        mask = df["dbt_build_status"] == status
        fig.add_bar(
            x=df.loc[mask, "index"],
            y=df.loc[mask, "dbt_build_time"],
            marker=dict(color=color),
            name=status.capitalize(),
        )

    return fig


def show_file_modifications_and_performance_metrics(metrics_df: pd.DataFrame) -> None:
    metrics_df["base_modified_sql_file"] = metrics_df["modified_sql_file"].apply(
        os.path.basename
    )

    file_modifications_and_performance = get_file_modifications_and_performance_metrics(
        metrics_df
    )

    st.write("### File Modifications and Average Performance Stats")
    st.write(file_modifications_and_performance)


def get_file_modifications_and_performance_metrics(
    metrics_df: pd.DataFrame,
) -> pd.DataFrame:
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

    return file_modifications_and_performance


def show_compiled_code(selected_row: pd.Series) -> None:
    query_params = st.experimental_get_query_params()
    show_code = query_params.get("show_code", ["False"])[0].lower() == "true"

    expander = st.expander(
        "Show **latest** compiled code snippet for selected dbt model", expanded=show_code
    )
    with expander:
        st.code(f"{selected_row['compiled_sql_file']}", language="text")

        with open(selected_row["compiled_sql_file"], "r") as f:
            compiled_sql_file_contents = f.read()
        st.code(compiled_sql_file_contents, language="sql")


def show_compiled_query(selected_row: pd.Series) -> None:
    query_params = st.experimental_get_query_params()
    show_code = query_params.get("show_code", ["False"])[0].lower() == "true"

    expander = st.expander(
        "Show iteration for compiled query for selected dbt model", expanded=show_code
    )
    with expander:
        compiled_query = selected_row["compiled_query"]
        st.code(compiled_query, language="sql")


def view_code_diffs(selected_row: pd.Series) -> None:
    old_code = selected_row["compiled_query"]
    with open(selected_row["compiled_sql_file"], "r") as f:
        new_code = f.read()

    expander = st.expander("View code diffs [Left=Selection, Right=Latest, Blank=No diffs]", expanded=True)
    with expander:
        diff_viewer.diff_viewer(old_text=old_code, new_text=new_code, lang='sql')

if __name__ == "__main__":
    main()
