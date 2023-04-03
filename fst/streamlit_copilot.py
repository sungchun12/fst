import os
from functools import cached_property, lru_cache
from typing import List, Optional
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit_ace
from fst.db_utils import get_duckdb_file_path
import diff_viewer
import pytz
import sqlglot

# TODO: get every component in the main function
# TODO: make everything an expander
# TODO: fix build vs. compile time for more accurate stats
# TODO: what would be so killer about this is if it persists information and average performance of production models along with the dev models for slider options. You continue to progress and see what's been done before!
# TODO: fix a bug where when dbt build fails with candidate bindings that it doesn't finish the rest of the metrics collection
# TODO: add fst logo
# TODO: show potential cost of queries in dollars for production
# TODO: add a way to see WHY each iteration failed/succeeded, I should probably store the logs in fst_metrics.duckdb
#TODO: add a way to see something like dbt audit helper between iterations to see what changed


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
                elif dup and "duplicates" in self.highlight_options:
                    style = "background-color: lightpink"
                styles.append(style)
            return styles

        return self.dataframe.style.apply(column_style, axis=0)


def main() -> None:
    metrics_df = fetch_metrics_data()
    display_query_section()
    show_metrics(metrics_df)
    transpile_sql_util()


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

    expander = st.expander(
        "**Run ad hoc SQL queries against your development database**"
    )
    with expander:
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
        "**Focus on a dbt model to work on:**",
        options=model_options,
        index=0,
        help="Only models that have been modified at least once are shown here with the full file path",
    )

    filtered_metrics_df = sorted_metrics_df.loc[
        sorted_metrics_df["modified_sql_file"] == selected_model
    ].copy()
    filtered_metrics_df = filtered_metrics_df.reset_index()

    iteration_options = filtered_metrics_df["timestamp"].tolist()
    num_iterations = len(iteration_options)
    if len(iteration_options) > 0:
        if num_iterations > 1:
            min_iteration_index = 0
            max_iteration_index = num_iterations - 1
            slider_label = "**Move the slider left and right to see how the model changed in code/performance with a data preview:**"

            selected_iteration_index = st.slider(
                slider_label,
                min_value=min_iteration_index,
                max_value=max_iteration_index,
                value=max_iteration_index,
                format="%d",
                help="The slider starts at zero and adds options for this model as you modify it",
            )

            selected_iteration = iteration_options[selected_iteration_index]
        else:
            selected_iteration = iteration_options[0]
            st.write("*No Slider Options: There is only one iteration available*")

        selected_row = filtered_metrics_df.loc[
            filtered_metrics_df["timestamp"] == selected_iteration
        ].iloc[0]
        selected_iteration_index = filtered_metrics_df.index[
            filtered_metrics_df["timestamp"] == selected_iteration
        ].tolist()[0]

        selected_timestamp(selected_iteration)
        show_selected_data_preview(selected_row)
        view_code_diffs(selected_row)
        show_performance_metrics(
            selected_row, sorted_metrics_df, selected_iteration_index
        )
        show_compiled_code_latest(selected_row)
        show_compiled_code_selected(selected_row)
    else:
        st.warning(
            "No iterations found for any dbt models. Modify a dbt model to see results here."
        )


def selected_timestamp(selected_iteration: pd.Series) -> None:
    utc_timestamp = selected_iteration.strftime("%Y-%m-%d %H:%M:%S")
    pacific = pytz.timezone("US/Pacific")
    pacific_timestamp = (
        selected_iteration.replace(tzinfo=pytz.utc)
        .astimezone(pacific)
        .strftime("%Y-%m-%d %I:%M:%S %p")
    )

    st.write(
        f"*Selected slider option timestamp (UTC): {utc_timestamp} | (Pacific Time): {pacific_timestamp}*"
    )


def show_selected_data_preview(selected_row: pd.Series) -> None:
    result_preview_df = pd.read_json(selected_row["result_preview_json"])
    st.write(result_preview_df)


def show_performance_metrics(
    selected_row: pd.Series,
    metrics_df: pd.DataFrame,
    selected_iteration_index: Optional[int] = None,
) -> None:
    filtered_metrics_df = metrics_df.loc[
        metrics_df["modified_sql_file"] == selected_row["modified_sql_file"]
    ].copy()

    if selected_iteration_index is None:
        selected_iteration_index = metrics_df["index"].min()

    filtered_metrics_df.loc[:, "rolling_average"] = calculate_rolling_average(
        filtered_metrics_df, "dbt_build_time"
    )

    filtered_metrics_df = filtered_metrics_df.reset_index()

    # Pass the selected_iteration_index as the second argument
    fig = create_line_chart(filtered_metrics_df, selected_iteration_index)

    st.write(fig)

    show_file_modifications_and_performance_metrics(metrics_df)


@st.cache_data
def calculate_rolling_average(df: pd.DataFrame, column: str) -> pd.Series:
    return df[column].rolling(window=len(df), min_periods=1).mean()


def create_line_chart(df: pd.DataFrame, selected_iteration_index: int) -> px.line:
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

    if not df.empty:
        selected_row = df.loc[selected_iteration_index]
        fig.add_shape(
            type="rect",
            x0=selected_row["index"] - 0.5,
            x1=selected_row["index"] + 0.5,
            y0=0,
            y1=selected_row["dbt_build_time"],
            yref="y",
            xref="x",
            line=dict(color="purple", width=2, dash="dot"),
            fillcolor="purple",
            opacity=0.2,
        )

    return fig


def show_file_modifications_and_performance_metrics(metrics_df: pd.DataFrame) -> None:
    metrics_df["base_modified_sql_file"] = metrics_df["modified_sql_file"].apply(
        os.path.basename
    )

    file_modifications_and_performance = get_file_modifications_and_performance_metrics(
        metrics_df
    )

    st.write("*All File Modifications and Average Performance Stats*")
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


def show_compiled_code_latest(selected_row: pd.Series) -> None:
    query_params = st.experimental_get_query_params()
    show_code = query_params.get("show_code", ["False"])[0].lower() == "true"

    expander = st.expander(
        "**Show latest compiled code for focused on dbt model**", expanded=show_code
    )
    with expander:
        st.code(f"{selected_row['compiled_sql_file']}", language="text")

        with open(selected_row["compiled_sql_file"], "r") as f:
            compiled_sql_file_contents = f.read()
        st.code(compiled_sql_file_contents, language="sql")


def show_compiled_code_selected(selected_row: pd.Series) -> None:
    query_params = st.experimental_get_query_params()
    show_code = query_params.get("show_code", ["False"])[0].lower() == "true"

    expander = st.expander(
        "**Show compiled code for the dbt model slider option selected**",
        expanded=show_code,
    )
    with expander:
        compiled_query = selected_row["compiled_query"]
        st.code(compiled_query, language="sql")


def view_code_diffs(selected_row: pd.Series) -> None:
    old_code = selected_row["compiled_query"]
    with open(selected_row["compiled_sql_file"], "r") as f:
        new_code = f.read()

    expander = st.expander(
        "View code diffs [Left=Slider Option, Right=Latest Code, Blank=No diffs]",
        expanded=True,
    )
    with expander:
        diff_viewer.diff_viewer(old_text=old_code, new_text=new_code, lang="sql")


def transpile_sql_util() -> None:
    expander = st.expander(
        "**Transpile SQL from and to dialects to get the benefits of syntax problems solved for one dialect to automatically translate to the one you're focused on**",
        expanded=True,
    )
    with expander:
        # Define the list of supported dialects
        SUPPORTED_DIALECTS = [
            "databricks",
            "redshift",
            "postgres",
            "duckdb",
            "sqlite",
            "snowflake",
            "bigquery",
            "trino",
            "clickhouse",
            "drill",
            "hive",
            "mysql",
            "oracle",
            "presto",
            "spark",
            "starrocks",
            "tableau",
            "teradata",
            "tsql",
        ]
        # Create an Ace text area for input SQL
        sql_placeholder = "SELECT DATEADD(year, '1', TIMESTAMP'2020-01-01') as 'foo bar'"
        input_sql = streamlit_ace.st_ace(
            value=sql_placeholder,
            theme="tomorrow",
            height=150,
            language="sql",
            key="input_sql",
            auto_update=True,
        )

        # Create a 'from' dialect dropdown and a 'to' dialect dropdown side by side
        from_dialect, to_dialect = st.columns(2)
        from_dialect = from_dialect.selectbox("From Dialect", SUPPORTED_DIALECTS)
        to_dialect = to_dialect.selectbox("To Dialect", SUPPORTED_DIALECTS)

        # Create a button to trigger the transpilation
        if st.button("Transpile SQL", use_container_width=True, type="primary"):
            try:
                # Transpile the input SQL from the selected 'from' dialect to the selected 'to' dialect
                transpiled_sql = sqlglot.transpile(
                    input_sql, read=from_dialect, write=to_dialect
                )[0]

                # Display the transpiled SQL
                st.code(transpiled_sql, language="sql")
            except Exception as e:
                st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
