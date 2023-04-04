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
from requests.exceptions import ConnectionError
from dbtc import dbtCloudClient
from typing import Any, List

# TODO: get every component in the main function
# TODO: make everything an expander
# TODO: fix build vs. compile time for more accurate stats
# TODO: what would be so killer about this is if it persists information and average performance of production models along with the dev models for slider options. You continue to progress and see what's been done before!
# TODO: fix a bug where when dbt build fails with candidate bindings that it doesn't finish the rest of the metrics collection
# TODO: add fst logo
# TODO: show potential cost of queries in dollars for production
# TODO: add a way to see WHY each iteration failed/succeeded, I should probably store the logs in fst_metrics.duckdb
# TODO: add a way to see something like dbt audit helper between iterations to see what changed


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
    dbt_cloud_workbench()
    display_query_section()
    # transpile_sql_util() # TODO add this back in if it's useful
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
            slider_label = "**Move the slider left to right viewing model changes in code/data/performance compared to the latest iteration:**"

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

        old_code = selected_row["compiled_query"]
        with open(selected_row["compiled_sql_file"], "r") as f:
            latest_code = f.read()

        selected_timestamp(selected_iteration)
        show_selected_data_preview(selected_row)
        view_code_diffs(old_code, latest_code, key="compare_old_latest")
        compare_two_iterations(filtered_metrics_df)
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


def view_code_diffs(old_code: str, new_code: str, key: str = None) -> None:
    diff_viewer.diff_viewer(old_text=old_code, new_text=new_code, lang="sql", key=key)


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
        sql_placeholder = (
            "SELECT DATEADD(year, '1', TIMESTAMP'2020-01-01') as 'foo bar'"
        )
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


def compare_two_iterations(filtered_metrics_df: pd.DataFrame) -> None:
    expander = st.expander(
        "**Compare any 2 iterations side by side for the dbt model in focus**"
    )
    with expander:
        iterations = filtered_metrics_df["timestamp"].tolist()
        indexed_iterations = [(i, ts) for i, ts in enumerate(iterations)]

        select_box_left, select_box_right = st.columns(2)

        with select_box_left:
            first_iteration = st.selectbox(
                "**Left Iteration:**",
                options=indexed_iterations,
                index=0,
                format_func=lambda x: f"{x[0]} - {x[1]}",
                help="Select the left iteration for comparison",
                key="select_box_left",  # Add a unique key
            )

        with select_box_right:
            second_iteration = st.selectbox(
                "**Right Iteration:**",
                options=indexed_iterations,
                index=len(iterations) - 1,
                format_func=lambda x: f"{x[0]} - {x[1]}",
                help="Select the right iteration for comparison",
                key="select_box_right",  # Add a unique key
            )

        first_row = filtered_metrics_df.loc[
            filtered_metrics_df["timestamp"] == first_iteration[1]
        ].iloc[0]

        second_row = filtered_metrics_df.loc[
            filtered_metrics_df["timestamp"] == second_iteration[1]
        ].iloc[0]

        col1, col2 = st.columns(2)

        with col1:
            show_selected_data_preview(first_row)

        with col2:
            show_selected_data_preview(second_row)

        old_code = first_row["compiled_query"]
        new_code = second_row["compiled_query"]
        view_code_diffs(old_code, new_code, key="compare_two_iterations")


# dbt Cloud Metrics Dashboard. This dashboard is designed to help you understand your workbench progress in the aim of improving your dbt Cloud deployment experience(read: you're confident about what you're shipping works)
# I'll put this at the top of the page
# Huge shoutout to Doug Guthrie for the awesome code below
# an expander: "Unleash your potential"
def dbt_cloud_workbench() -> None:
    expander = st.expander(
        "**Unleash your potential: Compare your work to Production**"
    )
    with expander:
        col1, col2 = st.columns(2)
        with col1:
            dbt_cloud_host_url = get_host_url()
        with col2:
            service_token = get_service_token()
        validate_service_token(service_token)
        col3, col4, col5, col6 = st.columns(4)
        with col3:
            get_account_widget()
        with col4:
            get_project_widget()
        with col5:
            get_environment_widget()
        with col6:
            get_run_widget()


def get_host_url() -> None:
    dbt_cloud_host_url = st.text_input(
        label="Enter your dbt Cloud host URL ",
        value="cloud.getdbt.com",
        key="dbt_cloud_host_url",
        help="Only change if you're on a single tenant instance or in a non-US multi-tenant region",
    )
    return dbt_cloud_host_url


# api key input box similar to Doug's example, tooltip and link to docs to get it


def get_service_token() -> None:
    service_token = st.text_input(
        label="Enter your dbt Cloud service token",
        value="",
        type="password",
        key="dbt_cloud_service_token",
        help="[Instructions to generate an API service token with permissions: ['Metadata Only', 'Job Admin']](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens#generating-service-account-tokens)",
    )
    return service_token


def validate_service_token(service_token: str) -> None:
    if st.session_state.dbt_cloud_service_token != "":
        st.cache_data.clear()
        st.session_state.dbtc_client = dbtCloudClient(
            service_token=st.session_state.dbt_cloud_service_token,
            host=st.session_state.dbt_cloud_host_url,
        )
        accounts = dynamic_request(
            st.session_state.dbtc_client.cloud, "list_accounts"
        ).get("data", [])
        st.session_state.accounts = list_to_dict(accounts)
        try:
            st.session_state.account_id = list(st.session_state.accounts.keys())[0]
        except IndexError:
            st.error(
                "No accounts were found with the service token entered.  "
                "Please try again."
            )
        else:
            projects = dynamic_request(
                st.session_state.dbtc_client.cloud,
                "list_projects",
                st.session_state.account_id,
            ).get("data", [])
            st.session_state.projects = projects
            st.success("Success!  Explore the rest of the dbt Cloud Workbench!")


def get_account_widget(states: List[str] = [
    'project_index', 'job_index', 'environment_index'
]):
    accounts = dynamic_request(
        st.session_state.dbtc_client.cloud,
        'list_accounts',
    ).get('data', [])
    accounts = list_to_dict(accounts)
    st.session_state.accounts = accounts
    return st.selectbox(
        label='Select Account',
        options=accounts.keys(),
        format_func=lambda x: accounts[x]['name'],
        key='account_id',
        on_change=clear_session_state,
        args=(states, )
    )


def get_project_widget(states: List[str] = [], is_required: bool = True):
    projects = dynamic_request(
        st.session_state.dbtc_client.cloud,
        'list_projects',
        st.session_state.account_id,
    ).get('data', [])
    projects = list_to_dict(projects)
    options = list(projects.keys())
    if not is_required:
        options.insert(0, None)
    st.selectbox(
        label='Select Project',
        options=options,
        format_func=lambda x: projects[x]['name'] if x is not None else x,
        key='project_id',
        on_change=clear_session_state,
        args=(states, )
    )
    st.session_state.projects = projects


def get_environment_widget(is_required: bool = True, **kwargs):
    environments = dynamic_request(
        st.session_state.dbtc_client.cloud,
        'list_environments',
        st.session_state.account_id,
        project_id=st.session_state.get('project_id', []),
        **kwargs
    ).get('data', [])
    environments = list_to_dict(environments)
    options = list(environments.keys())
    if not is_required:
        options.insert(0, None)
    st.session_state.environments = environments
    return st.selectbox(
        label='Select Environment',
        options=options,
        format_func=lambda x: environments[x]['name'] if x is not None else x,
        key='environment_id',
    )


def get_run_widget(is_required: bool = True, **kwargs):
    runs = dynamic_request(
        st.session_state.dbtc_client.cloud,
        'list_runs',
        st.session_state.account_id,
        job_definition_id=st.session_state.get('job_id', None),
        order_by='-id',
        **kwargs,
    ).get('data', [])
    runs = list_to_dict(runs, value_field='id', reverse=True)
    options = list(runs.keys())
    if not is_required:
        options.insert(0, None)
    return st.selectbox(
        label='Select Run',
        options=options,
        format_func=lambda x: runs[x]['id'] if x is not None else x,
        key='run_id',
    )


@st.cache_data(show_spinner=False)
def dynamic_request(_prop, method, *args, **kwargs):
    try:
        return getattr(_prop, method)(*args, **kwargs)
    except ConnectionError as e:
        st.error(e)
        st.stop()

def clear_session_state(states: List[str]):
    for state in states:
        if state in st.session_state:
            del st.session_state[state]

def list_to_dict(
    ls: List[Any],
    id_field: str = "id",
    value_field: str = "name",
    reverse: bool = False,
):
    if ls:
        ls = sorted(ls, key=lambda d: d[value_field], reverse=reverse)
        return {d[id_field]: d for d in ls}

    return {}


# pick a dbt cloud account and project based on the plain name, use a select box
# do a fuzzy match on the model name
# create a chart to show execution time over n production runs, show the view vs. table, success vs. failure
# default the input text box to 10 and allow the user to change it
# Show code diff to compare any workbench iteration to any production cloud run iteration(in the future maybe anyone's iteration)

# Have hyperlinks to allow the user to easily navigate to the dbt Cloud UI for the selected model job run


# show a table of tests and their pass and failure, if no tests, show a warning message and give the user a next step to modify the file and fst will take care of that for you ;)
if __name__ == "__main__":
    main()
