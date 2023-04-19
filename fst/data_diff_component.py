import logging
import tzlocal
import pandas as pd
import snowflake.connector
import streamlit as st
from data_diff import connect_to_table, diff_tables
import logging
from plotly.subplots import make_subplots
import plotly.graph_objects as go

logging.basicConfig(level=logging.DEBUG)
# import statement END


# setting page config- START
logging.basicConfig(level=logging.INFO)

st.set_page_config(layout="wide")
st.markdown(
    "<h1 style='text-align: center; color: #1489a6;'>❄❄ Snowflake Data Compare ❄❄</h1>",
    unsafe_allow_html=True)
current_tz = "'" + tzlocal.get_localzone_name() + "'"


# page configurtion end


# Creating Snowflake Connection Object START
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )


conn = init_connection()


# Loading Data from Snowflake END

@st.cache_data(ttl=600)
@st.cache_resource
def run_query_sf(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


SNOWFLAKE_CONN_INFO = {
    "driver": "snowflake",
    "user": st.secrets.snowflake.user,
    "account": st.secrets.snowflake.account,
    "database": "RAW",
    "warehouse": st.secrets.snowflake.warehouse,
    "role": st.secrets.snowflake.role,
    "schema": "JAFFLE_SHOP",
    "password": st.secrets.snowflake.password
}

# Markdown https://discuss.streamlit.io/t/alignment-of-content/29894/3

st.markdown(
    """
    <style>
        div[data-testid="column"]:nth-of-type(1)
        {
            border:1px solid blue;
            text-align: center;
        } 

        div[data-testid="column"]:nth-of-type(2)
        {
            border:1px solid blue;
            text-align: center;
        } 
        div[data-testid="column"]:nth-of-type(3)
        {
            border:1px solid blue;
            text-align: center;
        } 
    </style>
    """, unsafe_allow_html=True
)

# for the button color
m = st.markdown("""
<style>
div.stButton > button:first-child {
    background-color: #0099ff;
    color:#ffffff;
}
div.stButton > button:hover {
    background-color: #0088ff;
    color:#ffffff;
    }
</style>""", unsafe_allow_html=True)
# https://docs.streamlit.io/knowledge-base/using-streamlit/hide-row-indices-displaying-dataframe
# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """

st.markdown(hide_table_row_index, unsafe_allow_html=True)


# Markdown end
# load postgresql list of databases

def load_sf_db_list(count):
    # current_dt()

    db_list = run_query_sf(
        "SELECT DATABASE_NAME, date_trunc( 'second', CONVERT_TIMEZONE(" + current_tz +
        ", CREATED) ) as CREATED_TIME,DATABASE_OWNER, COMMENT FROM  SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY CREATED_TIME DESC;")

    db_list_df = pd.DataFrame(db_list, columns=['DATABASE_NAME', 'CREATED', 'DATABASE_OWNER', 'COMMENT'])
    db_name = st.selectbox('Please select the database that you would like to compare?', db_list_df, key=count + 1)

    schema_list = run_query_sf("SHOW TERSE SCHEMAS IN " + db_name + ";")
    schema_list_df = pd.DataFrame(schema_list, columns=['created_on', 'name', 'kind', 'database_name', 'SCHEMA_NAME'])
    schema_name = st.selectbox('Please select the schema that you would like to compare?', schema_list_df["name"],
                               key=count + 2)

    table_list = run_query_sf("SHOW TERSE TABLES IN SCHEMA " + db_name + "." + schema_name + ";")
    table_list_df = pd.DataFrame(table_list, columns=['created_on', 'name', 'kind', 'database_name', 'SCHEMA_NAME'])
    table_name = st.selectbox('Please select the table that you would like to compare?', table_list_df["name"],
                              key=count + 3)

    column_list = run_query_sf("SHOW COLUMNS IN " + db_name + "." + schema_name + "." + table_name + ";")
    column_list_df = pd.DataFrame(column_list,
                                  columns=['table_name', 'schema_name', 'column_name', 'data_type', 'null?',
                                           'default', 'kind', 'expression', 'comment', 'database_name',
                                           'autoincrement'])
    key_column_name = st.selectbox('Please select the unique key (primary key)?', column_list_df["column_name"],
                                   key=count + 4)

    full_qual_name = db_name + "." + schema_name + "." + table_name
    return full_qual_name, key_column_name, tuple(column_list_df["column_name"])


# common function to download as CSV
@st.cache_data
def convert_df_csv(df):
    return df.to_csv(index=False).encode('utf-8')


# Loading Data from Snowflake END


# Creating Snowflake Connection Object END
# TAB definition START
#tab1, tab2 = st.tabs(["📈 SINGLE TABLE", "🗃 ABOUT"])

count_in = 0

# TAB1
#with tab1:
try:
    col1, col2 = st.columns(2)
    with col1:
        col1.header("Source Table")
        count_in = 1

        full_qual_source_name, source_key_col, source_col_list = load_sf_db_list(count_in)
        st.write(full_qual_source_name)

    with col2:
        col2.header("Target Table")
        count_in = 100
        full_qual_target_name, target_key_col, target_col_list = load_sf_db_list(count_in)
        st.write(full_qual_target_name)

    if st.button('Show Table Diff', use_container_width=True):
        snowflake_table = connect_to_table(SNOWFLAKE_CONN_INFO, full_qual_source_name,
                                           source_key_col)  # Uses id by default
        snowflake_table2 = connect_to_table(SNOWFLAKE_CONN_INFO, full_qual_target_name,
                                            target_key_col)  # Uses id by default

        materialize_table_name = full_qual_target_name + "_DIFF"

        for different_row in diff_tables(snowflake_table, snowflake_table2, extra_columns=source_col_list,
                                         materialize_to_table=materialize_table_name):
            pass

        diff_op = pd.read_sql_query("SELECT *  FROM " + materialize_table_name + ";", conn)

        not_in_target = diff_op.loc[
            (diff_op['is_exclusive_a'].isin([True])) & (diff_op['is_exclusive_b'].isin([False]))]
        not_in_source = diff_op.loc[
            (diff_op['is_exclusive_a'].isin([False])) & (diff_op['is_exclusive_b'].isin([True]))]
        value_mismatch = diff_op.loc[
            (diff_op['is_exclusive_a'].isin([False])) & (diff_op['is_exclusive_b'].isin([False]))]

        # total_in_source = in_source
        # total_in_target = in_target
        total_not_in_target = len(not_in_target)
        total_not_in_source = len(not_in_source)
        total_value_mismatch = len(value_mismatch)



        fig_count = make_subplots(
            rows=1, cols=3,
            specs=[
                [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
            ],
            horizontal_spacing=0, vertical_spacing=0
        )

        fig_count.add_trace(
            go.Indicator(
                mode="number",
                value=total_not_in_target,
                title="<b>Missing in Target</b>",
                number={'font_color': 'red'},
            ),
            row=1, col=1
        )

        fig_count.add_trace(
            go.Indicator(
                mode="number",
                value=total_not_in_source,
                title="<b>Missing in Source</b>",
                number={'font_color': 'black'},
            ),
            row=1, col=2
        )

        fig_count.add_trace(
            go.Indicator(
                mode="number",
                value=total_value_mismatch,
                title="<b>Value Mismatch</b>",
                number={'font_color': 'orange'},
            ),
            row=1, col=3
        )
        fig_count.update_layout(font_family="Arial",
                                margin=dict(l=10, r=10, t=10, b=10), width=800, height=300)
        st.plotly_chart(fig_count, use_container_width=True)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader(":black[Missing in Target Table]", anchor=None)
            not_in_target_csv = convert_df_csv(not_in_target)
            st.download_button(
                "Click to Download",
                not_in_target_csv,
                "not_in_target.csv",
                "text/csv",
                key='download-csv-not_in_target'
            )
            st.write(not_in_target[source_key_col + "_a"])

        with col2:
            st.subheader(":black[Missing in Source Table]", anchor=None)
            not_in_source_csv = convert_df_csv(not_in_source)
            st.download_button(
                "Click to Download",
                not_in_source_csv,
                "not_in_source.csv",
                "text/csv",
                key='download-csv-not_in_source'
            )
            st.write(not_in_source[source_key_col + "_b"])

        with col3:
            st.subheader(":black[Value Mismatch]", anchor=None)
            value_mismatch_csv = convert_df_csv(value_mismatch)
            st.download_button(
                "Click to Download",
                value_mismatch_csv,
                "value_mismatch.csv",
                "text/csv",
                key='download-csv-value_mismatch'
            )
            st.write(value_mismatch[source_key_col + "_a"])



except Exception as er:
    st.error("Please select a valid (non-empty) schema/table combination")
    print(er)

#with tab2:
    st.markdown(
        """
        You can add more tabs to add more functionality
       """
    )
