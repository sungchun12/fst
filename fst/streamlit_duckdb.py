import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import threading
import queue
import time

# Connect to DuckDB database
con = duckdb.connect('jaffle_shop.duckdb')


def get_data():
    query = "SELECT * FROM new_file;"
    return con.execute(query).fetchdf()

def watch_for_new_data(q):
    last_known_row_count = 0

    while True:
        result = con.execute("SELECT COUNT(*) FROM new_file;").fetchone()

        if result is None:
            print("Error: fetchone() returned None")
            time.sleep(1)
            continue

        current_row_count = result[0]

        if current_row_count > last_known_row_count:
            q.put("New data detected")
            last_known_row_count = current_row_count

        time.sleep(1)


    # Query data from DuckDB
    df = get_data()

refresh_interval = 5  # Refresh every 5 seconds

# Streamlit app
st.set_page_config(layout="wide")

while True:
    # Query data from DuckDB
    df = get_data()

    # Create a line chart using Plotly
    fig = px.line(df, x='most_recent_order', y='customer_lifetime_value', title='Beautiful Line Chart')

    # Display the line chart in Streamlit
    st.plotly_chart(fig)

    time.sleep(refresh_interval)
    st.experimental_rerun()