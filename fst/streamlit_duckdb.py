import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# Connect to DuckDB database
con = duckdb.connect("jaffle_shop.duckdb")

# Example DuckDB table creation and data insertion
con.execute(
    """
CREATE TABLE IF NOT EXISTS sample_data (
    date DATE,
    value FLOAT
);
"""
)

con.execute(
    """
INSERT INTO sample_data (date, value) VALUES
    ('2023-01-01', 10),
    ('2023-01-02', 15),
    ('2023-01-03', 20),
    ('2023-01-04', 18),
    ('2023-01-05', 22);
"""
)

# Query data from DuckDB
query = "SELECT * FROM sample_data;"
df = con.execute(query).fetchdf()

# Create a line chart using Plotly
fig = px.line(df, x="date", y="value", title="Beautiful Line Chart")

# Display the line chart in Streamlit
st.plotly_chart(fig)
