from setuptools import setup, find_packages

setup(
    name="fst",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "fst = fst.main:main",
        ],
    },
    install_requires=[
        "watchdog",
        "psutil",
        "dbt-core",
        "pyyaml",
        "pygments",
        "colorlog",
        "duckdb==0.7.1",
        "termcolor",
        "tabulate",
        "click",
        "streamlit",
        "plotly",
        "streamlit-ace==0.1.1",
        "streamlit-diff-viewer==0.0.2",
        "marvin==0.6.1"
    ],
)
