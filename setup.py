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
        "duckdb",
        "termcolor",
        "tabulate",
        "click"
    ],
)
