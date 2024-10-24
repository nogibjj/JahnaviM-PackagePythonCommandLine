from setuptools import setup, find_packages

setup(
    name="databricks_etl",
    version="0.0.1",
    description="ETL Query CLI",
    author="Jahnavi Maddhuri",
    author_email="jahnavi.maddhuri@duke.edu",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-dotenv",
        "databricks-sql-connector",
        "rust",
    ],
    entry_points={
        "console_scripts": [
            "etl_query=etl:",
        ],
    },
)