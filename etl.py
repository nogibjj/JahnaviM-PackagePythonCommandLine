'''Use ETL to Extract, Transform and Load Data on bad drivers to databricks'''

from mylib.extract import extract
from mylib.transform_load import trans_load
from mylib.query import create_table2, query_complex

# Extract
extract()

# Transform and Load
trans_load()

# Queries
create_table2()
query_complex()
