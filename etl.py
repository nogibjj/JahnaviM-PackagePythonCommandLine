'''Use ETL to Extract, Transform and Load Data on bad drivers to databricks'''

import os
from dotenv import load_dotenv
from databricks import sql
from mylib.extract import extract
from mylib.transform_load import trans_load
from mylib.query import create_table2, query_complex

def interactive_query():
    cli_run = input('Would you like to run your own query? [y/n] ')
    if cli_run == 'y':
        query = input('Enter your Databricks SQL Query: ')
        load_dotenv()
        with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                        http_path       = os.getenv("HTTP_PATH"),
                        access_token    = os.getenv("DATABRICKS_KEY")) as connection:

            with connection.cursor() as cursor:
                try:
                    cursor.execute(query)
                    output = cursor.fetchall()

                    for row in output:
                        print('\t', row)
                    print('Custom Query Completed Running.')
                except:
                    print('No Query Results. Custom query must be valid according to schema and dialect constraints.')
    elif cli_run == 'n':
        print("No Custom Query Run.")
    else:
        print(cli_run, ' is not a valid response. To run your own query, rerun the script and respond with either y or n.')

if __name__ == "__main__":
    # Extract
    print('Extracting data from URL to a CSV file... ', end = '')
    extract()
    print('COMPLETE', '\n')

    # Transform and Load
    print('Transforming and Loading data to a Databricks DB... ', end = '')
    trans_load()
    print('COMPLETE', '\n')

    # Queries
    print('Creating an additional table... ', end = '')
    create_table2()
    print('COMPLETE', '\n')

    print('Running a Complex Query... ')
    query_complex()
    print('... COMPLETE', '\n')

    interactive_query()