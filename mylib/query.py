'''Functions to read dataset'''

import os
from databricks import sql
from dotenv import load_dotenv

def create_table2():
    '''Creates a record in bad drivers data based for a made up state - NewState'''
    load_dotenv()
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                     http_path       = os.getenv("HTTP_PATH"),
                     access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            # if table exists, drop it
            cursor.execute('''DROP TABLE IF EXISTS jm_baddrivers_speed;''')
            # Create main table
            cursor.execute('''CREATE TABLE jm_baddrivers_speed 
                           AS SELECT state, drivers_count*speeding_percent/100 as speed_ct FROM jm_baddrivers;''')
            
            # create output from fetching new table to return for testing
            cursor.execute('''SELECT * FROM jm_baddrivers_speed''')
            output = cursor.fetchall()
            
            cursor.close()
            connection.close()
    return output

def query_complex():
    load_dotenv()
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                     http_path       = os.getenv("HTTP_PATH"),
                     access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            # Combine both tables
            cursor.execute('''SELECT round(df.drivers_count) as rounded_driv_ct, COUNT(df.state) as num_states, AVG(df_sp.speed_ct) as avg_ct_speed
                           FROM jm_baddrivers df
                           LEFT JOIN jm_baddrivers_speed df_sp ON df.state = df_sp.state
                           GROUP BY round(df.drivers_count)
                           ORDER BY round(df.drivers_count);''')
            output = cursor.fetchall()
            for row in output:
                print(row)
            cursor.close()
            connection.close()
    return output
    

if __name__ == "__main__":
    create_table2()
    query_complex()