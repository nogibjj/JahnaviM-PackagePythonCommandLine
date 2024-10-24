'''Takes a csv and loads it as a db'''

import csv
import os
from dotenv import load_dotenv
from databricks import sql

PATH = 'data/bad-drivers.csv'

def trans_load(path = PATH):
    '''Takes the csv file and loads it onto databricks db'''

    # load data from csv
    payload = csv.reader(open(path, newline = ""), delimiter = ",")
    next(payload)
    load_dotenv()

    # connect to databricks
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                     http_path       = os.getenv("HTTP_PATH"),
                     access_token    = os.getenv("DATABRICKS_KEY")) as connection:

        with connection.cursor() as cursor:
            # Drop table if it already exists
            cursor.execute('''DROP TABLE IF EXISTS jm_baddrivers''')
            # Create main table
            cursor.execute('''CREATE TABLE jm_baddrivers 
                           (state STRING, drivers_count FLOAT, speeding_percent FLOAT, alc_percent FLOAT, 
                           no_distraction_percent FLOAT, no_prev_percent FLOAT, car_insurance FLOAT, 
                           insurance_losses FLOAT);''')
            insert_sql_str = "INSERT INTO jm_baddrivers VALUES"

            # Load data from csv into table
            payload_lst = list(payload)
            for index, ln in enumerate(payload_lst):
                if index == len(payload_lst) - 1:
                    end_char = ';'
                else:
                    end_char = ','
                insert_sql_str += '\n' + str(tuple(ln)) + end_char
            cursor.execute(insert_sql_str)

            # create output result of this created table to return for testing
            cursor.execute("SELECT * FROM jm_baddrivers")
            output = cursor.fetchall()
            
            cursor.close()
            connection.close()
    return output

if __name__ == "__main__":
    trans_load()
