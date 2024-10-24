'''This script is to test the etl.py script'''

import os
from mylib.query import create_table2, query_complex
from mylib.extract import extract
from mylib.transform_load import trans_load

def test_extract():
    '''tests if extract generates os path to bad-drivers dataset in directory'''
    extract()
    assert os.path.exists('data/bad-drivers.csv')

def test_trans_load():
    '''tests if trans_load generates all 51 rows for all states as expected'''
    output = trans_load()
    assert len(output) == 51

def test_create_table2():
    '''tests if create_table generates a new table with 51 rows'''
    output = create_table2()
    assert len(output) == 51

def test_query_complex():
    '''tests if query_complex generates an aggregate table with 16 rows'''
    output = query_complex()
    assert len(output) == 16 

if __name__ == "__main__":
    test_extract()
    test_trans_load()
    test_create_table2()
    test_query_complex()