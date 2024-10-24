'''Extracts a dataset'''
import requests

URL = 'https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/bad-drivers/bad-drivers.csv'
FILE_PATH = 'data/bad-drivers.csv'

def extract(url = URL, path = FILE_PATH):
    '''Extracts a dataset based on URL and saves it to the path'''
    with requests.get(url) as r:
        with open(path, 'wb') as f:
            f.write(r.content)
    return path

if __name__ == "__main__":
    extract()