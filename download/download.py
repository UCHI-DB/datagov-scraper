import requests
from requests import exceptions
import ssl

def download_file_to(url, filename, path):
    '''
        Downloads the file from the URL and
        stores it in the path provided
    '''
    r = None
    try:
        r = requests.get(url, stream=True)
    except: 
        print("ERROR while trying to download: " + str(url))
        return
    target_path = path + filename
    with open(target_path, 'wb+') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

if __name__ == "__main__":
    print("todo")
    filename = "test.csv"
    url = "https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD"
    path = "."
    download_file_to(url, filename, path)
    print("DONE!")
