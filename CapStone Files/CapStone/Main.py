import requests
from bs4 import BeautifulSoup
import os
import gzip
import shutil

def main():
    #data_url = "https://www.isaacdwang.com/datasets/movies/"
    data_url = "https://datasets.imdbws.com/"
    download_path = "C:\\Users\\Boas\\Downloads\\movies_datasets"
    unzip_path = os.path.join(download_path, "un_zip data")

    download_data(data_url, download_path)
    unzip_data(download_path,unzip_path)


def download_data(url, download_path):
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags with a href value
        links = soup.find_all('a', href=True)

        # Create a folder to save the files
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        # Iterate through the links and find the ones that end with .tsv.gz
        for link in links:
            href = link['href']

            if href.endswith('.tsv.gz'):
                # Get the filename from the URL
                filename = os.path.join(download_path, href.split('/')[-1])
                if not os.path.exists(filename):
                    file_response = requests.get(href, stream=True)
                    with open(filename, "wb") as f:
                        f.write(file_response.raw.data)

def unzip_data(download_path,unzip_path):
    if not os.path.exists(unzip_path):
        os.makedirs(unzip_path)

    files = os.listdir(download_path)
    for file in files:
        if file.endswith('.tsv.gz'):
            zip_file = os.path.join(download_path,file)
            unzip_file = os.path.join(unzip_path, file)
            unzip_file = unzip_file.removesuffix('.gz')
            if not os.path.exists(unzip_file):
                print(zip_file, "->", unzip_file)
                with gzip.open(zip_file, 'rb') as f_in:
                    with open(unzip_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

if __name__ == "__main__":
    main()

