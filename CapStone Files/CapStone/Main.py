import requests
from bs4 import BeautifulSoup
import os
import gzip
import shutil
import pandas as pd


def download_data(depth, url, download_path):
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
                if not os.path.exists(filename) and not None:
                    file_response = requests.get(href, stream=True)
                    with open(filename, "wb") as f:
                        f.write(file_response.raw.data)
                    log_message(filename, depth)

def unzip_data(depth, download_path,unzip_path):
    if not os.path.exists(unzip_path):
        os.makedirs(unzip_path)

    files = os.listdir(download_path)
    for file in files:
        if file.endswith('.tsv.gz'):
            zip_file = os.path.join(download_path,file)
            unzip_file = os.path.join(unzip_path, file)
            unzip_file = unzip_file.removesuffix('.gz')
            if not os.path.exists(unzip_file):
                log_message(str(zip_file + "->" + unzip_file), depth)
                with gzip.open(zip_file, 'rb') as f_in:
                    with open(unzip_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

def log_file(file_name, process_function, *args):
    print("************starting file: " + file_name + " ************")
    process_function(1, *args)
    print("************ending file: " + file_name + " ************")

def log_message(message, depth=1):
    print("\t" * depth + message)

def read_csv_to_panda(file_location):
    return pd.read_csv(file_location, delimiter='\t')

def explode_data(df, field):
    df[field] = df[field].str.split(',')
    return df.explode(field).reset_index(drop=True)

def main():

    data_url = "https://www.isaacdwang.com/datasets/movies/"
    #data_url = "https://datasets.imdbws.com/"
    download_path = "C:\\Users\\Boas\\Downloads\\movies_datasets"
    unzip_path = os.path.join(download_path, "un_zip data")

    log_file( "Downloading", download_data, data_url, download_path)
    log_file( "Unzip File", unzip_data, download_path,unzip_path)

    df = read_csv_to_panda("C:\\Users\\Boas\\Downloads\\movies_datasets\\un_zip data\\test.tsv")
    #df = explode_data(df,  "directors")
    df = explode_data(df, "writers")
    print(df)


if __name__ == "__main__":
    main()

