import requests
from bs4 import BeautifulSoup
import os
import gzip
import shutil
import pandas as pd


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
                if not os.path.exists(filename) and not None:
                    file_response = requests.get(href, stream=True)
                    with open(filename, "wb") as f:
                        f.write(file_response.raw.data)
                    log_message(filename)


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
                log_message(str(zip_file + "->" + unzip_file))
                with gzip.open(zip_file, 'rb') as f_in:
                    with open(unzip_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)


def log_file(file_name, process_function, *args):
    print("************starting file: " + file_name + " ************")
    process_function(*args)
    print("************ending file: " + file_name + " ************")


def log_message(message):
    print("\t" + message)


def read_csv_to_panda(file_location):
    return pd.read_csv(file_location, delimiter='\t')


def explode_data(df, field):
    df[field] = df[field].str.split(',')
    return df.explode(field).reset_index(drop=True)


def export_data_frame(df, file_name, des_location):
    file_location = os.path.join(des_location, file_name)
    df.to_csv(file_location, index=False)


def left_join_df(df1, df2, field):
    joined_table = pd.merge(df1, df2, on=field, how='left')
    return joined_table


def filter_episodes(df1, df2, field1, field2):
    df3 = df1[~df1[field1].isin(df2[field2])]
    return df3


def drop_row_based_on_value(df, column, value):
    filtered_df = df[df[column] != value]
    return filtered_df


def process_name_basics(download_location, download_file_name, export_location, export_file_name):
    file_location = os.path.join(download_location, download_file_name)
    df = read_csv_to_panda (file_location)
    df = explode_data(df,'primaryProfession')
    df = explode_data(df, 'knownForTitles')
    export_data_frame(df, export_file_name, export_location)


def process_title_akas(download_location, download_file_name, export_location, export_file_name):
    file_location = os.path.join(download_location, download_file_name)
    df = read_csv_to_panda(file_location)
    df = explode_data(df, 'types')
    export_data_frame(df, export_file_name, export_location)


def process_title_crew(download_location, download_file_name, export_location, export_file_name):
    file_location = os.path.join(download_location, download_file_name)
    df = read_csv_to_panda(file_location)
    df = explode_data(df, 'directors')
    df = explode_data(df, 'writers')
    export_data_frame(df, export_file_name, export_location)


def process_basic_and_episode(download_location_basic, download_file_name_basic, export_location_basic, export_file_name_basic, download_location_espisode, download_file_name_espisode, export_location_espisode, export_file_name_espisode):
    basic_file_location = os.path.join(download_location_basic, download_file_name_basic)
    episode_file_location = os.path.join(download_location_espisode, download_file_name_espisode)
    df_basic = read_csv_to_panda(basic_file_location)
    df_episode = read_csv_to_panda(episode_file_location)
    df_episode = left_join_df(df_episode,df_basic,'tconst')
    df_basic = drop_row_based_on_value(df_basic, 'isAdult', 1)
    df_basic = filter_episodes(df_basic, df_episode, 'tconst', 'tconst')
    export_data_frame(df_episode, export_file_name_espisode, export_location_espisode)
    export_data_frame(df_basic, export_file_name_basic, export_location_basic)


def process_simple(download_location, download_file_name, export_location, export_file_name):
    file_location = os.path.join(download_location, download_file_name)
    df = read_csv_to_panda(file_location)
    export_data_frame(df, export_file_name, export_location)

def main():

    data_url = "https://www.isaacdwang.com/datasets/movies/"
    #data_url = "https://datasets.imdbws.com/"
    download_path = "C:\\Users\\Boas\\Downloads\\movies_datasets"
    unzip_path = os.path.join(download_path, "un_zip data")
    output_path =  "C:\\Users\\Boas\\Downloads\\"

    log_file( "Downloading", download_data, data_url, download_path)
    log_file( "Unzip File", unzip_data, download_path,unzip_path)

    temp_file_name = 'name.basics.tsv'
    process_name_basics(unzip_path,temp_file_name, output_path, temp_file_name)

    temp_file_name = 'title.akas.tsv'
    process_title_akas(unzip_path,temp_file_name, output_path, temp_file_name)

    temp_file_name = 'title.principals.tsv'
    process_simple(unzip_path, temp_file_name, output_path, temp_file_name)

    temp_file_name = 'title.ratings.tsv'
    process_simple(unzip_path, temp_file_name, output_path, temp_file_name)

    temp_file_name = 'title.crew.tsv'
    process_title_crew(unzip_path, temp_file_name, output_path, temp_file_name)

    temp_file_name = 'title.episode.tsv'
    temp_file_name2 = 'title.basics.tsv'
    process_basic_and_episode(unzip_path, temp_file_name2, output_path, temp_file_name2, unzip_path, temp_file_name, output_path, temp_file_name)


if __name__ == "__main__":
    main()

