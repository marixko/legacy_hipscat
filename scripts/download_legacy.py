import os
import requests
from bs4 import BeautifulSoup
import argparse
from concurrent.futures import ThreadPoolExecutor

def download_from_url(base_url, save_to, num_threads, log_file, auth):
    response = requests.get(base_url, auth=auth)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Ensure the save_to directory exists
    if not os.path.exists(save_to):
        os.makedirs(save_to)

    tasks = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for link in soup.find_all('a'):
            href = link.get('href')
            if href == '../':  # it's the parent directory link
                continue
            if href.endswith('/'):  # it's a directory
                # Recursively fetch the directory contents
                tasks.append(executor.submit(download_from_url, base_url + href, os.path.join(save_to, href[:-1]), num_threads, log_file, auth))
            else:  # it's a file
                tasks.append(executor.submit(download_file, base_url + href, os.path.join(save_to, href), log_file, auth))

        # Wait for all tasks to complete
        for task in tasks:
            task.result()

def download_file(url, save_path, log_file, auth):
    try:
        print(f"Downloading {url}")
        with requests.get(url, stream=True, auth=auth) as response:
            response.raise_for_status()
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
    except Exception as e:
        if log_file:
            with open(log_file, 'a') as log:
                log.write(f"Failed to fetch {url}. Error: {str(e)}\n")

def main():
    parser = argparse.ArgumentParser(description='Download all files and directories from an Nginx autoindexed folder URL.')
    parser.add_argument('url', type=str, help='URL of the Nginx autoindexed folder.')
    parser.add_argument('save_directory', type=str, help='Local directory to save the downloaded files.')
    parser.add_argument('--threads', type=int, default=5, help='Number of threads to use for concurrent downloads. Default is 5.')
    parser.add_argument('--log', type=str, default=None, help='Path to the log file where failed downloads will be recorded.')
    parser.add_argument('--user', type=str, help='Username for basic authentication.')
    parser.add_argument('--password', type=str, help='Password for basic authentication.')
    
    print("-----------------------------------")
    print("Download files from Nginx autoindexed folder")

    args = parser.parse_args()
    auth = (args.user, args.password) if args.user and args.password else None
    download_from_url(args.url, args.save_directory, args.threads, args.log, auth)
    
    print("-----------------------------------")

if __name__ == '__main__':
    main()

