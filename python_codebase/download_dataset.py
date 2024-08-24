import requests
import os
from datetime import datetime, timedelta

# Define the base URL for Wikimedia pageviews
BASE_URL = "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:02d}/"

# Function to download files
def download_pageviews(year, month, day, hours, download_dir="../dataset"):
    os.makedirs(download_dir, exist_ok=True)

    for hour in hours:
        filename = f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
        url = f"{BASE_URL.format(year=year, month=month)}{filename}"
        
        print(f"Downloading {filename}...")
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            filepath = os.path.join(download_dir, filename)
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded {filename} to {filepath}")
        else:
            print(f"Failed to download {filename}, status code: {response.status_code}")

# Download 24 hours of data for August 1, 2024
year = 2024
month = 8
day = 1
hours = range(24)  # Download data from 00:00 to 23:00

download_pageviews(year, month, day, hours)