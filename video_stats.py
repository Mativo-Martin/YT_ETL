#!/usr/bin/env python3
import requests 
import json

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():

    try: 

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response
        data = response.json()

        # print(json.dumps(data, indent=4))


        channel_items = data['items']

        channel_playlistId = channel_items[0]['contentDetails']['relatedPlaylists']['uploads']

        # print(channel_playlistId)
        return channel_playlistId
    
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
    get_playlist_id()

