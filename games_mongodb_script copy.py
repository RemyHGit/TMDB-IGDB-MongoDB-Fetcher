import requests
import json
from pymongo import MongoClient
import os
import sys
import glob
import gzip
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Set the default encoding to UTF-8
sys.stdout.reconfigure(encoding="utf-8")

# Load environment variables from .env file
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
LOCAL_MONGO_URI = os.getenv("MONGO_URI")
TMDB_DB_NAME = os.getenv("TMDB_DB_NAME")
IMAGES_DIR = os.getenv("IMAGES_DIR")
TWITCH_ID = os.getenv("TWITCH_ID")
TWITCH_SECRET = os.getenv("TWITCH_SECRET")

import pandas as pd

response = requests.post(
    f"https://id.twitch.tv/oauth2/token?client_id={TWITCH_ID}&client_secret={TWITCH_SECRET}&grant_type=client_credentials"
)
access_token = response.json().get("access_token")
headers = {
    "Client-ID": TWITCH_ID,
    "Authorization": f"Bearer {access_token}",
}
def count_igdb_games():
    url = "https://api.igdb.com/v4/games/count"
    response = requests.post(url, headers=headers)
    count = response.json().get("count", 0)
    return count


print(count_igdb_games())