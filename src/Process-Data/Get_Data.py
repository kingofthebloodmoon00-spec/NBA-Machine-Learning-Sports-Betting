import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import toml

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.tools import get_json_data, to_data_frame

# ---------- SAFE CONFIG LOADING ----------
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
config_path = os.path.join(repo_root, "config.toml")

if not os.path.exists(config_path):
    raise FileNotFoundError(f"Cannot find config.toml at {config_path}")

config = toml.load(config_path)
# -----------------------------------------

url = config['data_url']

con = sqlite3.connect(os.path.join(repo_root, "Data/TeamData.sqlite"))

for key, value in config['get-data'].items():
    date_pointer = datetime.strptime(value['start_date'], "%Y-%m-%d").date()
    end_date = datetime.strptime(value['end_date'], "%Y-%m-%d").date()

    while date_pointer <= end_date:
        print("Getting data: ", date_pointer)

        raw_data = get_json_data(
            url.format(date_pointer.month, date_pointer.day, value['start_year'], date_pointer.year, key))
        df = to_data_frame(raw_data)

        date_pointer = date_pointer + timedelta(days=1)

        df['Date'] = str(date_pointer)

        df.to_sql(date_pointer.strftime("%Y-%m-%d"), con, if_exists="replace")

        time.sleep(random.randint(1, 3))

con.close()
