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

# Iterate seasons
for season_key, season_table in config['get-data'].items():
    # season_table might be nested if using dotted keys
    for subkey, value in season_table.items() if isinstance(season_table, dict) else [(season_key, season_table)]:
        # Auto-detect date format
        start_date_str = value['start_date']
        end_date_str = value['end_date']

        date_format = "%Y-%m-%d"
        if "/" in start_date_str:
            date_format = "%m/%d/%Y"

        date_pointer = datetime.strptime(start_date_str, date_format).date()
        end_date = datetime.strptime(end_date_str, date_format).date()

        while date_pointer <= end_date:
            print("Getting data:", date_pointer)

            raw_data = get_json_data(
                url.format(date_pointer.month, date_pointer.day, value['start_year'], date_pointer.year, season_key)
            )
            df = to_data_frame(raw_data)

            df['Date'] = str(date_pointer)
            df.to_sql(date_pointer.strftime("%Y-%m-%d"), con, if_exists="replace")

            date_pointer += timedelta(days=1)
            time.sleep(random.randint(1, 3))

con.close()
