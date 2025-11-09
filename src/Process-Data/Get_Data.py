import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import toml

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.tools import get_json_data, to_data_frame

# Load config
config = toml.load("../../config.toml")
url = config['data_url']

con = sqlite3.connect("../../Data/TeamData.sqlite")

# Loop through all get-data sections
for key, value in config.items():
    if key.startswith("get-data."):
        # Parse dates in MM/DD/YYYY format (matches your TOML)
        date_pointer = datetime.strptime(value['start_date'], "%m/%d/%Y").date()
        end_date = datetime.strptime(value['end_date'], "%m/%d/%Y").date()

        while date_pointer <= end_date:
            print("Getting data:", date_pointer)

            raw_data = get_json_data(
                url.format(date_pointer.month, date_pointer.day, value['start_year'], date_pointer.year, key))
            df = to_data_frame(raw_data)

            df['Date'] = str(date_pointer)
            df.to_sql(date_pointer.strftime("%Y-%m-%d"), con, if_exists="replace")

            date_pointer += timedelta(days=1)
            time.sleep(random.randint(1, 3))

con.close()
