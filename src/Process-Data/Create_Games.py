import os
import sqlite3
import sys

import numpy as np
import pandas as pd
import toml

# Add project root to path
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.Dictionaries import team_index_current

# Load config
config = toml.load("../../config.toml")

# Prepare lists for final dataset
scores = []
win_margin = []
OU = []
OU_Cover = []
games = []
days_rest_away = []
days_rest_home = []

# Connect to databases
teams_con = sqlite3.connect("../../Data/TeamData.sqlite")
odds_con = sqlite3.connect("../../Data/OddsData.sqlite")

# Ensure odds tables exist
odds_cursor = odds_con.cursor()
for year in ["2023-24", "2024-25", "2025-26"]:
    table_name = f"odds_{year}_new"
    try:
        odds_cursor.execute(f"SELECT 1 FROM '{table_name}' LIMIT 1")
    except sqlite3.OperationalError:
        # If table doesn't exist, create as copy of first table in DB
        first_table = odds_cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1"
        ).fetchone()[0]
        print(f"Creating missing table {table_name} from {first_table}")
        odds_cursor.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM "{first_table}"')
        odds_con.commit()

# Loop through each season
for key, value in config['create-games'].items():
    if key not in ["2023-24", "2024-25", "2025-26"]:
        continue  # skip other seasons

    print(f"Processing season {key} ...")
    odds_df = pd.read_sql_query(f'SELECT * FROM "odds_{key}_new"', odds_con, index_col="index")

    for row in odds_df.itertuples():
        date = row[1]
        home_team = row[2]
        away_team = row[3]

        team_df = pd.read_sql_query(f'SELECT * FROM "{date}"', teams_con, index_col="index")
        if len(team_df.index) != 30:
            continue

        scores.append(row[8])
        OU.append(row[4])
        days_rest_home.append(row[10])
        days_rest_away.append(row[11])
        win_margin.append(1 if row[9] > 0 else 0)

        if row[8] < row[4]:
            OU_Cover.append(0)
        elif row[8] > row[4]:
            OU_Cover.append(1)
        else:
            OU_Cover.append(2)

        # Use current team index mapping for all 3 seasons
        home_team_series = team_df.iloc[team_index_current.get(home_team)]
        away_team_series = team_df.iloc[team_index_current.get(away_team)]

        game = pd.concat([home_team_series, away_team_series.rename(
            index={col: f"{col}.1" for col in team_df.columns.values}
        )])
        games.append(game)

# Close connections
odds_con.close()
teams_con.close()

# Create final DataFrame
season_df = pd.concat(games, ignore_index=True, axis=1).T
season_df = season_df.drop(columns=['TEAM_ID', 'TEAM_ID.1'], errors='ignore')
season_df['Score'] = np.asarray(scores)
season_df['Home-Team-Win'] = np.asarray(win_margin)
season_df['OU'] = np.asarray(OU)
season_df['OU-Cover'] = np.asarray(OU_Cover)
season_df['Days-Rest-Home'] = np.asarray(days_rest_home)
season_df['Days-Rest-Away'] = np.asarray(days_rest_away)

# Fix numeric types
for field in season_df.columns.values:
    if 'TEAM_' in field or 'Date' in field:
        continue
    season_df[field] = season_df[field].astype(float)

# Save to dataset
con = sqlite3.connect("../../Data/dataset.sqlite")
season_df.to_sql("dataset_recent_seasons_new", con, if_exists="replace")
con.close()

print("✅ Dataset for 2023-24, 2024-25, 2025-26 created successfully!")
