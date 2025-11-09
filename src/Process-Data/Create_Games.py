import os
import sqlite3
import sys

import numpy as np
import pandas as pd
import toml

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.Dictionaries import team_index_07, team_index_08, team_index_12, team_index_13, team_index_14, team_index_current

# ---------- SAFE CONFIG LOADING ----------
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
config_path = os.path.join(repo_root, "config.toml")

if not os.path.exists(config_path):
    raise FileNotFoundError(f"Cannot find config.toml at {config_path}")

config = toml.load(config_path)
# -----------------------------------------

df = pd.DataFrame
scores = []
win_margin = []
OU = []
OU_Cover = []
games = []
days_rest_away = []
days_rest_home = []

teams_con = sqlite3.connect(os.path.join(repo_root, "Data/TeamData.sqlite"))
odds_con = sqlite3.connect(os.path.join(repo_root, "Data/OddsData.sqlite"))

for season_key, season_table in config['create-games'].items():
    for subkey, value in season_table.items() if isinstance(season_table, dict) else [(season_key, season_table)]:
        print("Processing season:", season_key)

        odds_df = pd.read_sql_query(f"SELECT * FROM \"odds_{season_key}_new\"", odds_con, index_col="index")
        season = season_key

        for row in odds_df.itertuples():
            home_team = row[2]
            away_team = row[3]
            date = row[1]

            team_df = pd.read_sql_query(f"SELECT * FROM \"{date}\"", teams_con, index_col="index")
            if len(team_df.index) != 30:
                continue

            scores.append(row[8])
            OU.append(row[4])
            days_rest_home.append(row[10])
            days_rest_away.append(row[11])
            win_margin.append(1 if row[9] > 0 else 0)
            OU_Cover.append(0 if row[8] < row[4] else 1 if row[8] > row[4] else 2)

            # pick team index based on season
            if season == '2007-08':
                home_team_series = team_df.iloc[team_index_07.get(home_team)]
                away_team_series = team_df.iloc[team_index_07.get(away_team)]
            elif season in ["2008-09", "2009-10", "2010-11", "2011-12"]:
                home_team_series = team_df.iloc[team_index_08.get(home_team)]
                away_team_series = team_df.iloc[team_index_08.get(away_team)]
            elif season == "2012-13":
                home_team_series = team_df.iloc[team_index_12.get(home_team)]
                away_team_series = team_df.iloc[team_index_12.get(away_team)]
            elif season == '2013-14':
                home_team_series = team_df.iloc[team_index_13.get(home_team)]
                away_team_series = team_df.iloc[team_index_13.get(away_team)]
            elif season in ['2022-23', '2023-24']:
                home_team_series = team_df.iloc[team_index_current.get(home_team)]
                away_team_series = team_df.iloc[team_index_current.get(away_team)]
            else:
                try:
                    home_team_series = team_df.iloc[team_index_14.get(home_team)]
                    away_team_series = team_df.iloc[team_index_14.get(away_team)]
                except Exception as e:
                    print("Team causing error:", home_team)
                    raise e

            game = pd.concat([
                home_team_series,
                away_team_series.rename(index={col: f"{col}.1" for col in team_df.columns.values})
            ])
            games.append(game)

odds_con.close()
teams_con.close()

season_df = pd.concat(games, ignore_index=True, axis=1).T
frame = season_df.drop(columns=['TEAM_ID', 'TEAM_ID.1'])
frame['Score'] = np.asarray(scores)
frame['Home-Team-Win'] = np.asarray(win_margin)
frame['OU'] = np.asarray(OU)
frame['OU-Cover'] = np.asarray(OU_Cover)
frame['Days-Rest-Home'] = np.asarray(days_rest_home)
frame['Days-Rest-Away'] = np.asarray(days_rest_away)

# fix types
for field in frame.columns.values:
    if 'TEAM_' in field or 'Date' in field or field not in frame:
        continue
    frame[field] = frame[field].astype(float)

con = sqlite3.connect(os.path.join(repo_root, "Data/dataset.sqlite"))
frame.to_sql("dataset_2012-24_new", con, if_exists="replace")
con.close()
