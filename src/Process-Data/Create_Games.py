import os
import sqlite3
import sys

import numpy as np
import pandas as pd
import toml

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.Dictionaries import team_index_07, team_index_08, team_index_12, team_index_13, team_index_14, team_index_current

config = toml.load("../../config.toml")

df = pd.DataFrame
scores = []
win_margin = []
OU = []
OU_Cover = []
games = []
days_rest_away = []
days_rest_home = []

teams_con = sqlite3.connect("../../Data/TeamData.sqlite")
odds_con = sqlite3.connect("../../Data/OddsData.sqlite")

# Loop through all create-games sections
for key, value in config.items():
    if key.startswith("create-games."):
        print("Processing season:", key)
        odds_df = pd.read_sql_query(f'SELECT * FROM "odds_{key}_new"', odds_con, index_col="index")
        season = key

        for row in odds_df.itertuples():
            home_team = row[2]
            away_team = row[3]
            date = row[1]

            team_df = pd.read_sql_query(f'SELECT * FROM "{date}"', teams_con, index_col="index")
            if len(team_df.index) == 30:
                scores.append(row[8])
                OU.append(row[4])
                days_rest_home.append(row[10])
                days_rest_away.append(row[11])
                win_margin.append(1 if row[9] > 0 else 0)
                OU_Cover.append(0 if row[8] < row[4] else 1 if row[8] > row[4] else 2)

                # Select correct team index mapping
                if season == '2007-08':
                    home_team_series = team_df.iloc[team_index_07.get(home_team)]
                    away_team_series = team_df.iloc[team_index_07.get(away_team)]
                elif season in ['2008-09', '2009-10', '2010-11', '2011-12']:
                    home_team_series = team_df.iloc[team_index_08.get(home_team)]
                    away_team_series = team_df.iloc[team_index_08.get(away_team)]
                elif season == "2012-13":
                    home_team_series = team_df.iloc[team_index_12.get(home_team)]
                    away_team_series = team_df.iloc[team_index_12.get(away_team)]
                elif season == '2013-14':
                    home_team_series = team_df.iloc[team_index_13.get(home_team)]
                    away_team_series = team_df.iloc[team_index_13.get(away_team)]
                elif season in ['2022-23', '2023-24', '2024-25', '2025-26']:
                    home_team_series = team_df.iloc[team_index_current.get(home_team)]
                    away_team_series = team_df.iloc[team_index_current.get(away_team)]
                else:
                    home_team_series = team_df.iloc[team_index_14.get(home_team)]
                    away_team_series = team_df.iloc[team_index_14.get(away_team)]

                game = pd.concat([home_team_series, away_team_series.rename(
                    index={col: f"{col}.1" for col in team_df.columns.values}
                )])
                games.append(game)

odds_con.close()
teams_con.close()

# Combine all games into one DataFrame
season_df = pd.concat(games, ignore_index=True, axis=1).T
season_df = season_df.drop(columns=['TEAM_ID', 'TEAM_ID.1'], errors='ignore')
season_df['Score'] = np.array(scores)
season_df['Home-Team-Win'] = np.array(win_margin)
season_df['OU'] = np.array(OU)
season_df['OU-Cover'] = np.array(OU_Cover)
season_df['Days-Rest-Home'] = np.array(days_rest_home)
season_df['Days-Rest-Away'] = np.array(days_rest_away)

# Convert numeric columns
for col in season_df.columns:
    if col not in ['Date'] and 'TEAM_' not in col:
        season_df[col] = season_df[col].astype(float)

con = sqlite3.connect("../../Data/dataset.sqlite")
season_df.to_sql("dataset_2012-26_new", con, if_exists="replace")
con.close()
print("✅ Dataset created successfully!")
