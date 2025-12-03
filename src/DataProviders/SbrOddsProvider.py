import requests

class SbrOddsProvider:
    """Fetch NBA odds directly from SBR API without using sbrscrape."""

    def __init__(self, sportsbook="fanduel"):
        self.sportsbook = sportsbook
        self.games, self.api_data = self._fetch_sbr_games()

    def _fetch_sbr_games(self):
        """Fetch today’s NBA games and odds from SBR API."""
        url = "https://www.sportsbookreview.com/api/betting-odds/event/scoreboard/nba/?market=spread"
        response = requests.get(url)
        data = response.json()

        games = []
        api_data = []

        for event in data.get('events', []):
            home = event['home_team']['name']
            away = event['away_team']['name']

            # Fix for LA Clippers naming to match original code
            if home == "Los Angeles Clippers":
                home = "LA Clippers"
            if away == "Los Angeles Clippers":
                away = "LA Clippers"

            games.append({'home_team': home, 'away_team': away})
            api_data.append(event)

        return games, api_data

    def get_odds(self):
        """Return odds in the same dictionary format as the old implementation."""
        dict_res = {}
        for game_event in self.api_data:
            home = game_event['home_team']['name']
            away = game_event['away_team']['name']

            # Fix for LA Clippers naming
            if home == "Los Angeles Clippers":
                home = "LA Clippers"
            if away == "Los Angeles Clippers":
                away = "LA Clippers"

            money_line_home_value = money_line_away_value = totals_value = None

            # Get money line bet values from the sportsbook
            if 'home_ml' in game_event and self.sportsbook in game_event['home_ml']:
                money_line_home_value = game_event['home_ml'][self.sportsbook]
            if 'away_ml' in game_event and self.sportsbook in game_event['away_ml']:
                money_line_away_value = game_event['away_ml'][self.sportsbook]

            # Get totals bet value
            if 'total' in game_event and self.sportsbook in game_event['total']:
                totals_value = game_event['total'][self.sportsbook]

            dict_res[f"{home}:{away}"] = {
                'under_over_odds': totals_value,
                home: {'money_line_odds': money_line_home_value},
                away: {'money_line_odds': money_line_away_value}
            }

        return dict_res
