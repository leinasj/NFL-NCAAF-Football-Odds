import requests
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date
from datetime import timedelta
from dateutil.parser import parse
from dateutil import tz

# Load environment variables such as API key stored in .env file
load_dotenv()
# API call using Rapid API/The Odds API, payload includes over/under lines and spread
def make_request(sport):
    
    odds_response = requests.get(
        f'https://odds.p.rapidapi.com/v4/sports/{sport}/odds',
        params={
            'regions': 'us',
            'markets': 'totals,spreads',
            'oddsFormat': 'american',
            'dateFormat': 'iso'
        },
        headers = {
        'X-RapidAPI-Key': os.environ.get("ODDS_API"),
        'X-RapidAPI-Host': 'odds.p.rapidapi.com'
        }
    )
    # Error handling for HTTP errors
    if odds_response.status_code != 200:
        print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')

    else:
        odds_json = odds_response.json()
    
    return make_table(odds_json)
# Flatten json response into pandas DataFrame
def make_table(response)-> pd.DataFrame:
    
    x = pd.json_normalize(response, record_path=['bookmakers', 'markets', 'outcomes'], meta = ['sport_title', 'commence_time', 'home_team', 'away_team', ['bookmakers', 'title'], ['bookmakers', 'markets', 'key'], ['bookmakers', 'markets', 'last_update']]).convert_dtypes(infer_objects=True)
    # Filter for only DraftKings odds
    x = x.loc[x['bookmakers.title']=='DraftKings']
    x.rename(columns = {'commence_time':'Time_of_Kickoff', 'name':'Line'}, inplace = True)
    # Convert time columns to UTC 
    x['Time_of_Kickoff'] = x['Time_of_Kickoff'].apply(func = lambda x: convert_timezone(x))
    x['bookmakers.markets.last_update'] = x['bookmakers.markets.last_update'].apply(func = lambda x: convert_timezone(x))
    x = x.loc[(x['Line']!='Under')]
    dates = x.loc[x['Line']=='Over']['Time_of_Kickoff'].unique()
    dates = date_range(dates)
    x = x.loc[x['Time_of_Kickoff'].isin(dates)]
    x.replace(to_replace='Over',value='O/U',inplace=True)
    return x
# Convert ISO-8601 to UTC and local time zone 
def convert_timezone(x):
    
    utc = parse(x)
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/Seattle')
    utc = utc.replace(tzinfo=from_zone)
    utc = utc.astimezone(to_zone)
    return utc

def date_range(dates):
    new_dates = []
    for d in dates:
        if d.date() < (date.today() + timedelta(weeks=1)):
            new_dates.append(d)
    return new_dates

def main():
    
    ncaa_odds_json = make_request('americanfootball_ncaaf')
    nfl_odds_json = make_request('americanfootball_nfl')
    ncaa_odds_json.to_csv("ncaa_odds_" + date.isoformat(date.today()) + ".csv", index = False)
    nfl_odds_json.to_csv("nfl_odds_" + date.isoformat(date.today()) + ".csv", index = False)
    
        
if __name__ == "__main__":
    main()