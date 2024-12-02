
import time
from functions import functions

# Replace with the path to your WebDriver
# driver_path = '/users/keeganbruns/documents/app_extensions'
 
f = functions()
f.sao(f.yesterday, 'results')
f.sao(f.today, 'slate')

#-----------------------------------------------------------------------------
"""
# Manual load
from datetime import datetime as dt
from functions import functions

f = functions()

date_string = "2024-11-17"

# Convert to datetime.date object
date_object = dt.strptime(date_string, "%Y-%m-%d").date()
f.sao(date_object, 'results')
"""


#-----------------------------------------------------------------------------
# Results discovery
"""
from google.cloud import bigquery
from google.cloud import secretmanager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime as dt, timedelta
import pytz
import os
import time
import re


driver = webdriver.Chrome()
url = 'https://www.scoresandodds.com/ncaab?date=2024-11-27'

time.sleep(2)
# Load the webpage
driver.get(url)

table = driver.find_elements(By.TAG_NAME, 'table')
table = table[1:]

columns=['game_info','score','spread','total','moneyline']
games = pd.DataFrame(columns=columns)

for event in table:
    game = pd.read_html(StringIO(str(event.get_attribute('outerHTML'))))[0]
    game = game.drop(['Line Movements','Notes'],axis=1)
    game.columns = columns
    games = pd.concat([games, game], ignore_index=True)
    time.sleep(1)

# Wait for the page to load
driver.quit()
games_copy = games.copy(deep=True)


games = games_copy.copy(deep=True)

games['date'] = '2024-11-27'

def _team(game_info):
    game_info = game_info.split(' ')
    record = game_info[-1]
    game_number = game_info[1]
    team = ' '.join(game_info[1:-1])
    team = re.sub(r'[0-9()]', '', team)
    team = team.rstrip()

    return team, record

games[['team','record']] = games.game_info.apply(_team).apply(pd.Series)
games[['win','loss']] = games.record.str.split('-',expand=True)
games[['spread','spread_odds']] = games.spread.str.split(' ',expand=True)
games[['total','total_odds']] = games.total.str.split(' ',expand=True)

# Split out the o/u from the total
games['total_over_under'] = games['total'].apply(lambda x: str(x)[0])
games['total_over_under'] = games.total_over_under.replace('o','over')
games['total_over_under'] = games.total_over_under.replace('u','under')

# Set the total to be just the numerical value
games['total'] = games['total'].apply(lambda x: str(x)[1:])

# Drop bs games that dont mean anything and dont have odds                
games = games[games['spread'] != 'NaN'].reset_index(drop=True)
games = games[games['total'] != 'an'].reset_index(drop=True)

games.head()

# Assign games ID
num_rows = len(games)
values = np.arange(1, num_rows // 2 + 2).repeat(2)[:num_rows]
games['game_id'] = values

dtype_dic = {'win': int, 
            'loss': int,
            'score': int,
            'spread': float,
            'total': float,
            'moneyline': int,
            'spread_odds': int,
            'total_odds': int}

games = games.astype(dtype_dic)

load_df = games[['date','game_id','team','win','loss','score',
                'moneyline','spread','spread_odds','total',
                'total_over_under','total_odds']]\
                .reset_index(drop=True)
"""
# # Drop games without a spread
# games['spread'] = games['spread'].astype(str)
# games = games[games['spread'] != 'nan'].reset_index(drop=True)

# # Seperate out fields
# games[['team','record']] = games.game_info.apply(_team).apply(pd.Series)
# games[['win','loss']] = games.record.str.split('-',expand=True)
# games[['spread','spread_odds','drop']] = games.spread.str.split(' ',expand=True)
# games[['total','total_odds','drop']] = games.total.str.split(' ',expand=True)
# games.drop(columns='drop', inplace=True)

# # Split out the o/u from the total
# games['total_over_under'] = games['total'].apply(lambda x: str(x)[0])
# games['total_over_under'] = games.total_over_under.replace('o','over')
# games['total_over_under'] = games.total_over_under.replace('u','under')

# # Set the total to be just the numerical value
# games['total'] = games['total'].apply(lambda x: str(x)[1:])
# games['spread_odds'] = games['spread_odds'].replace('even','+100')
# games[['moneyline','drop']] = games.moneyline.str.split(' ',expand=True)
# games.drop(columns='drop', inplace=True)

# # Assign games ID
# num_rows = len(games)
# values = np.arange(1, num_rows // 2 + 2).repeat(2)[:num_rows]
# games['game_id'] = values

# dtype_dic = {'win': int, 
#              'loss': int,
#              'spread': float,
#              'total': float,
#              'moneyline': float,
#              'spread_odds': int,
#              'total_odds': int}

# games = games.astype(dtype_dic)

# load_df = games[['date','game_id','team','win','loss','moneyline',
#                  'spread','spread_odds','total','total_over_under',
#                  'total_odds']].reset_index(drop=True)

# self._load_to_bq('results','raw_scores_and_odds','partition',
#                     load_df)
    
