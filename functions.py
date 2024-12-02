"""
Created on: 12/23/21
Author: Keegan Bruns

Purpose: One place to store different classes of functions
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

# # Set verb
# os.environ['GRPC_VERBOSITY'] = 'ERROR'

class functions:
    """
    This class stores any functions or general parameters that are used across
    multiple subsets of this project
    """

    def __init__(self):
        """
        Initalize variables
        """
        if os.getenv('GCP_PROJECT') is None:
            import pydata_google_auth 
            self.project_id = 'ball-dont-lie-420'
            
            self.SCOPES = [
                            'https://www.googleapis.com/auth/cloud-platform'
                         ]
            self.gcp_user_creds = pydata_google_auth.get_user_credentials(
                                    self.SCOPES, 
                                    auth_local_webserver=True)
            
            self.bq_client  = bigquery.Client(project = self.project_id,
                                          credentials = self.gcp_user_creds)                                     
        
        else:
            self.project_id = os.getenv('GCP_PROJECT') 
            self.bq_client = bigquery.Client(project = self.project_id)
                                                     
        self.yesterday = dt.now(pytz.timezone('America/New_York')).date() - \
            timedelta(days=1)
        self.today = dt.now(pytz.timezone('America/New_York')).date()

    def _team(self, game_info):
        game_info = game_info.split(' ')
        record = game_info[-1]
        game_number = game_info[1]
        team = ' '.join(game_info[1:-1])
        team = re.sub(r'[0-9()]', '', team)
        team = team.rstrip()

        return team, record
    
    def sao(self, date, table_id:str):
        """
        """
        driver = webdriver.Chrome()
        url = f'https://www.scoresandodds.com/ncaab?date={str(date)}'

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
            time.sleep(.75)

        # Wait for the page to load
        driver.quit()

        games['date'] = date
        
        if table_id == 'results':
            games[['team','record']] = games.game_info.apply(self._team).apply(pd.Series)
            games[['win','loss']] = games.record.str.split('-',expand=True)
            games[['spread','spread_odds']] = games.spread.str.split(' ',expand=True)
            games[['total','total_odds']] = games.total.str.split(' ',expand=True)

            # Split out the o/u from the total
            games['total_over_under'] = games['total'].apply(lambda x: str(x)[0])
            games['total_over_under'] = games.total_over_under.replace('o','over')
            games['total_over_under'] = games.total_over_under.replace('u','under')

            # Set the total to be just the numerical value
            games['total'] = games['total'].apply(lambda x: str(x)[1:])
            games['spread_odds'] = games['spread_odds'].replace('even','+100')
            games['moneyline'] = games['moneyline'].replace('even','+100')
            games['total_odds'] = games['total_odds'].replace('even','+100')

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

        else:            
            # Drop games without a spread
            games['spread'] = games['spread'].astype(str)
            games = games[games['spread'] != 'nan'].reset_index(drop=True)

            # Seperate out fields
            games[['team','record']] = games.game_info.apply(self._team).apply(pd.Series)
            games[['win','loss']] = games.record.str.split('-',expand=True)
            games[['spread','spread_odds','drop']] = games.spread.str.split(' ',expand=True)
            games[['total','total_odds','drop']] = games.total.str.split(' ',expand=True)
            games.drop(columns='drop', inplace=True)

            # Split out the o/u from the total
            games['total_over_under'] = games['total'].apply(lambda x: str(x)[0])
            games['total_over_under'] = games.total_over_under.replace('o','over')
            games['total_over_under'] = games.total_over_under.replace('u','under')

            # Set the total to be just the numerical value
            games['total'] = games['total'].apply(lambda x: str(x)[1:])
            games['spread_odds'] = games['spread_odds'].replace('even','+100')
            games['total_odds'] = games['total_odds'].replace('even','+100')
            games[['moneyline','drop']] = games.moneyline.str.split(' ',expand=True)
            games['moneyline'] = games['moneyline'].replace('even','+100')
            games.drop(columns='drop', inplace=True)

            # Assign games ID
            num_rows = len(games)
            values = np.arange(1, num_rows // 2 + 2).repeat(2)[:num_rows]
            games['game_id'] = values

            dtype_dic = {'win': int, 
                        'loss': int,
                        'spread': float,
                        'total': float,
                        'moneyline': float,
                        'spread_odds': int,
                        'total_odds': int}

            games = games.astype(dtype_dic)

            load_df = games[['date','game_id','team','win','loss','moneyline',
                             'spread','spread_odds','total','total_over_under',
                             'total_odds']].reset_index(drop=True)


        self._load_to_bq(table_id, 'raw_scores_and_odds','partition',load_df,
                        str(date))

        return

    def _load_to_bq(self, table_id, dataset_id, write_disp, load_df, date):
        """
        Load dataframe to bigquery.

        Write disp: append, truncate
        """
        if table_id == 'results': 
            schema = [
                    bigquery.SchemaField('date','DATE'),
                    bigquery.SchemaField('game_id','INTEGER'),
                    bigquery.SchemaField('team','STRING'),
                    bigquery.SchemaField('win','INTEGER'),
                    bigquery.SchemaField('loss','INTEGER'),
                    bigquery.SchemaField('moneyline','INTEGER'),
                    bigquery.SchemaField('spread','FLOAT64'),
                    bigquery.SchemaField('spread_odds','INTEGER'),
                    bigquery.SchemaField('total','FLOAT64'),
                    bigquery.SchemaField('total_over_under','STRING'),
                    bigquery.SchemaField('total_odds','INTEGER')                           
                    ] 

        elif table_id == 'slate':
            schema = [
                    bigquery.SchemaField('date','DATE'),
                    bigquery.SchemaField('game_id','INTEGER'),
                    bigquery.SchemaField('team','STRING'),
                    bigquery.SchemaField('win','INTEGER'),
                    bigquery.SchemaField('loss','INTEGER'),
                    bigquery.SchemaField('moneyline','INTEGER'),
                    bigquery.SchemaField('spread','FLOAT64'),
                    bigquery.SchemaField('spread_odds','INTEGER'),
                    bigquery.SchemaField('total','FLOAT64'),
                    bigquery.SchemaField('total_over_under','STRING'),
                    bigquery.SchemaField('total_odds','INTEGER')                           
                    ]

        # Load to GCP
        table_ref = self.bq_client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        if write_disp == 'partition':
            job_config.write_disposition = bigquery.WriteDisposition\
                                                                 .WRITE_APPEND
            job_config.time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field='date')

        elif write_disp == 'truncate':
            job_config.write_disposition = bigquery.WriteDisposition\
                                                                .WRITE_TRUNCATE
        
        job = self.bq_client.load_table_from_dataframe(load_df, table_ref, 
                                                    job_config = job_config)
        
        print(f'{date} {table_id} loaded to {dataset_id}')
            
 