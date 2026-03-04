import os
import time
import json
import random
import logging
import requests
import pandas as pd
import datetime as dt
import steam.utils as utl


class SteamApi(object):
    first_steam_id = 76561197960265729
    total_steam_users = (10 ** 9) * 4
    last_steam_id = first_steam_id + total_steam_users
    default_search_num = 10 ** 5
    base_url = 'http://api.steampowered.com/'
    owned_games_url = base_url + 'IPlayerService/GetOwnedGames/v0001/'
    apps_url = base_url + 'ISteamApps/GetAppList/v0001/'
    cur_players_url = base_url + 'ISteamUserStats/GetNumberOfCurrentPlayers/v1/'
    wishlist_url = base_url + 'wishlist/profiles/'
    player_sum_url = base_url + 'ISteamUser/GetPlayerSummaries/v2/'
    app_det_url = 'http://store.steampowered.com/api/appdetails/'

    def __init__(self, config_file=os.path.join('cfg', 'conf.json')):
        self.config_file = config_file
        self.config = utl.load_config_file(self.config_file)
        self.key = self.config['key']

    def set_last_steam_id(self):
        self.total_steam_users = self.get_total_users()
        self.last_steam_id = self.first_steam_id + self.total_steam_users

    def make_request(self, user_id, sleep_length=0, error=True):
        url = '{}?key={}&steamid={}&format=json'.format(
            self.owned_games_url, self.key, user_id)
        r = self.raw_request(url, sleep_length, error)
        return r

    def raw_request(self, url, sleep_length=0, error=True, params=None):
        if params:
            try:
                r = requests.get(url, params=params)
            except requests.urllib3.exceptions.ProtocolError as e:
                logging.warning('Error - retrying: {}'.format(e))
                time.sleep(60)
                r = self.raw_request(url, sleep_length, error, params)
        else:
            try:
                r = requests.get(url)
            except requests.urllib3.exceptions.ProtocolError as e:
                logging.warning('Error - retrying: {}'.format(e))
                time.sleep(60)
                r = self.raw_request(url, sleep_length, error, params)
        try:
            r.json()
            return r
        except json.decoder.JSONDecodeError as e:
            if error:
                logging.warning('Response not json.  Error: {}\n {}'.format(
                    e, r.text))
            time.sleep(sleep_length)
            return False

    @staticmethod
    def get_numbers_from_list(number_list):
        return sum([((10 ** y[0]) * y[1]) for y in number_list])

    def get_total_users(self, last_number=12, number_list=None):
        logging.info('Getting total users.')
        if not number_list:
            number_list = []
        for exponent in range(last_number, 1, -1):
            for multi in range(9, 1, -1):
                user_id = (self.first_steam_id + ((10 ** exponent) * multi) +
                           self.get_numbers_from_list(number_list))
                if self.make_request(user_id, error=False):
                    logging.info('10 to the power of {} times {} was a user. '
                                 'Number list {}'.format(exponent, multi,
                                                         number_list))
                    number_list.append((exponent, multi))
                    break
        total_users = self.get_numbers_from_list(number_list)
        return total_users

    def user_search_loop(self, search_num=None):
        if not search_num:
            search_num = self.default_search_num
        number_hits = 0
        df = pd.DataFrame()
        for x in range(search_num):
            logging.info('Search number {} of {} Hits: {}'
                         .format(x, search_num, number_hits))
            df, number_hits = self.get_random_user_df(df, number_hits)
        return df

    def get_random_user_df(self, df, number_hits=0):
        r, user_id = self.request_random_user()
        df, number_hits = self.get_df_from_response(df, r, user_id, number_hits)
        return df, number_hits

    def request_random_user(self):
        random_int = random.randint(self.first_steam_id, self.last_steam_id)
        logging.info('Searching user {}'.format(random_int))
        r = self.make_request(random_int, sleep_length=120)
        return r, random_int

    def request_random_user_wishlist(self):
        random_int = random.randint(self.first_steam_id, self.last_steam_id)
        logging.info('Searching user {}'.format(random_int))
        wish_url = ('{}{}/wishlistdata/?p=0'.format(
            self.wishlist_url, random_int))
        r = requests.get(wish_url)
        return r

    @staticmethod
    def get_df_from_response(df, r, user_id, number_hits=0):
        if r and ('response' in r.json() and r.json()['response'] and
                  'games' in r.json()['response']):
            number_hits += 1
            tdf = pd.DataFrame(r.json()['response']['games'])
            tdf['steam_id'] = user_id
            df = df.append(tdf, ignore_index=True)
        return df, number_hits

    def get_game_dict(self):
        r = requests.get(self.apps_url)
        game_dict = r.json()['applist']['apps']['app']
        game_dict = pd.DataFrame(game_dict)
        return game_dict

    def get_data_write_df(self, search_num=None):
        self.set_last_steam_id()
        today_date = dt.datetime.today().date()
        file_name = 'steam_users_{}.csv'.format(today_date.strftime('%Y%m%d'))
        df = self.user_search_loop(search_num=search_num)
        self.write_df(df, file_name)
        app_list = df['appid'].unique().tolist()
        current_players = self.get_current_players(app_list)
        df = df.append(current_players, ignore_index=True, sort=True)
        game_dict = self.get_game_dict()
        df = df.merge(game_dict, on='appid', how='left')
        steam_ids = df[df['steam_id'].notnull()]['steam_id'].unique().tolist()
        player_stats = self.get_player_stats(steam_ids)
        df = df.merge(player_stats, on='steam_id', how='left')
        app_details = self.get_app_details(app_list)
        df = df.merge(app_details, on='appid', how='left')
        df['gameeventdate'] = today_date
        utl.dir_check('data')
        self.write_df(df, file_name)

    @staticmethod
    def write_df(df, file_name):
        logging.info('Writing df to csv: {}'.format(file_name))
        df.to_csv(os.path.join('data', file_name), index=False)
        df.to_csv('steam_users.csv', index=False)
        logging.info('Finished writing df to csv')

    def get_current_players(self, game_ids):
        df = pd.DataFrame()
        for game_id in game_ids:
            logging.info('Getting current_players for id: {}'.format(game_id))
            r = self.raw_request(self.cur_players_url, sleep_length=60,
                                 params={'appid': game_id})
            if r and 'player_count' in r.json()['response']:
                tdf = pd.DataFrame([
                    {'player_count': r.json()['response']['player_count'],
                     'appid': game_id}])
                df = df.append(tdf, ignore_index=True, sort=True)
            else:
                logging.warning('Could not get player count for id: '
                                '{} \n '.format(game_id))
                if r is False:
                    game_ids.append(game_id)
                else:
                    logging.warning('Response: {}'.format(r.json()))
        df['appid'] = df['appid'].astype('int64')
        return df

    def get_app_details(self, game_ids):
        df = pd.DataFrame()
        for game_id in game_ids:
            logging.info('Getting app details for id: {}'.format(game_id))
            r = self.raw_request(self.app_det_url, sleep_length=60,
                                 params={'appids': game_id})
            if r and r.json()[str(game_id)]['success']:
                tdf = pd.DataFrame([r.json()[str(game_id)]['data']])
                df = df.append(tdf, ignore_index=True, sort=True)
            else:
                logging.warning('Could not get details for id: '
                                '{} \n '.format(game_id))
                if r is False:
                    game_ids.append(game_id)
                else:
                    logging.warning('Response: {}'.format(r.json()))
        df = df.rename(columns={'steam_appid': 'appid'})
        df = df.rename(columns={'name': 'app_detail_name'})
        df['appid'] = df['appid'].astype('int64')
        return df

    def get_player_stats(self, steam_ids):
        df = pd.DataFrame()
        steam_ids = [steam_ids[x:x + 100]
                     for x in range(0, len(steam_ids), 100)]
        for steam_id in [x for x in steam_ids if x]:
            logging.info('Getting player stats for id: {}'.format(steam_id))
            r = self.raw_request(
                self.player_sum_url, sleep_length=60, params={
                    'key': self.key,
                    'steamids': ','.join([str(int(x)) for x in steam_id])})
            tdf = pd.DataFrame(r.json()['response']['players'])
            df = df.append(tdf, ignore_index=True)
        df = df.rename(columns={'steamid': 'steam_id'})
        logging.info(df)
        df['steam_id'] = df['steam_id'].astype('int64')
        return df