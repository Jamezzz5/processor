import os
import sys
import time
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl


class SteApi(object):
    first_steam_id = 76561197960265729
    total_steam_users = (10 ** 9) * 4
    last_steam_id = first_steam_id + total_steam_users
    default_search_num = 10 ** 5
    config_path = utl.config_path

    base_api_url = 'https://api.steampowered.com'
    ccu_url = base_api_url + '/ISteamUserStats/GetNumberOfCurrentPlayers/v1'
    achievement_url = (base_api_url +
               '/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v0002')
    owned_games_url = base_api_url + 'IPlayerService/GetOwnedGames/v0001'
    apps_url = base_api_url + '/IStoreService/GetAppList/v1'
    player_sum_url = base_api_url + '/ISteamUser/GetPlayerSummaries/v2'

    base_store_url = 'https://store.steampowered.com'
    reviews_url = base_store_url + '/appreviews'
    app_det_url = base_store_url + '/api/appdetails'
    wishlist_url = base_store_url + '/wishlist/profiles'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.apps = None
        self.key = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Ste config file: {}'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.apps = self.config['apps']
        self.key = self.config['key']
        self.config_list = [self.config, self.key]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Ste config file.'
                                'Aborting.'.format(item))
                sys.exit(0)
        if not self.apps:
            logging.warning('No configured apps. Fetching all Steam apps.')

    # def set_last_steam_id(self):
    #     self.total_steam_users = self.get_total_users()
    #     self.last_steam_id = self.first_steam_id + self.total_steam_users

    def make_request(self, url, method, params=None, body=None, header=None, attempt=1):
        try:
            response = self.raw_request(url, method, params=params, body=body,
                                        header=header)
        except requests.exceptions.SSLError as e:
            if attempt > 10:
                logging.warning('Could not connect with error: {}'.format(e))
                response = None
            else:
                logging.warning('SSLError, retrying: {}'.format(e))
                time.sleep(30)
                attempt += 1
                response = self.make_request(url, method, params=params,
                                             body=body, header=header,
                                             attempt=attempt)
        return response

    def raw_request(self, url, method, params=None, body=None, header=None):
        try:
            if method == 'POST':
                r = requests.post(url, json=body, headers=header)
            else:
                r = requests.get(url, params=params, headers=header)
        except requests.urllib3.exceptions.ProtocolError as e:
            logging.warning('ProtocolError - retrying: {}'.format(e))
            time.sleep(60)
            r = self.raw_request(url, method, params, body, header)
        try:
            r.json()
            return r
        except json.decoder.JSONDecodeError as e:
            return False

    # @staticmethod
    # def get_numbers_from_list(number_list):
    #     return sum([((10 ** y[0]) * y[1]) for y in number_list])
    #
    # def get_total_users(self, last_number=12, number_list=None):
    #     logging.info('Getting total users.')
    #     if not number_list:
    #         number_list = []
    #     for exponent in range(last_number, 1, -1):
    #         for multi in range(9, 1, -1):
    #             user_id = (self.first_steam_id + ((10 ** exponent) * multi) +
    #                        self.get_numbers_from_list(number_list))
    #             if self.request_owned_games(user_id, error=False):
    #                 logging.info('10^{} * {} was a user. '
    #                              'Number list {}'.format(exponent, multi,
    #                                                      number_list))
    #                 number_list.append((exponent, multi))
    #                 break
    #     return self.get_numbers_from_list(number_list)
    #
    # def request_owned_games(self, user_id, error=True):
    #     params = {
    #         # 'key': self.key,
    #         'steamid': user_id, 'format': 'json'}
    #     r = self.raw_request(self.owned_games_url, 'GET', params=params)
    #     if r is False:
    #         return False
    #     try:
    #
    # def user_search_loop(self, search_num=None):
    #     if not search_num:
    #         search_num = self.default_search_num
    #     number_hits = 0
    #     df = pd.DataFrame()
    #     for x in range(search_num):
    #         logging.info('Search number {} of {} Hits: {}'
    #                      .format(x, search_num, number_hits))
    #         df, number_hits = self.get_random_user_df(df, number_hits)
    #     return df
    #
    # def get_random_user_df(self, df, number_hits=0):
    #     r, user_id = self.request_random_user()
    #     df, number_hits = self.get_df_from_response(df, r, user_id, number_hits)
    #     return df, number_hits
    #
    # def request_random_user(self):
    #     random_int = random.randint(self.first_steam_id, self.last_steam_id)
    #     logging.info('Searching user {}'.format(random_int))
    #     r = self.make_request(random_int, sleep_length=120)
    #     return r, random_int
    #
    # def request_random_user_wishlist(self):
    #     random_int = random.randint(self.first_steam_id, self.last_steam_id)
    #     logging.info('Searching user {}'.format(random_int))
    #     wish_url = ('{}{}/wishlistdata/?p=0'.format(
    #         self.wishlist_url, random_int))
    #     r = requests.get(wish_url)
    #     return r
    #
    # @staticmethod
    # def get_df_from_response(df, r, user_id, number_hits=0):
    #     if r and ('response' in r.json() and r.json()['response'] and
    #               'games' in r.json()['response']):
    #         number_hits += 1
    #         tdf = pd.DataFrame(r.json()['response']['games'])
    #         tdf['steam_id'] = user_id
    #         df = df.append(tdf, ignore_index=True)
    #     return df, number_hits

    def get_game_dict(self):
        logging.info('Getting Steam app list.')
        r = self.make_request(self.apps_url, 'GET',
                              params={'key': self.key})
        try:
            apps = r.json()['response']['apps']
            return pd.DataFrame(apps)
        except Exception as e:
            logging.warning('Could not get app list: {}'.format(e))
            return pd.DataFrame()

    # def get_data_write_df(self, search_num=None):
    #     self.set_last_steam_id()
    #     today_date = dt.datetime.today().date()
    #     file_name = 'steam_users_{}.csv'.format(today_date.strftime('%Y%m%d'))
    #     df = self.user_search_loop(search_num=search_num)
    #     self.write_df(df, file_name)
    #     app_list = df['appid'].unique().tolist()
    #     current_players = self.get_current_players(app_list)
    #     df = df.append(current_players, ignore_index=True, sort=True)
    #     game_dict = self.get_game_dict()
    #     df = df.merge(game_dict, on='appid', how='left')
    #     steam_ids = df[df['steam_id'].notnull()]['steam_id'].unique().tolist()
    #     player_stats = self.get_player_stats(steam_ids)
    #     df = df.merge(player_stats, on='steam_id', how='left')
    #     app_details = self.get_app_details(app_list)
    #     df = df.merge(app_details, on='appid', how='left')
    #     df['gameeventdate'] = today_date
    #     utl.dir_check('data')
    #     self.write_df(df, file_name)
    #
    # @staticmethod
    # def write_df(df, file_name):
    #     logging.info('Writing df to csv: {}'.format(file_name))
    #     df.to_csv(os.path.join('data', file_name), index=False)
    #     df.to_csv('steam_users.csv', index=False)
    #     logging.info('Finished writing df to csv')

    def get_player_count(self, app_id):
        r = self.make_request(self.ccu_url, 'GET', params={'appid': app_id})
        try:
            if 'player_count' not in r.json()['response']:
                return 0
            return r.json()['response']['player_count']
        except Exception as e:
            logging.warning('Could not get player count for appid {}: '
                            '{}'.format(app_id, e))
            return None

    def get_achievements(self, app_id):
        r = self.make_request(self.achievement_url, 'GET',
                              params={'gameid': app_id})
        try:
            if ('achievementpercentages' not in r.json() or
                len(r.json()['achievementpercentages']['achievements']) == 0):
                return {
                    'achievement_pct': 0,
                    'achievement_count': 0,
                }
            achievements = r.json()['achievementpercentages']['achievements']
            pcts = [float(a['percent']) for a in achievements]
            return {
                'achievement_pct': sum(pcts) / len(pcts),
                'achievement_count': len(pcts),
            }
        except Exception as e:
            logging.warning('Could not get achievements for appid {}: '
                            '{}'.format(app_id, e))
            return None

    def get_review_summary(self, app_id):
        url = '{}/{}'.format(self.reviews_url, app_id)
        r = self.make_request(url, 'GET', params={'json': 1, 'num_per_page': 0})
        try:
            qs = r.json()['query_summary']
            del qs['num_reviews']  # always 0 with num_per_page=0
            return qs
        except Exception as e:
            logging.warning('Could not get review summary for appid {}: '
                            '{}'.format(app_id, e))
            return {}

    def get_app_details(self, app_id):
        logging.info('Getting app details for appid: {}'.format(app_id))
        r = self.make_request(self.app_det_url, 'GET',
                              params={'appids': app_id})
        if not r.json()[str(app_id)]['success']:
            return {}
        return r.json()[str(app_id)]['data']

    # def get_player_stats(self, steam_ids):
    #     df = pd.DataFrame()
    #     steam_ids = [steam_ids[x:x + 100]
    #                  for x in range(0, len(steam_ids), 100)]
    #     for steam_id in [x for x in steam_ids if x]:
    #         logging.info('Getting player stats for id: {}'.format(steam_id))
    #         r = self.raw_request(
    #             self.player_sum_url, sleep_length=60, params={
    #                 'key': self.key,
    #                 'steamids': ','.join([str(int(x)) for x in steam_id])})
    #         tdf = pd.DataFrame(r.json()['response']['players'])
    #         df = df.append(tdf, ignore_index=True)
    #     df = df.rename(columns={'steamid': 'steam_id'})
    #     logging.info(df)
    #     df['steam_id'] = df['steam_id'].astype('int64')
    #     return df

    def get_data(self, sd=None, ed=None, fields=None):
        today = dt.datetime.now(dt.timezone.utc).date()

        if self.apps:
            game_dict = self.apps
        else:
            game_df = self.get_game_dict()
            game_dict = dict(game_df['appid'].astype('int64'), game_df['name'])

        app_ids = []
        names = []
        player_counts = []
        achievements = []
        review_summaries = []
        app_details = []
        for app_id, name in game_dict.items():
            logging.info('Getting Steam metrics for appid: {}'.format(app_id))
            app_ids.append(app_id)
            names.append(name)
            player_counts.append(self.get_player_count(app_id))
            achievements.append(self.get_achievements(app_id))
            review_summaries.append(self.get_review_summary(app_id))
            app_details.append(self.get_app_details(app_id))

        df = pd.DataFrame({
            'appid': app_ids,
            'productname': names,
            'date': today,
            'player_count': player_counts,
        })

        app_details_df = pd.DataFrame(app_details).drop(columns=['steam_appid', 'name'])

        df = pd.concat([df, pd.DataFrame(achievements),
                        pd.DataFrame(review_summaries),
                        app_details_df
                       ], axis=1)

        self.df = df
        return self.df