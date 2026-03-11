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
    # first_steam_id = 76561197960265729
    # total_steam_users = (10 ** 9) * 4
    # last_steam_id = first_steam_id + total_steam_users
    # default_search_num = 10 ** 5
    config_path = utl.config_path
    default_config_file_name = 'steconfig.json'
    base_api_url = 'https://api.steampowered.com/'
    cur_players_url = (base_api_url +
                       'ISteamUserStats/GetNumberOfCurrentPlayers/v1/')
    achievements_url = (base_api_url +
                        'ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/')
    apps_url = base_api_url + 'IStoreService/GetAppList/v1'
    # owned_games_url = base_api_url + 'IPlayerService/GetOwnedGames/v1'
    # wishlist_url = base_store_url + 'IWishlistService/GetWishlist/v1/'
    base_store_url = 'https://store.steampowered.com/'
    reviews_url = base_store_url + 'appreviews/'
    app_det_url = base_store_url + 'api/appdetails/'

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

    def get_apps(self):
        logging.info('Getting all Steam apps.')
        all_apps = []
        last_appid = None
        have_more_results = True
        while have_more_results:
            params = {
                'key': self.key,
                'max_results': 500,  # default 10k, max val 50k
            }
            if last_appid:
                params['last_appid'] = last_appid
            r = self.make_request(self.apps_url, 'GET', params)
            data = r.json()['response']
            all_apps.extend(data['apps'])
            # have_more_results = data.get('have_more_results', False)
            have_more_results = False
            last_appid = data.get('last_appid')
        return pd.DataFrame(all_apps)

    def get_current_players(self, app_ids):
        logging.info('Getting player counts.')
        rows = []
        for app_id in app_ids:
            r = self.make_request(self.cur_players_url, 'GET',
                                 params={'appid': app_id})
            data = r.json()['response']
            if data['result'] == 1:
                rows.append({
                    'player_count': data['player_count'],
                    'appid': app_id})
            else:
                logging.warning('Could not get player count for appid: '
                                '{}'.format(app_id))
        return pd.DataFrame(rows)

    def get_app_details(self, app_ids):
        logging.info('Getting details for apps.')
        max_retries = 20  # 20 * 15s = 300s (5 min rate limit)
        rows = []
        for app_id in app_ids:
            for attempt in range(max_retries):
                r = self.make_request(self.app_det_url, 'GET', params={
                    'appids': app_id, 'cc': 'us', 'l': 'english'})
                data = r.json()
                if data and data[str(app_id)]['success']:
                    rows.append(data[str(app_id)]['data'])
                    break
                elif not data:
                    if attempt == max_retries:
                        logging.error('Max retries exceeded.  Aborting.')
                        sys.exit(0)
                    logging.warning('Empty response for appid: {}. Retrying '
                                    '{}/{} in 15s.'.format(app_id, attempt + 1,
                                                           max_retries))
                    time.sleep(15)
                    continue
                else:
                    logging.warning('Could not get details for appid: '
                                    '{}'.format(app_id))
                    break
        df = pd.DataFrame(rows)
        return df.rename(columns={'steam_appid': 'appid',
                                  'name': 'app_detail_name'})

    def get_avg_achievement_pcts(self, app_ids):
        logging.info('Getting achievement percentages.')
        rows = []
        for app_id in app_ids:
            r = self.make_request(self.achievements_url, 'GET',
                                  params={'gameid': app_id})
            data = r.json()
            if ('achievementpercentages' in data and
                    len(data['achievementpercentages']['achievements']) > 0):
                achievements = data['achievementpercentages']['achievements']
                pcts = [float(a['percent']) for a in achievements]
                avg_pct = sum(pcts) / len(pcts)
                rows.append({'appid': app_id, 'avg_achievement_pct': avg_pct})
            else:
                # logging.warning('No achievements found for appid: '
                #                 '{}'.format(app_id))
                pass
        return pd.DataFrame(rows)

    def get_review_summaries(self, app_ids):
        logging.info('Getting review summaries.')
        rows = []
        for app_id in app_ids:
            r = self.make_request(self.reviews_url + str(app_id), 'GET',
                                  params={'json': 1, 'num_per_page': 0})
            data = r.json()
            if data['success'] == 1:
                summary: dict = data['query_summary']
                summary.pop('num_reviews')  # always 0 with num_per_page=0
                summary['appid'] = app_id
                rows.append(summary)
            else:
                logging.warning('Could not get review summary for appid: '
                                '{}'.format(app_id))
        return pd.DataFrame(rows)

    def get_data(self, sd=None, ed=None, fields=None):
        today = dt.datetime.now(dt.timezone.utc).date()
        if self.apps:
            df = pd.DataFrame(list(self.apps.items()),
                              columns=['name', 'appid'])
        else:
            df = self.get_apps()
        app_ids = df['appid'].tolist()
        current_players = self.get_current_players(app_ids)
        df = df.merge(current_players, on='appid', how='left')
        app_details = self.get_app_details(app_ids)
        df = df.merge(app_details, on='appid', how='left')
        avg_achievement_pcts = self.get_avg_achievement_pcts(app_ids)
        df = df.merge(avg_achievement_pcts, on='appid', how='left')
        review_summaries = self.get_review_summaries(app_ids)
        df = df.merge(review_summaries, on='appid', how='left')
        # sampled_users = ...
        # wishlists/owned = ...(sampled_users, app_ids)
        # df = df.merge(wishlists/owned, on='appid', how='left')
        df['gameeventdate'] = today
        self.df = df
        return df

    # def set_last_steam_id(self):
    #     self.total_steam_users = self.get_total_users()
    #     self.last_steam_id = self.first_steam_id + self.total_steam_users
    #
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
