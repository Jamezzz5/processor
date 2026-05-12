import os
import sys
import time
import json
import random
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
    default_config_file_name = 'steconfig.json'
    base_api_url = 'https://api.steampowered.com/'
    cur_players_url = (base_api_url +
                       'ISteamUserStats/GetNumberOfCurrentPlayers/v1/')
    achievements_url = (base_api_url +
                        'ISteamUserStats/'
                        'GetGlobalAchievementPercentagesForApp/v2/')
    owned_games_url = base_api_url + 'IPlayerService/GetOwnedGames/v1/'
    wishlist_url = base_api_url + 'IWishlistService/GetWishlist/v1/'
    base_store_url = 'https://store.steampowered.com/'
    reviews_url = base_store_url + 'appreviews/'
    app_det_url = base_store_url + 'api/appdetails/'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.key = None
        self.search_num = self.default_search_num
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
        self.key = self.config['key']
        search_num = self.config.get('search_num', self.default_search_num)
        try:
            self.search_num = int(search_num)
        except (TypeError, ValueError):
            logging.error('{} cannot be converted to an integer. '
                          'Aborting.'.format(search_num))
            sys.exit(0)
        self.config_list = [self.config, self.key]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Ste config file. '
                                'Aborting.'.format(item))
                sys.exit(0)

    def make_request(self, url, method='GET', params=None, body=None,
                     header=None, attempt=1):
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

    def raw_request(self, url, method='GET', params=None, body=None,
                    header=None):
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
        except json.decoder.JSONDecodeError:
            return False

    def request_random_user(self):
        random_int = random.randint(self.first_steam_id, self.last_steam_id)
        r = self.make_request(self.owned_games_url, params={
            'key': self.key, 'steamid': random_int, 'format': 'json'})
        return r, random_int

    def user_search_loop(self):
        rows = []
        number_hits = 0
        for x in range(self.search_num):
            if (x + 1) % 100 == 0:
                logging.info('Search number {} of {} Hits: {}'.format(
                    x + 1, self.search_num, number_hits))
            r, user_id = self.request_random_user()
            if not r:
                continue
            response = r.json().get('response') or {}
            games = response.get('games')
            if not games:
                continue
            number_hits += 1
            for game in games:
                game['steam_id'] = user_id
                rows.append(game)
        logging.info('Sampled {} users, {} owned games, '
                     '{} ownership rows.'.format(
                         self.search_num, number_hits, len(rows)))
        return pd.DataFrame(rows)

    def get_wishlists(self, steam_ids):
        logging.info('Getting wishlists for {} users.'.format(len(steam_ids)))
        rows = []
        for steam_id in steam_ids:
            r = self.make_request(self.wishlist_url, params={
                'key': self.key, 'steamid': steam_id})
            if not r:
                continue
            response = r.json().get('response') or {}
            items = response.get('items') or []
            for item in items:
                appid = item.get('appid')
                if appid is None:
                    continue
                rows.append({'steam_id': steam_id, 'appid': appid})
        return pd.DataFrame(rows)

    def get_current_players(self, app_ids):
        logging.info('Getting player counts for {} games.'.format(len(app_ids)))
        rows = []
        for app_id in app_ids:
            r = self.make_request(self.cur_players_url,
                                  params={'appid': app_id})
            if not r:
                continue
            data = r.json().get('response') or {}
            if data.get('result') == 1:
                rows.append({'appid': app_id,
                             'player_count': data['player_count']})
            else:
                logging.warning('Could not get player count for appid: '
                                '{}'.format(app_id))
        return pd.DataFrame(rows)

    def get_avg_achievement_pcts(self, app_ids):
        logging.info('Getting achievement percentages for {} games.'.format(
            len(app_ids)))
        rows = []
        for app_id in app_ids:
            r = self.make_request(self.achievements_url,
                                  params={'gameid': app_id})
            if not r:
                continue
            data = r.json()
            achievements = (data.get('achievementpercentages') or {}
                            ).get('achievements') or []
            if achievements:
                pcts = [float(a['percent']) for a in achievements]
                rows.append({'appid': app_id,
                             'avg_achievement_pct': sum(pcts) / len(pcts)})
        return pd.DataFrame(rows)

    def get_review_summaries(self, app_ids):
        logging.info('Getting review summaries for {} games.'.format(
            len(app_ids)))
        rows = []
        for app_id in app_ids:
            r = self.make_request(self.reviews_url + str(app_id),
                                  params={'json': 1, 'num_per_page': 0})
            if not r:
                continue
            data = r.json()
            if data.get('success') == 1:
                summary = dict(data['query_summary'])
                summary.pop('num_reviews', None)
                summary['appid'] = app_id
                rows.append(summary)
            else:
                logging.warning('Could not get review summary for appid: '
                                '{}'.format(app_id))
        return pd.DataFrame(rows)

    def get_app_details(self, app_ids):
        logging.info('Getting app details for {} games.'.format(len(app_ids)))
        max_retries = 22  # 22 * 15s = 330s (5 min rate limit + 30s)
        rows = []
        for app_id in app_ids:
            for attempt in range(max_retries):
                r = self.make_request(self.app_det_url, params={
                    'appids': app_id, 'cc': 'us', 'l': 'english'})
                if not r:
                    if attempt == max_retries - 1:
                        logging.error('Max retries exceeded for appid: '
                                      '{}'.format(app_id))
                        break
                    logging.warning('Empty response for appid: {}. Retrying '
                                    '{}/{} in 15s.'.format(
                                        app_id, attempt + 1, max_retries))
                    time.sleep(15)
                    continue
                data = r.json()
                if data and data[str(app_id)]['success']:
                    rows.append(data[str(app_id)]['data'])
                else:
                    logging.warning('Could not get details for appid: '
                                    '{}'.format(app_id))
                break
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df.rename(columns={'steam_appid': 'appid',
                                  'name': 'app_detail_name'})

    def get_data(self, sd=None, ed=None, fields=None):
        run_time = dt.datetime.now(dt.timezone.utc)
        user_games = self.user_search_loop()
        if user_games.empty:
            logging.warning('No games found from user sample.')
            self.df = user_games
            return user_games
        user_games['appid'] = user_games['appid'].astype('int64')
        user_games['steam_id'] = user_games['steam_id'].astype('int64')
        app_ids = user_games['appid'].unique().tolist()
        steam_ids = user_games['steam_id'].unique().tolist()
        df = user_games.groupby('appid', as_index=False).agg(
            owners_in_sample=('steam_id', 'nunique'))
        wishlists = self.get_wishlists(steam_ids)
        if not wishlists.empty:
            wishlists['appid'] = wishlists['appid'].astype('int64')
            wishlist_counts = wishlists.groupby('appid', as_index=False).agg(
                wishlists_in_sample=('steam_id', 'nunique'))
            df = df.merge(wishlist_counts, on='appid', how='left')
        if 'wishlists_in_sample' not in df.columns:
            df['wishlists_in_sample'] = 0
        df['wishlists_in_sample'] = (
            df['wishlists_in_sample'].fillna(0).astype('int64'))
        current_players = self.get_current_players(app_ids)
        if not current_players.empty:
            df = df.merge(current_players, on='appid', how='left')
        if 'player_count' not in df.columns:
            df['player_count'] = 0
        df['player_count'] = df['player_count'].fillna(0).astype('int64')
        avg_achievement_pcts = self.get_avg_achievement_pcts(app_ids)
        if not avg_achievement_pcts.empty:
            df = df.merge(avg_achievement_pcts, on='appid', how='left')
        review_summaries = self.get_review_summaries(app_ids)
        if not review_summaries.empty:
            df = df.merge(review_summaries, on='appid', how='left')
        app_details = self.get_app_details(app_ids)
        if not app_details.empty:
            df = df.merge(app_details, on='appid', how='left')
        df['gameeventdate'] = run_time.replace(tzinfo=None)
        df['gameeventname'] = (str(int(run_time.timestamp()))
                               + df['appid'].astype(str))
        self.df = df
        return df
