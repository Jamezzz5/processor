import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc

config_path = utl.config_path


class TikApi(object):
    version = 'v1.3'
    base_url = 'https://business-api.tiktok.com/open_api/'
    ad_url = '/ad/get/'
    smart_url = '/smart_plus/ad/get/'
    campaign_url = '/campaign/get/'
    advertiser_url = '/oauth2/advertiser/get/'
    ad_report_url = '/report/integrated/get/'
    new_date = 'stat_time_day'
    old_date = 'stat_datetime'
    dimensions = ['stat_time_day', 'ad_id']
    metrics = {'clicks': 'click_cnt',
               'cost_per_conversion': 'conversion_cost',
               'conversion_rate': 'conversion_rate',
               'conversion': 'convert_cnt',
               'ctr': 'ctr',
               'impressions': 'show_cnt',
               'spend': 'stat_cost',
               'video_watched_2s': 'play_duration_2s',
               'video_watched_6s': 'play_duration_6s',
               'video_views_p100': 'play_over',
               'video_views_p75': 'play_third_quartile',
               'video_views_p50': 'play_midpoint',
               'video_views_p25': 'play_first_quartile',
               'video_play_actions': 'total_play',
               'comments': 'ad_comment',
               'campaign_automation_type': 'campaign_automation_type',
               'likes': 'ad_like',
               'shares': 'ad_share',
               'follows': 'ad_follows',
               'frequency': 'frequency',
               'real_time_app_install': 'real_time_app_install',
               'app_install': 'app_install',
               'registration': 'registration',
               'purchase': 'purchase',
               'checkout': 'checkout',
               'view_content': 'view_content',
               'engagements': 'Clicks (all)',
               'offline_add_to_cart_events': 'Adds to cart (offline)',
               'offline_add_to_wishlist_events': 'Adds to wishlist (offline)',
               'offline_initiate_checkout_events': 'Checkouts initiated (offline)',
               'offline_contact_events': 'Contacts (offline)',
               'offline_view_content_events': 'Content views (offline)',
               'offline_download_events': 'Downloads (offline)',
               'offline_form_events': 'Form submissions (offline)',
               'offline_place_order_events': 'Orders placed (offline)',
               'offline_add_payment_info_events': 'Payment info adds (offline)',
               'offline_shopping_events': 'Purchases (offline)',
               'offline_complete_registration_events': 'Registrations (offline)',
               'offline_total_schedule': 'Schedules (offline)',
               'offline_subscribe_events': 'Subscriptions (offline)'}
    default_config_file_name = 'tikapi.json'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.access_token = None
        self.advertiser_id = None
        self.campaign_id = None
        self.ad_id_list = []
        self.campaign_id_list = []
        self.config_list = None
        self.headers = None
        self.params = {'advertiser_id': self.advertiser_id,
                       'report_type': 'BASIC',
                       'data_level': 'AUCTION_AD',
                       'metrics': json.dumps(list(self.metrics.keys())),
                       'dimensions': json.dumps(self.dimensions),
                       'page_size': 1000}
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Tik config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.access_token = self.config['access_token']
        self.config_list = [self.config, self.access_token]
        if 'advertiser_id' in self.config:
            self.advertiser_id = self.config['advertiser_id']
            self.params['advertiser_id'] = self.advertiser_id
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Tik config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'Access-Token': self.access_token,
                        'Content-Type': 'application/json'}

    def make_request(self, url, method, headers=None, json_body=None, data=None,
                     params=None, attempt=1):
        if not json_body:
            json_body = {}
        if not headers:
            headers = {}
        if not data:
            data = {}
        if not params:
            params = {}
        if method == 'POST':
            request_method = requests.post
        else:
            request_method = requests.get
        try:
            r = request_method(url, headers=headers, json=json_body, data=data,
                               params=params)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Could not connection with error: {}'.format(e))
                r = None
            else:
                logging.warning('Connection error, pausing for 60s '
                                'and retrying: {}'.format(e))
                time.sleep(60)
                r = self.make_request(url, method, headers, json_body, attempt)
        return r

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    def get_data_default_check(self, sd, ed):
        """
        Insures start date and end date are valid and if not sets them to
        default

        :param sd: start date
        :param ed: end date
        :returns: start date and end date
        """
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        sd, ed = self.date_check(sd, ed)
        return sd, ed

    def get_ids(self, ad=True, ad_url=ad_url, campaign_id=None):
        """
        Gets ad ids or campaign ids depending on ad parameter.
        Will pull all pages of ids and return a list of lists of 100 ids each.

        :param ad: boolean, if True will pull ad ids,
        if False will pull campaign ids
        :param ad_url: url endpoint for ad ids
        :param campaign_id: list of campaign ids to filter on
        :returns: list of lists of 100 ids each
        """
        logging.info('Getting ad ids for campaign_id: {}'.format(campaign_id))
        self.set_headers()
        url = self.base_url + self.version
        if ad:
            url += ad_url
        else:
            url += self.campaign_url
        params = {'advertiser_id': self.advertiser_id,
                  'page': 1,
                  'page_size': 100}
        filters = {'primary_status': 'STATUS_ALL',
                   'status': 'AD_STATUS_ALL' if ad else 'CAMPAIGN_STATUS_ALL',
                   'campaign_ids': [campaign_id]}
        for buying_types in [['AUCTION', 'RESERVATION_RF'],
                             ['RESERVATION_TOP_VIEW']]:
            filters['buying_types'] = buying_types
            params['filtering'] = json.dumps(filters)
            ids, r = self.request_id(url, params, [], ad_ids=ad)
            if r and ids:
                total_pages = r.json()['data']['page_info']['total_page']
                for x in range(total_pages - 1):
                    page_num = x + 2
                    params['page'] = page_num
                    if page_num % 10 == 0:
                        logging.info('Pulling ad_ids page #{} of {}'.format(
                            page_num, total_pages))
                    ids, r = self.request_id(url, params, ids, ad_ids=ad)
        ids = [ids[x:x + 100] for x in range(0, len(ids), 100)]
        logging.info('Returning all ad ids.')
        return ids

    def request_id(self, url, params, ids, ad_ids=True):
        """
        Requests ad ids or campaign ids depending on ad_ids parameter and
        importantly, appends to self.ad_id_list, which is used for merging later
        on

        :param url: url endpoint for request
        :param params: request parameters
        :param ids: list of ids to append to
        :param ad_ids: boolean, if True will pull ad ids,
        if False will pull campaign ids

        :returns: list of ids and request response
        """
        key = 'ad_id' if ad_ids else 'campaign_id'
        r = self.make_request(url, method='GET', headers=self.headers,
                              params=params)
        response_data = r.json()['data']
        if 'list' not in response_data:
            logging.warning(
                'No list in response please make sure accounts '
                'have been given access:\n {}'.format(r.json()))
            return ids, r
        results = response_data['list']
        if ad_ids:
            for result in results:
                creative_list = result.get('creative_list', [])
                if creative_list:
                    result['ad_id'] = creative_list[0].get(
                        'smart_plus_creative_id')
            self.ad_id_list.extend(results)
        else:
            self.campaign_id_list.extend(results)
        ids.extend([x[key] for x in results])
        return ids, r

    @staticmethod
    def unpack_nested_dataframe(df):
        for col in ['dimensions', 'metrics']:
            tdf = pd.DataFrame(df[col].to_list())
            df = df.join(tdf)
        return df

    @staticmethod
    def clean_ad_name(df):
        # ACO / Dynamic Creative ads come back with the video material
        # filename prepended to ad_name (e.g. 'Video4_..._abc.mp4_<real name>').
        # Strip everything up to and including the last <video ext>_ marker.
        if 'ad_name' not in df.columns:
            return df
        pattern = r'(?i)^.*\.(?:mp4|mov|avi|webm|m4v)_'
        df['ad_name'] = df['ad_name'].astype(str).str.replace(
            pattern, '', regex=True)
        return df

    def request_and_get_data(self, sd, ed):
        """
        Requests data from TikTok Ads API and returns a dataframe.

        :param sd: start date for data pull
        :param ed: end date for data pull

        :returns: dataframe
        """
        url = self.base_url + self.version + self.ad_report_url
        self.params['start_date'] = sd
        self.params['end_date'] = ed
        filters = [{'field_name': 'ad_status',
                    'filter_type': 'IN',
                    'filter_value': "[\"STATUS_ALL\"]"}]
        self.params['filtering'] = json.dumps(filters)
        for x in range(1, 1000):
            logging.info('Getting data from {} to {}.  Page #{}.'
                         ''.format(sd, ed, x))
            self.params['page'] = x
            r = self.make_request(url=url, method='GET', headers=self.headers,
                                  params=self.params)
            if ('data' not in r.json() or 'list' not in r.json()['data'] or
                    not r.json()['data']['list']):
                logging.warning('Data not in response as follows:\n'
                                '{}'.format(r.json()))
                return self.df
            df = pd.DataFrame(r.json()['data']['list'])
            df = self.unpack_nested_dataframe(df)
            self.df = pd.concat([self.df, df], ignore_index=True)
            page_rem = r.json()['data']['page_info']['total_page']
            if x >= page_rem:
                break
            logging.info('Data retrieved {} pages remaining'
                         ''.format(page_rem - x))
        id_df = (pd.DataFrame(self.ad_id_list).drop_duplicates(subset="ad_id"))
        self.df = self.df.merge(pd.DataFrame(id_df), on='ad_id',
                                how='left')
        self.df = self.clean_ad_name(self.df)
        cols = self.metrics.copy()
        cols[self.new_date] = self.old_date
        self.df = self.df.rename(columns=cols)
        logging.info('Data successfully pulled.  Returning df.')
        return self.df

    def filter_df_on_campaign(self, df):
        """
        Filters dataframe on campaign name column based on campaign_id
        in config file

        :param df: dataframe to filter
        :returns: filtered dataframe
        """
        campaign_col = 'campaign_name'
        if self.campaign_id:
            df = utl.filter_df_on_col(df, campaign_col, self.campaign_id)
        return df

    def reset_params(self):
        self.df = pd.DataFrame()
        self.ad_id_list = []

    def get_data(self, sd=None, ed=None, fields=None):
        """
        Main function to get data from TikTok Ads API,
        called in importhandler.py
        """
        sd, ed = self.get_data_default_check(sd, ed)
        self.reset_params()
        for ad_url in self.check_url():
            self.get_ids(ad_url=ad_url['ad_url'],
                         campaign_id=ad_url['campaign_id'])
        self.df = self.request_and_get_data(sd, ed)
        self.df = self.filter_df_on_campaign(self.df)
        return self.df

    def check_url(self):
        self.set_headers()
        url = self.base_url + self.version + self.campaign_url
        params = {'advertiser_id': self.advertiser_id,
                  'fields': '["campaign_automation_type", "campaign_name"]'}
        r = self.make_request(url=url, method='GET', headers=self.headers,
                              params=params)
        response = r.json()
        campaign_list = response.get('data', {}).get('list', [])
        if self.campaign_id:
            campaign_list = [
                c for c in campaign_list
                if self.campaign_id in c.get('campaign_name', '')
            ]
        urls = []
        for campaign in campaign_list:
            ad_url = self.smart_url if 'SMART' in campaign.get(
                'campaign_automation_type', '') else self.ad_url
            urls.append(
                {'ad_url': ad_url, 'campaign_id': campaign.get('campaign_id')})
        return urls


    def check_advertiser_id(self, results, acc_col, success_msg, failure_msg):
        metrics = 'spend'
        dimensions = 'campaign_id'
        sd = dt.datetime.today() - dt.timedelta(days=365)
        ed = dt.datetime.today()
        url = self.base_url + self.version + self.ad_report_url
        self.set_headers()
        self.params['start_date'] = sd.date().isoformat()
        self.params['end_date'] = ed.date().isoformat()
        self.params['metrics'] = json.dumps([metrics])
        self.params['dimensions'] = json.dumps([dimensions])
        self.params['data_level'] = 'AUCTION_CAMPAIGN'
        r = self.make_request(
            url=url, method='GET', headers=self.headers, params=self.params)
        if (r.status_code == 200 and
                'data' in r.json() and 'list' in r.json()['data']):
            row = [acc_col, ' '.join([success_msg, str(self.advertiser_id)]),
                   True]
            results.append(row)
        else:
            msg = ('Advertiser ID NOT Found. '
                   'Double Check ID and Ensure Permissions were granted.'
                   '\n Error Msg:')
            r = r.json()
            row = [acc_col, ' '.join([failure_msg, msg, r['message']]), False]
            results.append(row)
        return results, r

    def check_campaign_ids(self, results, camp_col, success_msg, failure_msg):
        self.get_ids(ad=False)
        if not self.campaign_id_list:
            msg = ' '.join([failure_msg, 'No Campaigns Under Advertiser. '
                                         'Check Active and Permissions.'])
            row = [camp_col, msg, False]
            results.append(row)
            return results
        df = pd.DataFrame(data=self.campaign_id_list)
        df = self.filter_df_on_campaign(df)
        if 'campaign_name' not in df.columns:
            msg = ' '.join([failure_msg, 'No Campaigns Under Advertiser. '
                                         'Check Active and Permissions.'])
            row = [camp_col, msg, False]
            results.append(row)
            return results
        campaign_names = df['campaign_name'].to_list()
        msg = ' '.join(
            [success_msg, 'CAMPAIGNS INCLUDED IF DATA PAST START DATE:'])
        row = [camp_col, msg, True]
        results.append(row)
        for campaign in campaign_names:
            row = [camp_col, campaign, True]
            results.append(row)
        return results

    def test_connection(self, acc_col, camp_col, acc_pre):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        self.set_headers()
        results, r = self.check_advertiser_id(
            [], acc_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        results = self.check_campaign_ids(
            results, camp_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
