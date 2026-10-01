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
               'reach': 'reach',
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
               'offline_subscribe_events': 'Subscriptions (offline)',
               'offline_total_crm_events': 'CRM events (offline)',
               'offline_add_to_cart_events_value':
                   'Adds to cart (offline) - Value',
               'offline_add_to_wishlist_events_value':
                   'Adds to wishlist (offline) - Value',
               'offline_initiate_checkout_events_value':
                   'Checkouts initiated (offline) - Value',
               'offline_contact_events_value':
                   'Contacts (offline) - Value',
               'offline_view_content_events_value':
                   'Content views (offline) - Value',
               'offline_download_events_value':
                   'Downloads (offline) - Value',
               'offline_form_events_value':
                   'Form submissions (offline) - Value',
               'offline_place_order_events_value':
                   'Orders placed (offline) - Value',
               'offline_add_payment_info_events_value':
                   'Payment info adds (offline) - Value',
               'offline_shopping_events_value':
                   'Purchases (offline) - Value',
               'offline_complete_registration_events_value':
                   'Registrations (offline) - Value',
               'offline_total_schedule_value':
                   'Schedules (offline) - Value',
               'offline_subscribe_events_value':
                   'Subscriptions (offline) - Value',
               'offline_crm_event_value': 'CRM event value (offline)'}
    default_config_file_name = 'tikapi.json'
    id_page_size = 100
    request_timeout = 120
    request_attempts = 100
    retry_pause = 60
    campaign_fields = ['campaign_id', 'campaign_name',
                       'campaign_automation_type']
    buying_type_groups = [['AUCTION', 'RESERVATION_RF'],
                          ['RESERVATION_TOP_VIEW']]
    ad_id_col = 'ad_id'
    campaign_col = 'campaign_name'
    campaign_id_col = 'campaign_id'
    api_field_options = (
        ('No Reach',
         'Skip the whole range campaign and ad group reach and the daily '
         'advertiser reach'),)
    reach_metrics = ['reach', 'frequency']
    reach_max_days = 365
    daily_reach_max_days = 30
    reach_reports = [
        {'data_level': 'AUCTION_CAMPAIGN', 'dimensions': ['campaign_id'],
         'status': 'campaign_status', 'names': ['campaign_name'],
         'suffix': ' - no_date - campaign_report'},
        {'data_level': 'AUCTION_ADGROUP', 'dimensions': ['adgroup_id'],
         'status': 'adgroup_status',
         'names': ['campaign_name', 'campaign_id', 'adgroup_name'],
         'suffix': ' - no_date - adgroup_report'},
        {'data_level': 'AUCTION_ADVERTISER',
         'dimensions': ['advertiser_id', 'stat_time_day'], 'status': '',
         'names': [], 'suffix': ' - vendor_report', 'daily': True}]

    def __init__(self):
        self.config = None
        self.config_file = None
        self.access_token = None
        self.advertiser_id = None
        self.campaign_id = None
        self.campaign_ids = []
        self.campaign_name_filter = []
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

    def make_request(self, url, method, headers=None, json_body=None,
                     data=None, params=None):
        """
        Sends one request, retrying a dropped or unanswered connection.

        A timeout is retried like a refused socket: TikTok answering
        nothing for two minutes and TikTok refusing the connection both
        used to end the whole import, since only the latter was caught.
        Every retry resends the same params, so a report page is never
        re-requested without them.

        :param url: endpoint to request
        :param method: 'GET' or 'POST'
        :param headers: request headers
        :param json_body: json body of a POST
        :param data: form data of a POST
        :param params: query params
        :returns: the response, None once every attempt failed
        """
        request_method = requests.post if method == 'POST' else requests.get
        kwargs = {'headers': headers or {}, 'json': json_body or {},
                  'data': data or {}, 'params': params or {},
                  'timeout': self.request_timeout}
        for attempt in range(1, self.request_attempts + 1):
            try:
                return request_method(url, **kwargs)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                logging.warning(
                    'Connection error on attempt {} of {}, pausing for {}s '
                    'and retrying: {}'.format(attempt, self.request_attempts,
                                              self.retry_pause, e))
                if attempt < self.request_attempts:
                    time.sleep(self.retry_pause)
        logging.warning('No response from {} after {} attempts.'.format(
            url, self.request_attempts))
        return None

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
        :param campaign_id: campaign id to filter on, every campaign if unset
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
                  'page_size': self.id_page_size}
        filters = {'primary_status': 'STATUS_ALL',
                   'status': 'AD_STATUS_ALL' if ad else 'CAMPAIGN_STATUS_ALL'}
        if campaign_id:
            filters['campaign_ids'] = [campaign_id]
        ids = []
        for buying_types in self.buying_type_groups:
            filters['buying_types'] = buying_types
            params['filtering'] = json.dumps(filters)
            params['page'] = 1
            group_start = len(ids)
            ids, r = self.request_id(url, params, ids, ad_ids=ad)
            if not r or len(ids) == group_start:
                continue
            page_info = r.json()['data'].get('page_info', {})
            total_pages = page_info.get('total_page', 1)
            for x in range(total_pages - 1):
                page_num = x + 2
                params['page'] = page_num
                if page_num % 10 == 0:
                    logging.info('Pulling ad_ids page #{} of {}'.format(
                        page_num, total_pages))
                ids, r = self.request_id(url, params, ids, ad_ids=ad)
        ids = [ids[x:x + self.id_page_size]
               for x in range(0, len(ids), self.id_page_size)]
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
        key = self.ad_id_col if ad_ids else self.campaign_id_col
        r = self.make_request(url, method='GET', headers=self.headers,
                              params=params)
        if not r:
            logging.warning('No response getting {}s.'.format(key))
            return ids, r
        response_data = r.json().get('data', {})
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

    @property
    def reach_cols(self):
        """
        Every column a reach row fills, so importhandler can find the reach
        rows a merged raw file already holds and replace them.

        :returns: list of suffixed reach and frequency column names
        """
        return ['{}{}'.format(x, report['suffix'])
                for report in self.reach_reports for x in self.reach_metrics]

    @staticmethod
    def status_filter(status_field):
        """
        A filter keeping deleted objects, which a synchronous basic report
        otherwise drops by defaulting the status to STATUS_NOT_DELETE.

        :param status_field: ad_status, adgroup_status or campaign_status
        :returns: the filtering param as a json string
        """
        return json.dumps([{'field_name': status_field,
                            'filter_type': 'IN',
                            'filter_value': json.dumps(['STATUS_ALL'])}])

    def request_pages(self, sd, ed, params):
        """
        Requests every page of one report.

        :param sd: start date, for logging
        :param ed: end date, for logging
        :param params: report params, whose page is set in place
        :returns: dataframe of every page with dimensions and metrics
            unpacked into columns
        """
        url = self.base_url + self.version + self.ad_report_url
        df = pd.DataFrame()
        for x in range(1, 1000):
            logging.info('Getting data from {} to {}.  Page #{}.'
                         ''.format(sd, ed, x))
            params['page'] = x
            r = self.make_request(url=url, method='GET', headers=self.headers,
                                  params=params)
            if not r:
                logging.warning('No response for page #{}.'.format(x))
                break
            if ('data' not in r.json() or 'list' not in r.json()['data'] or
                    not r.json()['data']['list']):
                logging.warning('Data not in response as follows:\n'
                                '{}'.format(r.json()))
                break
            tdf = pd.DataFrame(r.json()['data']['list'])
            tdf = self.unpack_nested_dataframe(tdf)
            df = pd.concat([df, tdf], ignore_index=True)
            page_rem = r.json()['data']['page_info']['total_page']
            if x >= page_rem:
                break
            logging.info('Data retrieved {} pages remaining'
                         ''.format(page_rem - x))
        return df

    def request_reach(self, sd, ed):
        """
        Requests reach at every level: one row per campaign and per ad
        group for the whole range, and one row per day for the advertiser.

        :param sd: start date as YYYY-MM-DD
        :param ed: end date as YYYY-MM-DD
        :returns: dataframe of reach rows, their measures suffixed per level
        """
        frames = [self.request_reach_report(sd, ed, report)
                  for report in self.reach_reports]
        return pd.concat(frames, ignore_index=True)

    def request_reach_report(self, sd, ed, report):
        """
        Requests one reach report and dates its rows so a date merge keeps
        them: a whole range row to the start of the range, a daily row to
        its own day.

        Leaving the day dimension out is what makes TikTok count each
        user once across the range, and it lifts the report's cap from
        30 days to 365.  A longer range is skipped rather than split,
        since the reach of two halves cannot be added back together.  The
        daily advertiser report counts people once within each day, so
        its 30 day blocks stack.  TikTok cannot narrow an advertiser row
        to the card's campaigns, so a card with a campaign filter gets no
        daily advertiser reach rather than the whole account's.

        :param sd: start date as YYYY-MM-DD
        :param ed: end date as YYYY-MM-DD
        :param report: the reach_reports entry to pull
        :returns: dataframe of the report's rows, their measures suffixed
        """
        start = dt.datetime.strptime(sd, '%Y-%m-%d')
        end = dt.datetime.strptime(ed, '%Y-%m-%d')
        if report.get('daily'):
            if self.parse_campaign_filter():
                logging.warning(
                    'Skipping daily advertiser reach: TikTok cannot narrow '
                    'it to the campaign filter {}.'.format(
                        self.campaign_name_filter))
                return pd.DataFrame()
            blocks = utl.date_blocks(start, end, self.daily_reach_max_days)
        else:
            days = (end - start).days + 1
            if days > self.reach_max_days:
                logging.warning(
                    'Skipping reach: TikTok counts each user once only '
                    'within a single request of at most {} days, and {} to '
                    '{} is {} days.'.format(self.reach_max_days, sd, ed,
                                            days))
                return pd.DataFrame()
            blocks = [(start, end)]
        frames = []
        for block_sd, block_ed in blocks:
            params = self.reach_params(block_sd.strftime('%Y-%m-%d'),
                                       block_ed.strftime('%Y-%m-%d'), report)
            frames.append(self.request_pages(params['start_date'],
                                             params['end_date'], params))
        df = pd.concat(frames, ignore_index=True)
        df = df.rename(columns={
            x: '{}{}'.format(x, report['suffix']) for x in self.reach_metrics})
        if df.empty:
            return df
        if report.get('daily'):
            return df.rename(columns={self.new_date: self.old_date})
        df[self.old_date] = '{} 00:00:00'.format(sd)
        return df

    def reach_params(self, sd, ed, report):
        """
        The request params of one reach report.  A status filter keeps
        deleted objects in a campaign or ad group report; the advertiser
        level accepts no status filter and counts every ad.

        :param sd: start date as YYYY-MM-DD
        :param ed: end date as YYYY-MM-DD
        :param report: the reach_reports entry to request
        :returns: dict of request params
        """
        params = {'advertiser_id': self.advertiser_id,
                  'report_type': 'BASIC',
                  'data_level': report['data_level'],
                  'dimensions': json.dumps(report['dimensions']),
                  'metrics': json.dumps(report['names'] + self.reach_metrics),
                  'start_date': sd,
                  'end_date': ed,
                  'page_size': 1000}
        if report['status']:
            params['filtering'] = self.status_filter(report['status'])
        return params

    def request_and_get_data(self, sd, ed):
        """
        Requests data from TikTok Ads API and returns a dataframe.

        :param sd: start date for data pull
        :param ed: end date for data pull

        :returns: dataframe
        """
        self.params['start_date'] = sd
        self.params['end_date'] = ed
        self.params['filtering'] = self.status_filter('ad_status')
        self.df = pd.concat([self.df, self.request_pages(sd, ed, self.params)],
                            ignore_index=True)
        self.df = self.merge_ad_ids(self.df)
        self.df = self.clean_ad_name(self.df)
        cols = self.metrics.copy()
        cols[self.new_date] = self.old_date
        self.df = self.df.rename(columns=cols)
        logging.info('Data successfully pulled.  Returning df.')
        return self.df

    def merge_ad_ids(self, df):
        """
        Joins the pulled ad ids on to the report for their names.

        :param df: the downloaded report
        :returns: the report with the ad and campaign names joined on
        """
        id_df = pd.DataFrame(self.ad_id_list)
        has_ids = [self.ad_id_col in x.columns for x in (df, id_df)]
        if not all(has_ids):
            logging.warning(
                'No ad ids to merge on, returning the report without ad '
                'names.  Ads pulled: {}.  Report rows: {}.'.format(
                    len(self.ad_id_list), len(df)))
            return df
        id_df = id_df.drop_duplicates(subset=self.ad_id_col)
        return df.merge(id_df, on=self.ad_id_col, how='left')

    def parse_campaign_filter(self):
        """
        Splits the campaign filter into ids to match and values to match.

        :returns: the filter values to match campaigns against
        """
        self.campaign_ids, self.campaign_name_filter = (
            utl.parse_campaign_filter(self.campaign_id))
        return self.campaign_name_filter

    def filter_df_on_campaign(self, df, keep_on_no_match=True):
        """
        Filters a dataframe down to the campaigns the filter names.

        :param df: dataframe to filter
        :param keep_on_no_match: whether a filter that matches nothing keeps
            the unfiltered df
        :returns: filtered dataframe
        """
        self.parse_campaign_filter()
        return utl.filter_df_on_campaign(
            df, self.campaign_name_filter, self.campaign_col,
            self.campaign_id_col, keep_on_no_match=keep_on_no_match)

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
        if 'No Reach' not in (fields or []):
            self.df = pd.concat([self.df, self.request_reach(sd, ed)],
                                ignore_index=True)
        self.df = self.filter_df_on_campaign(self.df)
        return self.df

    def get_reach(self, sd=None, ed=None, fields=None):
        """
        Only the reach rows, the whole range campaign and ad group rows
        and the daily advertiser rows, for a caller whose delivery range
        is shorter than the range reach has to cover: importhandler trims
        a merge card's delivery to its merge window but pulls reach from
        the card's start date.

        :param sd: start of the reach range
        :param ed: end of the reach range
        :param fields: the card's api fields, read only for No Reach
        :returns: dataframe of reach rows, their measures suffixed per level
        """
        if 'No Reach' in (fields or []):
            return pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        self.set_headers()
        return self.filter_df_on_campaign(self.request_reach(sd, ed))

    def request_campaigns(self, buying_types):
        """
        Requests every campaign of one buying type group, all pages.

        The endpoint returns ten rows a page by default, so an unpaginated
        call silently reports only the first ten campaigns of an advertiser
        that has more than that.

        :param buying_types: buying types to filter the campaign list on
        :returns: list of campaign dicts
        """
        url = self.base_url + self.version + self.campaign_url
        campaigns = []
        for page in range(1, 1000):
            params = {'advertiser_id': self.advertiser_id,
                      'page': page,
                      'page_size': self.id_page_size,
                      'fields': json.dumps(self.campaign_fields),
                      'filtering': json.dumps(
                          {'buying_types': buying_types})}
            r = self.make_request(url=url, method='GET',
                                  headers=self.headers, params=params)
            if not r:
                logging.warning('No response for campaign list, buying '
                                'types {}.'.format(buying_types))
                break
            response_data = r.json().get('data', {})
            if 'list' not in response_data:
                logging.warning('No campaign list in response for buying '
                                'types {}:\n {}'.format(buying_types,
                                                        r.json()))
                break
            campaigns.extend(response_data['list'])
            total_pages = response_data.get('page_info', {}).get(
                'total_page', 1)
            if page >= total_pages:
                break
            if page >= 999:
                logging.warning(
                    'Maximum number of pages (999) reached for campaign list, '
                    'buying types {}. There may be additional campaigns that '
                    'were not retrieved.'.format(buying_types)
                )
        return campaigns

    def get_campaign_list(self):
        """
        Every campaign of the advertiser across all buying type groups.

        :returns: list of campaign dicts
        """
        self.set_headers()
        campaign_list = []
        for buying_types in self.buying_type_groups:
            campaign_list.extend(self.request_campaigns(buying_types))
        return campaign_list

    def check_url(self):
        """
        Pairs every campaign with the ad endpoint its ads live on.

        Smart+ campaigns expose their ads on their own endpoint, so the
        campaign list is walked first rather than pulling ad ids blind.  An
        advertiser whose campaign list does not come back is pulled blind
        anyway, since no ad ids at all leaves the report with no ad names.

        :returns: list of dicts of ad url and campaign id
        """
        campaign_list = self.get_campaign_list()
        if not campaign_list:
            logging.warning('No campaigns returned, pulling every ad id of '
                            'the advertiser instead.')
            return [{'ad_url': self.ad_url, 'campaign_id': None}]
        campaign_list = self.filter_campaign_list(campaign_list)
        urls = []
        for campaign in campaign_list:
            ad_url = self.smart_url if 'SMART' in campaign.get(
                'campaign_automation_type', '') else self.ad_url
            urls.append(
                {'ad_url': ad_url, 'campaign_id': campaign.get('campaign_id')})
        logging.info('Found {} campaigns to pull ad ids for.'.format(
            len(urls)))
        return urls

    def filter_campaign_list(self, campaign_list):
        """
        Narrows the campaign list to the campaigns the filter names.

        A value is matched against the campaign id as well as the name,
        since a filter is as often one as the other.  A filter that matches
        nothing keeps every campaign, so a stale value costs the report its
        campaign filter rather than every ad name in it.

        :param campaign_list: every campaign of the advertiser
        :returns: the campaigns to pull ad ids for
        """
        values = self.parse_campaign_filter()
        if not values:
            return campaign_list
        campaigns = [
            c for c in campaign_list
            if any(v in str(c.get(self.campaign_col, '')) or
                   v == str(c.get(self.campaign_id_col, '')) for v in values)]
        if not campaigns:
            logging.warning(
                'Campaign filter {} did not match any of the {} campaigns '
                'returned, pulling ad ids for all of them.  Campaigns: '
                '{}'.format(values, len(campaign_list),
                            sorted(str(c.get(self.campaign_col, ''))
                                   for c in campaign_list)))
            return campaign_list
        logging.info('Filtered to {} of {} campaigns on the campaign '
                     'filter.'.format(len(campaigns), len(campaign_list)))
        return campaigns

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
        df = self.filter_df_on_campaign(df, keep_on_no_match=False)
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
