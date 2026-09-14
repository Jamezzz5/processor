import os
import sys
import time
import json
import shutil
import logging
import requests
import reporting.utils as utl
import tableaudocumentapi as tda
import tableauserverclient as tsc
import reporting.hyper.postgres_extractor as pge

config_path = utl.config_path

tsc_exceptions = tsc.server.endpoint.exceptions
retry_errors = (tsc_exceptions.ServerResponseError,
                tsc_exceptions.InternalServerError,
                tsc_exceptions.NonXMLResponseError,
                requests.exceptions.RequestException)


class TabApi(object):
    base_url = 'https://us-east-1.online.tableau.com/api/'
    version = '3.3'
    base_url = '{}{}/'.format(base_url, version)

    def __init__(self, config_file='tabconfig.json'):
        self.config_file = config_file
        self.config = None
        self.username = None
        self.password = None
        self.site = None
        self.site_id = None
        self.datasource = None
        self.config_list = None
        self.headers = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Tableau config file: {}'.format(config))
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
        self.username = self.config['username']
        self.password = self.config['password']
        self.site = self.config['site']
        self.datasource = self.config['datasource']
        self.config_list = [self.config, self.username, self.password,
                            self.site, self.datasource]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Tableau config file.  '
                                'Aborting.'.format(item))

    def set_headers(self):
        self.headers = {'Content-Type': 'application/json',
                        'Accept': 'application/json'}
        data = {"credentials": {
                "personalAccessTokenName": self.username,
                "personalAccessTokenSecret": self.password,
                "site": {"contentUrl": self.site}}}
        url = self.create_url('auth/signin')
        r = self.make_request(url, 'post', resp_key='credentials',
                              json_data=data)
        if 'credentials' not in r.json():
            logging.warning('credentials not in: {}'.format(r.json()))
            return False
        session_id = r.json()['credentials']['token']
        self.headers['X-Tableau-Auth'] = session_id
        self.site_id = r.json()['credentials']['site']['id']
        return True

    def create_url(self, addition_url, site_url=False):
        url = self.base_url
        if site_url:
            url = '{}sites/{}/'.format(url, self.site_id)
        url = '{}{}'.format(url, addition_url)
        return url

    def make_request(self, url, request_type='get', resp_key=None,
                     json_data=None, attempt=1):
        if request_type == 'get':
            r = requests.get(url, headers=self.headers)
        else:
            r = requests.post(url, json=json_data, headers=self.headers)
        if r and resp_key and resp_key not in r.json():
            logging.warning('Attempt {}, {} not in response: '
                            '{}'.format(attempt, resp_key, r.json()))
            time.sleep(10)
            attempt += 1
            if attempt < 10:
                r = self.make_request(url, request_type, resp_key,
                                      json_data=json_data, attempt=attempt)
            else:
                logging.warning('Maximum attempts exceeded - stopping.')
                return False
        return r

    def find_datasource(self):
        logging.info('Finding datasource: {}.'.format(self.datasource))
        ds_url = 'datasources?pageSize=1000'
        ds_url = self.create_url(ds_url, site_url=True)
        r = self.make_request(ds_url, resp_key='datasources')
        ds_id = [x['id'] for x in r.json()['datasources']['datasource']
                 if x['name'] == self.datasource][0]
        return ds_id

    def find_extract_refreshes(self, ds_id):
        logging.info('Finding extract refresh id: {}.'.format(ds_id))
        er_url = 'tasks/extractRefreshes'
        er_url = self.create_url(er_url, site_url=True)
        r = self.make_request(er_url, resp_key='tasks')
        er_id = [x['extractRefresh']['id'] for x in r.json()['tasks']['task']
                 if 'datasource' in x['extractRefresh'] and
                 x['extractRefresh']['datasource']['id'] == ds_id]
        if er_id:
            er_id = er_id[0]
        else:
            er_id = None
        return er_id

    def send_refresh_request(self, er_id):
        logging.info('Sending refresh request for id: {}'.format(er_id))
        rr_url = 'tasks/extractRefreshes/{}/runNow'.format(er_id)
        rr_url = self.create_url(rr_url, site_url=True)
        r = self.make_request(rr_url, 'post', resp_key='job', json_data={})
        job_id = r.json()['job']['id']
        return job_id

    def check_job(self, job_id):
        job_url = 'jobs/{}'.format(job_id)
        job_url = self.create_url(job_url, site_url=True)
        r = self.make_request(job_url, resp_key='job')
        logging.info('Job request response as follows {}'.format(r.json()))
        if 'completedAt' in r.json()['job']:
            logging.info('Extract refresh completed at {}'
                         ''.format(r.json()['job']['completedAt']))
            return True
        else:
            return False

    def check_job_until_complete(self, job_id):
        for x in range(1, 100):
            if self.check_job(job_id):
                break
            else:
                logging.info('Extract refresh not complete.  '
                             'Attempt #{}'.format(x))
                time.sleep(120)

    def refresh_extract(self):
        if self.datasource and self.username:
            header_set = self.set_headers()
            if not header_set:
                logging.error('Extract not refreshed.')
                return False
            ds_id = self.find_datasource()
            er_id = self.find_extract_refreshes(ds_id)
            if er_id:
                job_id = self.send_refresh_request(er_id)
                self.check_job_until_complete(job_id)
            else:
                logging.warning('No extract to refresh configure one.')
        else:
            logging.warning('Tableau api not configured, it was not refreshed.')

    def get_tsc_auth(self):
        tableau_auth = tsc.PersonalAccessTokenAuth(self.username, self.password,
                                                   self.site)
        server = tsc.Server('https://us-east-1.online.tableau.com',
                            use_server_version=True)
        return tableau_auth, server

    @staticmethod
    def retry_call(func, description, *args, attempts=10, wait=10, **kwargs):
        """Call a Tableau endpoint, retrying it when the server is unwell.

        :param func: endpoint method to call
        :param description: phrase naming the call, used in log messages
        :param args: passed through to func
        :param attempts: how many times to try before giving up
        :param wait: seconds to wait after the first failure
        :param kwargs: passed through to func
        :returns: func's return value, or None when every attempt failed
        """
        for x in range(attempts):
            try:
                return func(*args, **kwargs)
            except retry_errors as e:
                msg = 'Attempt {} of {}.  Could not {}: \n{}'.format(
                    x + 1, attempts, description, e)
                logging.warning(msg)
                if x + 1 < attempts:
                    sleep_for = min(wait * (x + 1), 60)
                    logging.warning('Retrying in {}s'.format(sleep_for))
                    time.sleep(sleep_for)
        logging.error('Could not {} after {} attempts.  Skipping.'.format(
            description, attempts))
        return None

    @staticmethod
    def create_hyper(db, table_name='auto_processor'):
        db_conn_dict = dict(
            host=db.config['HOST'],
            dbname=db.config['DATABASE'],
            user=db.config['USER'],
            password=db.config['PASS'],
            port=db.config['PORT'])
        db_conn_dict = dict(connection=db_conn_dict)
        pg = pge.PostgresExtractor(db_conn_dict, '', '')
        hyper_paths = pg.query_to_hyper_files(
            source_table="lqadb.{}".format(table_name),
            hyper_table_name="{}.hyper".format(table_name))
        for x in hyper_paths:
            logging.info('Hyper created at {}'.format(x))

    def publish_object(self, db, object_name='auto_processor',
                       object_type='datasource'):
        tableau_auth, server = self.get_tsc_auth()
        with server.auth.sign_in(tableau_auth):
            publish_mode = tsc.Server.PublishMode.Overwrite
            for project in tsc.Pager(server.projects):
                if project.name == 'dev-test':
                    project_id = project.id
                    break
            if object_type == 'datasource':
                tsc_object = tsc.DatasourceItem
                ser_object = server.datasources
                ext_object = '.hyper'
            elif object_type == 'workbook':
                tsc_object = tsc.WorkbookItem
                ser_object = server.workbooks
                ext_object = '.twbx'
            pub_obj = tsc_object(project_id)
            connection = tsc.ConnectionItem()
            connection.server_address = db.config['HOST']
            connection.server_port = db.config['PORT']
            conn_cred = tsc.ConnectionCredentials(
                name=db.config['USER'], password=db.config['PASS'], embed=True)
            connection.connection_credentials = conn_cred
            published_obj = ser_object.publish(
                pub_obj, '{}{}'.format(object_name, ext_object),
                mode=publish_mode, connection_credentials=conn_cred)
            logging.info('Object published with name: {}'.format(
                published_obj.name))
            return True

    def publish_object_with_error_catch(self, db, object_name,
                                        object_type='datasource'):
        """Publish a datasource or workbook, retrying a failing server.

        The whole publish is retried, sign in and project lookup included,
        since those reach the same endpoints that drop the connection.

        :param db: database whose connection details get embedded
        :param object_name: name to publish the object under
        :param object_type: 'datasource' or 'workbook'
        :returns: True when the object published
        """
        published = self.retry_call(
            self.publish_object, 'publish {}'.format(object_name), db,
            wait=30, object_name=object_name, object_type=object_type)
        return bool(published)

    def create_publish_hyper(self, db, table_name='auto_processor'):
        self.create_hyper(db, table_name)
        self.publish_object_with_error_catch(db, object_name=table_name,
                                             object_type='datasource')

    def download_workbook(self, workbook_name='auto_template'):
        tableau_auth, server = self.get_tsc_auth()
        with server.auth.sign_in(tableau_auth):
            for wb in tsc.Pager(server.workbooks):
                if wb.name == workbook_name:
                    wb_id = wb.id
                    file_path = server.workbooks.download(wb_id)
                    logging.info('workbook downloaded to {}'.format(file_path))
                    return file_path

    def download_workbook_with_error_catch(self, wb_name='auto_template'):
        """Download a workbook, retrying a failing server.

        :param wb_name: name of the workbook to download
        :returns: path the workbook downloaded to, or '' when it did not
        """
        file_path = self.retry_call(
            self.download_workbook, 'download {}'.format(wb_name), wb_name)
        return file_path if file_path else ''

    @staticmethod
    def change_workbook_datasource(
            workbook_name='auto_template', table_name='auto_processor'):
        source_wb = tda.Workbook('{}'.format(workbook_name))
        source_wb.datasources[-1].connections[0].dbname = table_name
        source_wb.save()

    def create_publish_workbook_hyper(self, db, table_name='auto_processor',
                                      wb_name='auto_template', new_wb_name=''):
        """Publish the hyper extract and the workbook that reads it.

        :param db: database whose connection details get embedded
        :param table_name: name to publish the extract under
        :param wb_name: template workbook to base the new one on
        :param new_wb_name: name to publish the new workbook under
        :returns: True when the workbook published
        """
        if not self.username:
            return False
        self.create_publish_hyper(db, table_name)
        file_path = self.download_workbook_with_error_catch(wb_name)
        if not file_path:
            return False
        self.change_workbook_datasource(file_path, table_name)
        new_path = file_path.replace(wb_name, new_wb_name)
        shutil.copy(file_path, new_path)
        return self.publish_object_with_error_catch(
            db, object_name=new_wb_name, object_type='workbook')
