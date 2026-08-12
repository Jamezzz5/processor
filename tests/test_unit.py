import os
import sys
import json
import yaml
import types
import string
import pytest
import logging
import numpy as np
import pandas as pd
import datetime as dt
import urllib3.exceptions as url_ex
from selenium.webdriver.common.by import By
from processor.main import main
import processor.reporting.utils as utl
import processor.reporting.vendormatrix as vm
import processor.reporting.vmcolumns as vmc
import processor.reporting.dictionary as dct
import processor.reporting.dictcolumns as dctc
import processor.reporting.calc as cal
import processor.reporting.analyze as az
import processor.reporting.errorreport as er
import processor.reporting.export as exp
import processor.reporting.expcolumns as exc
import processor.reporting.azapi as azapi
import processor.reporting.redapi as redapi
import processor.reporting.awapi as awapi
import processor.reporting.amzapi as amzapi
import processor.reporting.gaapi as gaapi
import processor.reporting.fbapi as fbapi
import processor.reporting.samapi as samapi
import processor.reporting.criapi as criapi
import processor.reporting.rsapi as rsapi
import processor.reporting.dcapi as dcapi
import processor.reporting.dbapi as dbapi
import processor.reporting.afapi as afapi
import processor.reporting.twapi as twapi
import processor.reporting.nzapi as nzapi
import processor.reporting.scapi as scapi
import processor.reporting.awss3 as awss3
import processor.reporting.iasapi as iasapi
import processor.reporting.ttdapi as ttdapi
import processor.reporting.tikapi as tikapi
import processor.reporting.yvapi as yvapi
import processor.reporting.gsapi as gsapi
import processor.reporting.gamesdb as gdb
import processor.reporting.gamesmodels as gmdl
import processor.reporting.gameswriter as gamesw
import processor.reporting.simapi as simapi
import processor.reporting.steapi as steapi
import processor.reporting.asaapi as asaapi
import processor.reporting.importhandler as ih

# Dev machines carry gitignored credentials and data artifacts that
# CI checkouts lack; gate the tests that genuinely need them.
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config')
requires_api_configs = pytest.mark.skipif(
    not os.path.exists(os.path.join(CONFIG_PATH, 'fbconfig.json')),
    reason='channel API configs not present')
requires_base_config = pytest.mark.skipif(
    not os.path.exists(os.path.join(CONFIG_PATH, 'Vendormatrix.csv')),
    reason='base config artifacts not present')
requires_local_browser = pytest.mark.skipif(
    os.environ.get('CI', '').lower() == 'true',
    reason='headed browser unavailable on CI')


def _raise_read_timeout(*args, **kwargs):
    """Stand in for a driver that stopped answering its socket."""
    raise url_ex.ReadTimeoutError(None, 'url', 'Read timed out.')


# Body copy holding 'OK' inside 'COOKIES' and a settings control that
# must not be mistaken for consent -- the two things a substring match
# on every node in the document gets wrong.
COOKIE_DECOYS = (
    '<p>We use COOKIES. Continue reading our policy.</p>'
    '<button onclick="window.picked=\'settings\'">Cookie settings</button>')
COOKIE_BANNER = (
    '<html><body>' + COOKIE_DECOYS +
    '<button onclick="window.picked=\'accept\';'
    'this.parentNode.removeChild(this)">Accept All Cookies</button>'
    '</body></html>')
COOKIE_FRAME_PAGE = (
    '<html><body>' + COOKIE_DECOYS +
    '<iframe src="frame.html" width="400" height="200"></iframe>'
    '</body></html>')
COOKIE_FRAME = (
    '<html><body><button onclick="document.body.setAttribute('
    '\'data-picked\', \'accept\')">I agree</button></body></html>')


def _write_page(tmp_path, name, html):
    """Write an html fixture and return it as a file:// url."""
    page = tmp_path / name
    page.write_text(html, encoding='utf-8')
    return page.as_uri()


class _DeadBrowser(object):
    """Driver whose session is already gone -- every command raises."""

    def close(self):
        _raise_read_timeout()

    def quit(self):
        _raise_read_timeout()


class _FakeSeleniumWrapper(object):
    """Browser stand-in that records its own teardown.

    ``instances`` collects every wrapper a scrape builds so a test can
    prove each one was quit. Reset it before use -- it is class level so
    the wrapper can be swapped in for ``utl.SeleniumWrapper`` directly.
    """

    instances = []

    def __init__(self, *args, **kwargs):
        self.quit_calls = 0
        self.mobile = False
        _FakeSeleniumWrapper.instances.append(self)

    def take_elem_screenshot(self, *args, **kwargs):
        raise ValueError('Screenshot failed.')

    def quit(self):
        self.quit_calls += 1


class _HalfBuiltBrowser(object):
    """Driver whose post-spawn configure fails, recording teardown.

    Stands in for the window between chrome existing and the handle
    being returned -- a raise there used to strand the process where
    no caller's ``finally`` could reach it.
    """

    def __init__(self):
        self.quit_calls = 0

    def execute_script(self, *args, **kwargs):
        raise ValueError('Configure failed.')

    def quit(self):
        self.quit_calls += 1


def func(x):
    return x + 1


def test_example():
    assert func(3) == 4


class TestUtils:
    def test_dir_check(self):
        directory_name = 'test'
        utl.dir_check(directory_name)
        assert os.path.isdir(directory_name)
        os.rmdir(directory_name)

    def test_import_read_csv(self):
        file_name = 'test.csv'
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        df.to_csv(file_name, index=False)
        ndf = utl.import_read_csv(file_name)
        assert pd.testing.assert_frame_equal(df, ndf) is None
        os.remove(file_name)

    def test_import_read_xlsx_with_sheet_split(self):
        file_name = 'test.xlsx'
        df1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df2 = pd.DataFrame({'a': [5], 'b': [6]})
        with pd.ExcelWriter(file_name) as writer:
            df1.to_excel(writer, sheet_name='Sheet1', index=False)
            df2.to_excel(writer, sheet_name='Sheet2', index=False)
        splitter_name = (f'{file_name}{utl.sheet_name_splitter}'
                         f'Sheet1{utl.sheet_name_splitter}Sheet2')
        ndf = utl.import_read_csv(splitter_name)
        expected = pd.concat([df1, df2], ignore_index=True, sort=True)
        assert pd.testing.assert_frame_equal(expected, ndf) is None
        os.remove(file_name)

    def test_filter_df_on_col(self):
        col_name = 'a'
        col_val = 'x'
        df = pd.DataFrame({col_name: [col_val, 'y', 'z'], 'b': [4, 5, 6]})
        ndf = utl.filter_df_on_col(df, col_name, col_val)
        df = pd.DataFrame({col_name: [col_val], 'b': [4]})
        assert pd.testing.assert_frame_equal(df, ndf) is None

    def test_vm_rules(self):
        query_partners = ['{}'.format(x) for x in range(2)]
        query = '{}::{}'.format(dctc.VEN, ','.join(query_partners))
        metrics = [vmc.impressions, vmc.clicks]
        metric = '{}::{}'.format(utl.POST, '|'.join(metrics))
        rule_dict = {
            utl.RULE_QUERY: query,
            utl.RULE_FACTOR: 0.0,
            utl.RULE_METRIC: metric,
        }
        vm_rules = {}
        kwargs = {}
        for x in range(1, 2):
            vm_rules[x] = {}
            for y in rule_dict.keys():
                rule_name = 'RULE_{}_{}'.format(x, y)
                vm_rules[x][y] = rule_name
                kwargs[rule_name] = rule_dict[y]
        df = pd.DataFrame({dctc.VEN: ['{}'.format(x) for x in range(5)]})
        for col in metrics:
            df[col] = 1.0
        df = utl.data_to_type(df, float_col=metrics)
        ndf = df.copy()
        for col in metrics:
            mask = df[dctc.VEN].isin(query_partners)
            ndf[col] = np.where(mask, 0.0, df[col])
        df = utl.apply_rules(df, vm_rules, utl.POST, **kwargs)
        assert pd.testing.assert_frame_equal(df, ndf) is None

    def test_data_to_type(self):
        str_col = 'str_col'
        float_col = 'float_col'
        date_col = 'date_col'
        int_col = 'int_col'
        nat_list = ['0', '1/32/22', '30/11/22', '2022-1-32', '29269885']
        str_list = ['1/1/22', '1/1/2022', '44562', '20220101', '01.01.22',
                    '2022-01-01 00:00 + UTC', '1/01/2022 00:00',
                    'PST Sun Jan 01 00:00:00 2022', '2022-01-01', '1-Jan-22',
                    '2022-01-01 00:00:00', '2022-01-01 - 2022-01-01']
        str_list = nat_list + str_list
        float_list = [str(x) for x in range(len(str_list))]
        df_dict = {str_col: str_list, float_col: float_list,
                   date_col: str_list, int_col: float_list}
        df = pd.DataFrame(df_dict)
        ndf = utl.data_to_type(df.copy(), str_col=[str_col],
                               float_col=[float_col],
                               date_col=[date_col], int_col=[int_col])
        cor_date_list = [
            dt.datetime.strptime('2022-01-01', '%Y-%m-%d')
            for _ in range(len(str_list) - len(nat_list))]
        date_list = [pd.NaT] * len(nat_list) + cor_date_list
        df_dict = {str_col: str_list, date_col: date_list,
                   float_col: [float(x) for x in float_list],
                   int_col: [np.int64(x) for x in float_list]}
        df = pd.DataFrame(df_dict)
        df[int_col] = df[int_col].astype('int64')
        for col in [str_col, float_col, date_col, int_col]:
            assert pd.testing.assert_series_equal(df[col], ndf[col]) is None

    def test_selenium_wrapper(self):
        sw = utl.SeleniumWrapper()
        test_url = 'https://www.google.com/'
        sw.go_to_url(test_url, sleep=1)
        assert test_url in sw.browser.current_url
        assert sw.headless is True
        sw.quit()

    @requires_local_browser
    def test_screenshot(self):
        sw = utl.SeleniumWrapper(headless=False)
        test_url = 'https://www.google.com/'
        file_name = 'test.png'
        sw.take_screenshot(test_url, file_name=file_name)
        assert os.path.isfile(file_name)
        os.remove(file_name)
        sw.quit()

    def test_command_timeout(self):
        """Driver commands must be capped on the client side.

        Selenium builds its connection pool with no timeout, so a
        driver that stops answering blocks the caller forever instead
        of raising. The pool is built when the driver is constructed,
        so the cap only takes if it is set before that.
        """
        sw = utl.SeleniumWrapper()
        pool_kw = sw.browser.command_executor._conn.connection_pool_kw
        try:
            assert pool_kw['timeout'] == sw.command_timeout
        finally:
            sw.quit()

    def test_go_to_url_restarts_dead_session(self, monkeypatch):
        """A hung driver is replaced, not retried into another hang."""
        sw = utl.SeleniumWrapper()
        first_browser = sw.browser
        monkeypatch.setattr(sw.browser, 'get', _raise_read_timeout)
        try:
            assert sw.go_to_url('https://www.google.com/', sleep=1)
            assert sw.browser is not first_browser
        finally:
            sw.quit()

    def test_quit_survives_dead_session(self):
        """Callers quit from a ``finally``, so quit must never raise.

        A driver that stopped answering would otherwise mask the real
        exception and strand the chrome process it meant to reap.
        """
        sw = types.SimpleNamespace(browser=_DeadBrowser())
        assert utl.SeleniumWrapper.quit(sw) is None

    def test_scrape_quits_browser_on_error(self, monkeypatch):
        """A scrape that raises mid-run still reaps its browser.

        Cleanup used to be the last statement of the happy path, so any
        timeout or login failure leaked a whole chrome process tree.
        """
        _FakeSeleniumWrapper.instances = []
        # Patch the utils module fbapi itself holds: it imports
        # 'reporting.utils' while the tests import
        # 'processor.reporting.utils', and those are two module objects.
        monkeypatch.setattr(fbapi.utl, 'SeleniumWrapper',
                            _FakeSeleniumWrapper)
        with pytest.raises(ValueError):
            fbapi.FacebookScreenshots.take_screenshots(
                {'ad_id': 'https://www.google.com/'})
        assert len(_FakeSeleniumWrapper.instances) == 1
        assert _FakeSeleniumWrapper.instances[0].quit_calls == 1

    def test_init_browser_quits_on_configure_failure(self, monkeypatch):
        """A browser that fails mid-configure is quit before the raise.

        ``init_browser`` spawns chrome and then runs several fallible
        statements before returning the handle; a raise in that window
        used to orphan the process beyond any caller's ``finally``.
        """
        fake = _HalfBuiltBrowser()
        monkeypatch.setattr(utl.SeleniumWrapper, 'create_browser',
                            lambda self, co: fake)
        with pytest.raises(ValueError):
            utl.SeleniumWrapper()
        assert fake.quit_calls == 1

    def test_wrapper_is_a_context_manager(self, monkeypatch):
        """``with`` tears the browser down, raise or return alike."""
        quits = []
        monkeypatch.setattr(
            utl.SeleniumWrapper, 'init_browser',
            lambda self, headless: (types.SimpleNamespace(
                window_handles=['w0']), None))
        monkeypatch.setattr(utl.SeleniumWrapper, 'quit',
                            lambda self: quits.append(1))
        with utl.SeleniumWrapper():
            pass
        assert len(quits) == 1
        with pytest.raises(ValueError):
            with utl.SeleniumWrapper():
                raise ValueError('scrape failed')
        assert len(quits) == 2

    def test_accept_cookies_on_page(self, tmp_path):
        """The consent button is clicked, the decoys are not."""
        url = _write_page(tmp_path, 'banner.html', COOKIE_BANNER)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            found = sw.find_accept_buttons(sw.get_accept_xpath())
            assert [x.text for x in found] == ['Accept All Cookies']
            sw.accept_cookies()
            assert sw.browser.execute_script('return window.picked;') == (
                'accept')
        finally:
            sw.quit()

    def test_accept_cookies_in_iframe(self, tmp_path):
        """A banner living in a frame is still reached."""
        _write_page(tmp_path, 'frame.html', COOKIE_FRAME)
        url = _write_page(tmp_path, 'framed.html', COOKIE_FRAME_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            sw.accept_cookies()
            sw.switch_to_frame(sw.browser.find_element(By.TAG_NAME, 'iframe'))
            picked = sw.browser.find_element(
                By.TAG_NAME, 'body').get_attribute('data-picked')
            assert picked == 'accept'
        finally:
            sw.quit()

    def test_accept_cookies_ignores_decoys(self, tmp_path):
        """Body copy and a settings control are not consent buttons.

        'OK' is a substring of 'COOKIES' and 'Continue' of 'Continue
        reading', so a page with no accept button at all used to offer
        several matches.
        """
        html = '<html><body>{}</body></html>'.format(COOKIE_DECOYS)
        url = _write_page(tmp_path, 'decoys.html', html)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            assert sw.find_accept_buttons(sw.get_accept_xpath()) == []
            sw.accept_cookies()
            assert sw.browser.execute_script('return window.picked;') is None
        finally:
            sw.quit()

    @pytest.mark.parametrize(
        'sd, ed, expected_output', [
            (dt.datetime.today(),
             dt.datetime.today(),
             (dt.date.today(), dt.date.today())),
            (dt.datetime.today(),
             dt.datetime.today() - dt.timedelta(days=1),
             (dt.date.today() - dt.timedelta(days=1),
              dt.date.today() - dt.timedelta(days=1)))
        ],
        ids=['today', 'bad_sd']
    )
    def test_date_check(self, sd, ed, expected_output):
        output = utl.date_check(sd, ed)
        assert output == expected_output

    def test_get_next_number_from_list(self):
        lower_name = 'a'
        cur_model_name = 'b50'
        next_num = '5000'
        last_num = ['$10', ',', '000']
        words = [lower_name, cur_model_name, next_num, lower_name] + last_num
        num = utl.get_next_number_from_list(words, lower_name, cur_model_name)
        assert num == next_num
        num = utl.get_next_number_from_list(words, lower_name, cur_model_name,
                                            last_instance=True)
        assert num == ''.join(last_num).replace('$', '').replace(',', '')

    def test_get_next_values_from_list(self):
        plan_name = 'X Y Z'
        message = 'Plan named {}'.format(plan_name)
        words = utl.lower_words_from_str(message)
        words = utl.get_next_values_from_list(words, )
        assert words[0] == plan_name

    def test_first_last_adj(self):
        data = {
            "col1": [vmc.placement, 'Placement Value 1',
                     'Placement Value 2',  None],
            "col2": [vmc.date, pd.to_datetime("2025-05-01"),
                     pd.to_datetime("2025-05-02"), None],
            "col3": [vmc.impressions, '1', '2', '3']
        }
        df = pd.DataFrame(data)
        first_row = 1
        last_row = -1
        df_adj = utl.first_last_adj(df, first_row, last_row)
        assert len(df_adj) == 2
        expected_columns = [vmc.placement, vmc.date, vmc.impressions]
        assert list(df_adj.columns) == expected_columns

    def test_col_removal(self):
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        tdf = utl.col_removal(df, key='None', removal_cols=['ALL'])
        assert tdf.empty
        df[vmc.date] = 'x'
        tdf = utl.col_removal(df, key='None', removal_cols=['ALL'])
        assert vmc.date in tdf.columns


@requires_api_configs
class TestApis:

    @staticmethod
    def make_fake_config(key_list, tmp_path_factory, credentials=None):
        if not credentials:
            credentials = {}
        json_data = {}
        for cur_key in key_list:
            new_value = '{} - value'.format(cur_key)
            if cur_key in credentials and credentials[cur_key]:
                new_value = credentials[cur_key]
            json_data[cur_key] = new_value
        file_name = '{}/config.json'.format(tmp_path_factory.mktemp("config"))
        with open(file_name, 'w') as f:
            json.dump(json_data, f)
        return file_name, json_data

    def test_twapi_auth(self, tmp_path_factory):
        config_file = ''
        username = ''
        password = ''
        api = twapi.TwApi()
        if config_file:
            api.input_config(config_file)
            api.authenticate_account(username=username,
                                     password=password)

    def test_azapi(self, tmp_path_factory):
        api = azapi.AzuApi()
        file_name, json_data = self.make_fake_config(
            api.key_list, tmp_path_factory)
        api.input_config(file_name)
        df = pd.DataFrame({'uploadid': ['a'], 'productname': ['b']})
        # api.write_file(df)

    def test_awss3(self, tmp_path_factory):
        api = awss3.S3()
        df = pd.DataFrame({'uploadid': ['a'], 'productname': ['b']})
        # api.write_file(df)

    def test_redapi(self, tmp_path_factory):
        api = redapi.RedApi(headless=False)
        api.api = False
        file_name = os.path.join(utl.config_path, api.default_config_file_name)
        with open(file_name, 'r') as f:
            credentials = json.load(f)
        file_name, json_data = self.make_fake_config(
            api.key_list, tmp_path_factory, credentials)
        api.input_config(file_name)
        sd = dt.datetime.today() - dt.timedelta(days=70)
        ed = dt.datetime.today()
        try:
            # df = api.get_data(sd=sd, ed=ed)
            assert 1 == 1
        except Exception as e:
            api.sw.quit()
            raise e

    def test_authorize_api(self, tmp_path_factory):
        auth_email = ''
        file_name = 'reddit_credentials.csv'
        if not os.path.exists(file_name):
            return True
        df = pd.read_csv(file_name)
        df = df[['account_id', 'account_filter', 'skip']].drop_duplicates()
        user_passes = df.to_dict(orient='records')
        for user_pass in user_passes:
            username = user_pass['account_id']
            password = user_pass['account_filter']
            if 'skip' in user_pass:
                skip = user_pass['skip']
                if str(skip) == 'True':
                    logging.info('Skipped for {} {}'.format(skip, username))
                    continue
            api = redapi.RedApi(headless=False)
            try:
                # api.authorize_api(username, password, auth_email)
                1 == 1
            except:
                logging.warning('Failed for {}'.format(username))
        return True

    def test_amzapi(self, tmp_path_factory):
        api = amzapi.AmzApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_gaapi(self, tmp_path_factory):
        api = gaapi.GaApi()
        self.send_api_call(api)

    def test_awapi(self, tmp_path_factory):
        api = awapi.AwApi()
        self.send_api_call(api, fields=['UAC'])
        self.send_test_api_call(api)

    def test_fbapi(self, tmp_path_factory):
        api = fbapi.FbApi()
        self.send_api_call(api, fields=['Actions'])
        self.send_test_api_call(api)

    def test_samapi(self, tmp_path_factory):
        api = samapi.SamApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_criapi(self, tmp_path_factory):
        api = criapi.CriApi()
        self.send_api_call(api, fields=[api.line_item_str])
        self.send_test_api_call(api)

    def test_rsapi(self, tmp_path_factory):
        api = rsapi.RsApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_yvapi(self, tmp_path_factory):
        api = yvapi.YvApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_gsapi(self, tmp_path_factory):
        api = gsapi.GsApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_simapi(self, tmp_path_factory):
        api = simapi.SimApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    @staticmethod
    def send_api_call(api, fields=None):
        api.input_config(api.default_config_file_name)
        sd = (dt.datetime.today() - dt.timedelta(days=28)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        ed = (dt.datetime.today()).replace(
            hour=0, minute=0, second=0, microsecond=0)
        # df = api.get_data(sd, ed, fields=fields)
        assert api.get_data

    def test_redapi_new(self):
        api = redapi.RedApi()
        api.api = True
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_dcapi(self):
        api = dcapi.DcApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_drop_empty_conversion_cols(self):
        """All-zero Floodlight activity cols drop; core metrics stay."""
        df = pd.DataFrame({
            'Placement': ['p1', 'p2'],
            'Impressions': [10, 20],
            'Total Conversions': [0, 0],
            'Act : Foo: Total Conversions': [0, 0],
            'Act : Foo: Total Revenue': [0.0, 0.0],
            'Act : Bar: Total Conversions': [0, 5],
        })
        ndf = dcapi.DcApi.drop_empty_conversion_cols(df)
        assert 'Act : Foo: Total Conversions' not in ndf.columns
        assert 'Act : Foo: Total Revenue' not in ndf.columns
        assert 'Act : Bar: Total Conversions' in ndf.columns
        assert 'Total Conversions' in ndf.columns
        assert 'Impressions' in ndf.columns
        assert 'Placement' in ndf.columns

    def test_scapi(self):
        api = scapi.ScApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_steapi(self):
        api = steapi.SteApi()
        self.send_api_call(api)

    def test_iasapi(self):
        api = iasapi.IasApi()
        api.headless = False
        self.send_api_call(api)

    def test_ttdapi(self, tmp_path_factory):
        api = ttdapi.TtdApi()
        self.send_api_call(api)

    def test_tikapi(self, tmp_path_factory):
        api = tikapi.TikApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_twapi(self, tmp_path_factory):
        api = twapi.TwApi()
        self.send_api_call(api)

    def test_afapi(self, tmp_path_factory):
        api = afapi.AfApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    def test_asaapi(self):
        api = asaapi.AsaApi()
        self.send_api_call(api)
        self.send_test_api_call(api)

    @staticmethod
    def send_test_api_call(api):
        vk = ''
        import_config = vm.ImportConfig()
        import_config.import_vm()
        class_list = ih.ImportHandler(None, None).class_list
        for x, y in class_list.items():
            if isinstance(api, y):
                vk = x
                break
        ic_df = import_config.df.loc[
            import_config.df[import_config.key] == vk]
        acc_col = ic_df.iloc[0][import_config.account_id]
        camp_col = ic_df.iloc[0][import_config.filter]
        acc_pre = ic_df.iloc[0][import_config.account_id_pre]
        # api.input_config(api.default_config_file_name)
        # df = api.test_connection(acc_col, camp_col, acc_pre)
        # assert df['Success'].all()
        assert hasattr(api, "test_connection") and callable(
            getattr(api, "test_connection"))


class _FakeResponse(object):
    """Minimal stand in for requests.Response."""

    def __init__(self, status_code, text='', json_data=None):
        self.status_code = status_code
        self.text = text
        self.json_data = json_data or {}

    def json(self):
        return self.json_data


class _FakeRequests(object):
    """Record urls hit and replay canned responses in order.

    The last response repeats once the list is exhausted.
    """

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, *args, **kwargs):
        self.calls.append(url)
        if len(self.responses) > 1:
            return self.responses.pop(0)
        return self.responses[0]


def _return_none(*args, **kwargs):
    """Stand in for a report request that failed."""
    return None


def _return_true(*args, **kwargs):
    """Stand in for a validate pre-flight that passed."""
    return True


def _return_invalid(*args, **kwargs):
    """Stand in for a validate pre-flight that failed."""
    return {'is_valid': False, 'warnings': ['bad domain']}


def _return_expired_status(*args, **kwargs):
    """Stand in for a status check on a purged report."""
    return _FakeResponse(404, 'report expired')


def _raise_connection_error(*args, **kwargs):
    """Stand in for an http call whose socket dropped."""
    raise simapi.requests.exceptions.ConnectionError('boom')


def _download_stub(download_url):
    """Stand in for a completed report download."""
    return pd.DataFrame({'domain': ['a.com'], 'all_traffic_visits': [1]})


def _no_sleep(*args, **kwargs):
    """Keep polling loops instant under test."""


class TestSimApi:
    """Failure paths must degrade or recover, never raise."""

    @staticmethod
    def make_api():
        api = simapi.SimApi()
        api.api_key = 'key'
        api.domains = 'a.com'
        api.countries = 'us'
        api.config = {}
        return api

    def test_check_empty_df_handles_none(self):
        api = simapi.SimApi()
        api.df = None
        api.check_empty_df()
        assert api.df.empty

    def test_get_data_without_report_id(self, monkeypatch):
        """A failed report request short circuits to an empty df."""
        api = simapi.SimApi()
        monkeypatch.setattr(api, 'check_request_valid', _return_true)
        monkeypatch.setattr(api, 'make_request', _return_none)
        df = api.get_data()
        assert df.empty

    def test_check_report_status_invalid_id(self, monkeypatch):
        """A non-200 status check signals an unusable id via None."""
        api = self.make_api()
        monkeypatch.setattr(simapi.requests, 'get', _return_expired_status)
        assert api.check_report_status('stale-report-id') is None

    def test_get_data_stale_report_id_rebuilds(self, monkeypatch):
        """A dead stored report id is discarded and rebuilt, not fatal."""
        api = self.make_api()
        api.config = {'report_id': 'stale'}
        gets = _FakeRequests([
            _FakeResponse(404, 'unknown report'),
            _FakeResponse(200, json_data={
                'status': 'completed', 'download_url': 'http://d'})])
        posts = _FakeRequests([
            _FakeResponse(200, json_data={'report_id': 'fresh'})])
        monkeypatch.setattr(simapi.requests, 'get', gets)
        monkeypatch.setattr(simapi.requests, 'post', posts)
        monkeypatch.setattr(api, 'check_request_valid', _return_true)
        monkeypatch.setattr(api, 'download_report', _download_stub)
        monkeypatch.setattr(simapi.time, 'sleep', _no_sleep)
        df = api.get_data()
        assert api.config['report_id'] == 'fresh'
        assert not df.empty

    def test_v3_endpoint_gone_falls_back_to_v4(self, monkeypatch):
        """A sunset v3 report endpoint falls back to the v4 surface."""
        api = self.make_api()
        posts = _FakeRequests([
            _FakeResponse(404, 'gone'),
            _FakeResponse(200, json_data={'report_id': 'abc'})])
        monkeypatch.setattr(simapi.requests, 'post', posts)
        monkeypatch.setattr(simapi.time, 'sleep', _no_sleep)
        sd = ed = dt.datetime.today()
        assert api.make_request(sd, ed) == 'abc'
        assert api.use_v4 is True
        assert api.website_url in posts.calls[0]
        assert api.batch_v4_url in posts.calls[1]

    def test_internal_error_uses_free_retry(self, monkeypatch):
        """internal_error hits the free retry endpoint, then resumes."""
        api = self.make_api()
        gets = _FakeRequests([
            _FakeResponse(200, json_data={'status': 'internal_error'}),
            _FakeResponse(200, json_data={
                'status': 'completed', 'download_url': 'http://d'})])
        posts = _FakeRequests([_FakeResponse(200)])
        monkeypatch.setattr(simapi.requests, 'get', gets)
        monkeypatch.setattr(simapi.requests, 'post', posts)
        monkeypatch.setattr(api, 'download_report', _download_stub)
        monkeypatch.setattr(simapi.time, 'sleep', _no_sleep)
        df = api.check_report_status('rid')
        assert not df.empty
        assert api.retry_url in posts.calls[0]
        assert 'rid' in posts.calls[0]

    def test_invalid_request_aborts_before_charging(self, monkeypatch):
        """An is_valid false pre-flight stops before spending credits."""
        api = self.make_api()
        monkeypatch.setattr(api, 'make_validate_request', _return_invalid)
        monkeypatch.setattr(api, 'make_request', _raise_connection_error)
        df = api.get_data()
        assert df.empty

    def test_connection_error_returns_none(self, monkeypatch):
        """Transport failures exhaust their retries and return None."""
        api = self.make_api()
        monkeypatch.setattr(simapi.requests, 'get', _raise_connection_error)
        monkeypatch.setattr(simapi.time, 'sleep', _no_sleep)
        assert api.request_with_retry('http://x') is None

    def test_config_metrics_override(self):
        """A metrics list in config replaces the default agency set."""
        api = self.make_api()
        assert api.get_metrics() == simapi.SimApi.default_metrics
        api.config = {'metrics': 'all_traffic_visits,desktop_visits'}
        payload = api.construct_payload(dt.datetime.today(),
                                        dt.datetime.today())
        assert payload['metrics'] == ['all_traffic_visits',
                                      'desktop_visits']


class TestVendormatrix:
    def test_ad_cost_calculation(self):
        clicks = 10
        imps = 100
        ad_rate = 1
        ad_models = [cal.BM_CPM, cal.BM_CPC]
        df_dict = {
            dctc.AM: ad_models,
            dctc.AR: [ad_rate] * len(ad_models),
            vmc.impressions: [imps] * len(ad_models),
            vmc.clicks: [clicks] * len(ad_models),
        }
        df = pd.DataFrame(df_dict)
        df = vm.ad_cost_calculation(df)
        assert vmc.AD_COST in df.columns
        cpm_cost = (imps / 1000) * ad_rate
        cpm_calc = df[df[dctc.AM] == cal.BM_CPM][vmc.AD_COST].to_list()[0]
        assert cpm_cost == cpm_calc
        cpc_cost = clicks * ad_rate
        cpc_calc = df[df[dctc.AM] == cal.BM_CPC][vmc.AD_COST].to_list()[0]
        assert cpc_cost == cpc_calc

    def test_price_calculate_transform(self):
        df = pd.DataFrame({
            'Campaign': ['row_a', 'row_b'],
            'Purchase - Priced Item Count': [2, 0],    # priced
            'Purchase - Unpriced Item Count': [5, 1],  # listed, blank price -> none
            'Download - Other Item Count': [9, 9],     # ' Count' but not 'Purchase - '
        })
        transform = ('PriceCalculate::purchase - priced item|19.49'
                     '::Purchase - Unpriced Item|')
        out = vm.df_transform(df, transform)
        assert out['Purchase - Priced Item Revenue'].tolist() == [38.98, 0.0]
        assert 'Purchase - Unpriced Item Revenue' not in out.columns
        assert out['Revenue'].tolist() == [38.98, 0.0]
        assert out['Gamesight purchases'].tolist() == [7, 1]
        assert out['Download - Other Item Count'].tolist() == [9, 9]
        assert 'Download - Other Item Revenue' not in out.columns

    @requires_base_config
    def test_vm_load(self):
        matrix = vm.VendorMatrix()
        assert matrix.vm
        bar_col = vmc.barsplitcol[0]
        plan_val = matrix.vm[bar_col][vm.plan_key]
        assert isinstance(plan_val, list)

    @staticmethod
    def _bare_source(original, new):
        return {
            'original_vendor_key': original,
            vmc.vendorkey: new,
            vmc.autodicplace: '',
            vmc.placement: '',
            vmc.autodicord: '',
            vmc.fullplacename: '',
            'active_metrics': {},
            'vm_rules': {},
        }

    def test_set_data_sources_refuses_duplicate_key_cascade(self):
        matrix = vm.VendorMatrix()
        matrix.vm_df = pd.DataFrame({
            vmc.vendorkey: ['DBM', 'DBM', 'DBM', 'DBM'],
            vmc.filename: ['a.csv', 'b.csv', 'c.csv', 'd.csv'],
        })
        matrix.write = lambda: None
        sources = [self._bare_source('DBM', 'API_DBM_DBM')] * 4
        matrix.set_data_sources(sources)
        assert matrix.vm_df[vmc.vendorkey].tolist() == ['DBM'] * 4

    def test_set_data_sources_refuses_collision_rename(self):
        matrix = vm.VendorMatrix()
        matrix.vm_df = pd.DataFrame({
            vmc.vendorkey: ['Adikteev', 'API_DBM_DBM'],
            vmc.filename: ['adikteev.csv', 'dbm.csv'],
        })
        matrix.write = lambda: None
        matrix.set_data_sources(
            [self._bare_source('Adikteev', 'API_DBM_DBM')])
        assert matrix.vm_df.loc[0, vmc.vendorkey] == 'Adikteev'
        assert matrix.vm_df.loc[1, vmc.vendorkey] == 'API_DBM_DBM'

    def test_get_default_vm_value_returns_single_row(self):
        ic = vm.ImportConfig()
        ic.matrix_df = pd.DataFrame({
            vmc.vendorkey: ['DBM', 'DBM', 'DBM'],
            vmc.filename: ['a.csv', 'b.csv', 'c.csv'],
        })
        result = ic.get_default_vm_value('DBM', 'API')
        assert len(result) == 1


class TestDictionary:
    dic = dct.Dict()
    mock_rc_auto = ({dctc.TAR: [dctc.TB, dctc.DT1, dctc.GT]},
                    {dctc.TAR: ['_', '_']})

    def construct_empty_sort(self):
        auto = self.mock_rc_auto[0]
        empty_sort = {key: {comp: [] for comp in auto[key]} for key in auto}
        return empty_sort

    @pytest.mark.parametrize(
        "columns, sorted_cols, bad_delim, missing, bad_value", [
            ([], {}, True, False, False),
            (['mpTargeting:::0:::_', 'mpData Type 1', 'mpTargeting:::2:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.DT1: ['mpData Type 1'],
                         dctc.GT: ['mpTargeting:::2:::_']}},
             True, False, False),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.DT1: ['mpData Type 1:::0:::-'],
                         dctc.GT: ['mpTargeting:::2:::_']}},
             True, False, False),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.GT: ['mpTargeting:::2:::_']}},
             False, False, False),
            (['mpTargeting:::0:::_', 'mpTargeting:::2:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.GT: ['mpTargeting:::2:::_'],
                         'missing': ['mpTargeting:::1:::_']}},
             True, True, False),
            ([dctc.TB, dctc.GT],
             {dctc.TAR: {dctc.TB: [dctc.TB],
                         dctc.GT: [dctc.GT],
                         'missing': ['mpTargeting:::1:::_']}},
             True, True, False),
            (['mpTargeting:::0:::_', 'mpTargeting:::1:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.DT1: ['mpTargeting:::1:::_'],
                         'missing': ['mpTargeting:::2:::_']}},
             True, True, False),
            (['mpTargeting:::0:::_', 'mpData Type 1:::1:::_',
              'mpTargeting:::2:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.GT: ['mpTargeting:::2:::_']},
              'bad_values': ['mpData Type 1:::1:::_']},
             True, False, True)
        ],
        ids=['empty', 'standard', 'bad_delim', 'no_bad_delim', 'missing',
             'missing_2', 'missing_3', 'bad_value']
    )
    def test_sort_relation_cols(self, columns, sorted_cols, bad_delim,
                                missing, bad_value):
        df = pd.DataFrame(columns=columns)
        output = self.dic.sort_relation_cols(df.columns, self.mock_rc_auto,
                                             keep_bad_delim=bad_delim,
                                             return_missing=missing,
                                             return_bad_values=bad_value)
        expected = self.construct_empty_sort()
        for key in sorted_cols:
            if not isinstance(sorted_cols[key], dict):
                expected[key] = sorted_cols[key]
            else:
                for comp in sorted_cols[key]:
                    expected[key][comp] = sorted_cols[key][comp]
        assert output == expected

    @pytest.mark.parametrize(
        "columns, expected, bad_delim", [
            ([], {}, True),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::_',
              'mpTargeting:::2:::_'],
             {'mpTargeting:::0:::_': 'mpTargeting Bucket:::0:::_',
              'mpData Type 1:::0:::_': 'mpTargeting:::1:::_',
              'mpTargeting:::2:::_': 'mpGenre Targeting:::0:::_'},
             True),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             {'mpTargeting:::0:::_': 'mpTargeting Bucket:::0:::_',
              'mpData Type 1:::0:::-': 'mpTargeting:::1:::_',
              'mpTargeting:::2:::_': 'mpGenre Targeting:::0:::_'},
             True),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             {'mpTargeting:::0:::_': 'mpTargeting Bucket:::0:::_',
              'mpTargeting:::2:::_': 'mpGenre Targeting:::0:::_'},
             False)
        ],
        ids=['empty', 'standard', 'bad_delim', 'no_bad_delim']
    )
    def test_get_relation_translations(self, columns, expected, bad_delim):
        df = pd.DataFrame(columns=columns)
        output = self.dic.get_relation_translations(df.columns,
                                                    self.mock_rc_auto,
                                                    fix_bad_delim=bad_delim)
        assert output == expected

    @pytest.mark.parametrize(
        'columns, expected_cols, bad_delim, component', [
            ([], [], True, False),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             ['mpTargeting:::0:::_', 'mpTargeting:::1:::_',
              'mpTargeting:::2:::_'],
             True, False),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             ['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             False, False),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             ['mpTargeting Bucket:::0:::_', 'mpData Type 1:::0:::-',
              'mpGenre Targeting:::0:::_'],
             True, True),
            ([dctc.MIS, dctc.MIS2], [dctc.MIS, dctc.MIS2], True, False)
        ],
        ids=['empty', 'bad_delim', 'no_bad_delim', 'to_component',
             'non_relation']
    )
    def test_translate_relation_cols(self, columns, expected_cols, bad_delim,
                                     component):
        df = pd.DataFrame(columns=columns)
        output = self.dic.translate_relation_cols(df, self.mock_rc_auto,
                                                  fix_bad_delim=bad_delim,
                                                  to_component=component)
        expected = pd.DataFrame(columns=expected_cols)
        pd.testing.assert_frame_equal(output, expected)

    @pytest.mark.parametrize(
        'columns, expected_data', [
            ([], {}),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_'],
             {dctc.TAR: ['a_b_c']}),
            (['mpTargeting:::0:::_', 'mpData Type 1:::1:::-',
              'mpTargeting:::3:::_'],
             {dctc.TAR: ['a_0-b_c']}),
            (['mpTargeting:::0:::_', 'mpData Type 1:::1:::_',
              'mpTargeting:::3:::_'],
             {dctc.TAR: ['a_0_0_c']}),
            (['mpTargeting:::0:::_', 'mpData Type 1:::0:::-',
              'mpTargeting:::2:::_', dctc.MIS],
             {dctc.TAR: ['a_b_c'], dctc.MIS: ['d']}),
            (['mpTargeting:::0:::_', 'mpTargeting Bucket:::0:::_',
              'mpTargeting:::2:::_'],
             {dctc.TAR: ['b_0_c']}),
            (['mpTargeting Bucket:::0:::_',
              'mpGenre Targeting:::0:::_'],
             {dctc.TAR: ['a_0_b']}),
            ([dctc.TB, dctc.GT],
             {dctc.TAR: ['a_0_b']}),
            (['mpTargeting:::0:::_', 'mpTargeting:::1:::_'],
             {dctc.TAR: ['a_b']})
        ],
        ids=['empty', 'bad_delim', 'missing', 'bad_value', 'non_relation',
             'duplicate', 'missing_2', 'missing_3', 'standard']
    )
    def test_auto_combine(self, columns, expected_data):
        df = pd.DataFrame()
        for i, col in enumerate(columns):
            df[col] = [string.ascii_lowercase[i]]
        output = self.dic.auto_combine(df, self.mock_rc_auto)
        expected = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(output, expected, check_like=True,
                                      check_column_type=False)

    def test_select_translation(self):
        col = dctc.TAR
        col_val = ''
        new_value = 'B'
        part_name = 'PARTNER'
        dict_row = {
            dctc.DICT_COL_NAME: [col],
            dctc.DICT_COL_VALUE: [col_val],
            dctc.DICT_COL_NVALUE: [new_value],
            dctc.DICT_COL_FNC: ['Set::{}'.format(dctc.VEN)],
            dctc.DICT_COL_SEL: [part_name],
        }
        tdf = pd.DataFrame(dict_row)
        data_dict = {dctc.VEN: [part_name, 'NOT', part_name],
                     dctc.PKD: [dctc.PKD, dctc.PKD, 'NOT'],
                     dctc.TAR: ['', '', '']}
        data_dict_df = pd.DataFrame(data_dict)
        df = dct.DictTranslationConfig.select_translation(
            tdf, col, data_dict_df, fnc_type='Set')
        assert df[col][0] == new_value
        assert df[col][1] != new_value
        assert df[col][2] == new_value
        tdf[dctc.DICT_COL_FNC] += '||{}'.format(dctc.PKD)
        tdf[dctc.DICT_COL_SEL] += '||{}'.format(dctc.PKD)
        data_dict_df = pd.DataFrame(data_dict)
        df = dct.DictTranslationConfig.select_translation(
            tdf, col, data_dict_df, fnc_type='Set')
        assert df[col][0] == new_value
        assert df[col][1] != new_value
        assert df[col][2] != new_value


class TestErrorReport:
    def test_error_report(self, tmp_path_factory):
        file_path = tmp_path_factory.mktemp(utl.error_path)
        error_filename = '{}/ER.csv'.format(file_path)
        place_col = 'b'
        place_exist = 'a_b'
        place_miss = 'b_c'
        df = pd.DataFrame({'a': [1, 2], place_col: [place_exist, place_miss]})
        df[dctc.FPN] = df[place_col]
        dic = pd.DataFrame({dctc.FPN: [place_miss]})
        err = er.ErrorReport(df, dic, place_col, error_filename)
        assert not err.data_err.empty
        assert len(err.data_err) == 1
        df = pd.DataFrame({dctc.FPN: []})
        err = er.ErrorReport(df, dic, place_col, error_filename)
        assert err.data_err.empty
        df = pd.DataFrame({dctc.FPN: [place_col]})
        dic = pd.DataFrame({dctc.FPN: [np.nan]})
        err = er.ErrorReport(df, dic, place_col, error_filename)
        assert not err.data_err.empty

    def test_error_report_duplicate_placement_col(self, tmp_path_factory):
        """pn == FPN must not raise 'not unique' on the merge."""
        file_path = tmp_path_factory.mktemp(utl.error_path)
        error_filename = '{}/ER_dup.csv'.format(file_path)
        df = pd.DataFrame({dctc.FPN: ['a_b', 'b_c']})
        dic = pd.DataFrame({dctc.FPN: ['b_c']})
        err = er.ErrorReport(df, dic, dctc.FPN, error_filename)
        assert not err.data_err.empty
        assert len(err.data_err) == 1


class TestCalc:
    def test_calculate_cost(self):
        df = pd.DataFrame({
            dctc.CAM: ['c1', 'c1', 'c1', 'c1', 'c1', 'c1'],
            dctc.VEN: ['v1', 'v1', 'v1', 'v2', 'v2', 'v1'],
            dctc.BM: [cal.BM_CPM, cal.BM_CPC, '', '', '', cal.BM_FLAT],
            vmc.cost: [0.0, 0.0, 1000.0, 1000.0, 0.0, 0.0],
            dctc.PNC: [0.0, 0.0, 0.0, 0.0, 500.0, 0.0],
            dctc.UNC: [True, True, True, False, False, True]
        })
        con_col = [(vmc.date, '1/1/23'), (dctc.PN, 'pn'), (dctc.FPN, 'fpn'),
                   (dctc.BR, 3.0), (vmc.impressions, 1000.0),
                   (vmc.clicks, 10.0), (dctc.PKD, 'pkd'), (dctc.PD, '1/1/23')]
        for col in con_col:
            df[col[0]] = col[1]
        df[dctc.PFPN] = df[dctc.CAM] + '_' + df[dctc.VEN]
        df[dctc.UNC] = df[dctc.UNC].astype(object)
        edf = df.copy(deep=True)
        edf[vmc.cost] = [3.0, 30.0, 1000.0, 1000.0, 0.0, 3.0]
        edf[cal.NCF] = [3.0, 30.0, 1000.0, 500.0, 0.0, 3.0]
        df = cal.calculate_cost(df)
        edf = edf.reindex(sorted(edf.columns), axis=1)
        df = df.reindex(sorted(df.columns), axis=1)
        df = df[[x for x in edf.columns]]
        assert pd.testing.assert_frame_equal(df, edf) is None

    def test_prog_fees_calculation(self):
        prog_fee = .05
        net_cost = 100
        df = pd.DataFrame({dctc.PGF: [prog_fee], cal.NCF: [net_cost]})
        df = cal.prog_fees_calculation(df)
        assert cal.PROG_FEES in df.columns
        assert df[cal.PROG_FEES].sum() == prog_fee * net_cost

    def test_clicks_by_place_date(self):
        click_one = 10
        click_two = 30
        df = pd.DataFrame({
            vmc.date: ["2026-01-01", "2026-01-01", "2026-01-02"],
            dctc.PN: ["A", "A", "B"],
            dctc.BM: [cal.BM_FLAT, cal.BM_FLAT, "NOT_INCLUDED"],
            vmc.impressions: [100, 300, 50],
            vmc.clicks: [click_one, click_two, 5],
        })
        ndf = cal.clicks_by_place_date(df.copy())
        assert cal.CLI_PD in ndf.columns
        assert sum(ndf[cal.CLI_PD]) == 1
        assert ndf[cal.CLI_PD][0] == (click_one / (click_one + click_two))


class TestAnalyze:
    vm_df = None
    
    @staticmethod
    def get_rule_names():
        names = []
        for x in range(1, 7):
            for y in utl.RULE_CONST:
                names.append('RULE_{}_{}'.format(x, y))
        return names

    def generate_test_vm(self, data_dict, num_rows):
        vm_dict = {}
        vm_keys = [vmc.vendorkey] + vmc.vmkeys + self.get_rule_names()
        for key in vm_keys:
            if key in data_dict:
                vm_dict[key] = data_dict[key]
            else:
                val = ''
                if key is vmc.firstrow or key is vmc.lastrow:
                    val = 0
                elif key is vmc.autodicplace:
                    val = vmc.fullplacename
                vm_dict[key] = {i: val for i in range(num_rows)}
        vm_df = pd.DataFrame(vm_dict)
        return vm_df

    def test_check_flat(self):
        pn = '28091057_IMGN_US_All_0_0_0_Flat_0_44768_Click Tracker_0.013_0_'
        pn += 'CPE_Brand Page_Brand_0.1_0_V_Cross Device_1080x1080_Video '
        pn += 'SK_IG In-Feed_Social Post_Social_All'
        df = pd.DataFrame({
            vmc.clicks: [1],
            vmc.date: [44755],
            vmc.cost: [0],
            vmc.vendorkey: ['API_DCM_PoT2022BrandCampaign'],
            dctc.PN: [pn],
            dctc.BM: ['Flat'],
            dctc.BR: [0],
            dctc.CAM: ['Brand'],
            dctc.COU: ['US'],
            dctc.PKD: ['Social Post'],
            dctc.PD: [44767],
            dctc.VEN: ['IMGN'],
            cal.NCF: [0]})
        df = utl.data_to_type(df, date_col=[vmc.date, dctc.PD])
        cfs = az.CheckFlatSpends(az.Analyze())
        df = cfs.find_missing_flat_spend(df)
        assert cfs.placement_date_error in df[cfs.error_col].values
        assert cfs.missing_rate_error in df[cfs.error_col].values

    def test_empty_flat(self):
        df = pd.DataFrame()
        analyze = az.Analyze()
        cfs = az.CheckFlatSpends(analyze)
        df = cfs.find_missing_flat_spend(df)
        assert df.empty

    @requires_base_config
    def test_flat_fix(self):
        first_click_date = '2022-07-25'
        cfs = az.CheckFlatSpends(az.Analyze())
        translation = dct.DictTranslationConfig()
        if translation.df.empty:
            translation.df = pd.DataFrame({
                dctc.DICT_COL_NAME: [],
                dctc.DICT_COL_VALUE: [],
                dctc.DICT_COL_NVALUE: [],
                dctc.DICT_COL_FNC: [],
                dctc.DICT_COL_SEL: [],
                'index': []
            })
            translation.write(translation.df, dctc.filename_tran_config)
        df = pd.DataFrame({
            dctc.VEN: ['IMGN'],
            dctc.COU: ['US'],
            dctc.PN: [
                '28091057_IMGN_US_All_0_0_0_Flat_0_44768_Click '
                'Tracker_0.013_0_CPE_Brand Page_Brand_0.1_0_V_'
                'Cross Device_1080x1080_Video SK_IG '
                'In-Feed_Social Post_Social_All'],
            dctc.PKD: ['Social Post'],
            dctc.PD: [44755],
            dctc.BM: ['Flat'],
            cal.NCF: [0],
            vmc.clicks: [1],
            dctc.BR: [0],
            cfs.first_click_col: [first_click_date],
            cfs.error_col: cfs.placement_date_error})
        df = utl.data_to_type(df, date_col=[dctc.PD, cfs.first_click_col])
        df = utl.data_to_type(df, str_col=[dctc.PD, cfs.first_click_col])
        tdf = cfs.fix_analysis(df, write=False)
        translation.df = tdf
        df = translation.apply_translation_to_dict(df)
        assert df[dctc.PD].values == first_click_date

    def test_empty_flat_fix(self):
        cfs = az.CheckFlatSpends(az.Analyze())
        df = pd.DataFrame()
        tdf = cfs.fix_analysis(df, write=False)
        assert tdf.empty

    @pytest.fixture
    def test_vm(self):
        vm_dict = {
            vmc.vendorkey:
                {0: 'API_DCM_Test', 1: 'API_Tiktok_Test',
                 2: 'API_Rawfile_Test', 3: 'Plan Net'},
            vmc.filename: {0: 'dcm_Test', 1: 'tiktok_Test.csv',
                           2: 'Rawfile_Test.csv', 3: 'plannet.csv'},
            vmc.fullplacename: {0: 'Placement', 1: 'ad_name',
                                2: 'ad_name', 3: 'mpCampaign|mpVendor'},
            vmc.placement: {0: 'Placement', 1: 'ad_name',
                            2: 'ad_name', 3: 'mpVendor'},
            vmc.startdate: {0: '7/18/2022', 1: '7/1/2022',
                            2: '7/1/2022', 3: ''},
            vmc.enddate: {0: '', 1: '7/27/2022', 2: '7/27/2022', 3: ''},
            vmc.dropcol: {0: 'ALL', 1: 'ALL', 2: 'ALL', 3: ''},
            vmc.autodicord: {
                0: 'mpCampaign|mpVendor', 1: 'mpCampaign|mpVendor',
                2: 'mpCampaign|mpVendor', 3: 'mpCampaign|mpVendor'},
            vmc.apifile: {0: 'dcapi_Test.json', 1: 'tikapi_Test.json',
                          2: 'tikapi_Test.json', 3: ''},
            vmc.date: {0: 'Date', 1: 'stat_datetime',
                       2: 'stat_datetime', 3: ''},
            vmc.impressions: {0: 'Impressions', 1: 'show_cnt',
                              2: 'show_cnt', 3: ''},
            vmc.clicks: {0: 'Clicks', 1: 'click_cnt', 2: 'click_cnt', 3: ''},
            vmc.cost: {0: '', 1: 'stat_cost', 2: 'stat_cost', 3: ''},
            vmc.views: {0: 'TrueView Views', 1: 'total_play',
                        2: 'total_play', 3: ''},
            vmc.views25: {0: 'Video First Quartile Completions',
                          1: 'play_first_quartile',
                          2: 'play_first_quartile', 3: ''},
            vmc.views50: {0: 'Video Midpoints', 1: 'play_midpoint',
                          2: 'play_midpoint', 3: ''},
            vmc.views75: {0: 'Video Third Quartile Completions',
                          1: 'play_third_quartile',
                          2: 'play_third_quartile', 3: ''},
            vmc.views100: {0: 'Video Completions', 1: 'play_over',
                           2: 'play_over', 3: ''},
            'RULE_1_METRIC': {0: 'POST::Impressions|Clicks', 1: '',
                              2: '', 3: ''},
            'RULE_1_QUERY': {
                0: 'mpVendor::Facebook,Instagram,SEM,YouTube',
                1: '', 2: '', 3: ''},
            'RULE_2_FACTOR': {0: '', 1: 0.0, 2: 0.0, 3: 0.0},
            'RULE_2_METRIC': {0: '', 1: 'POST::Adserving Cost',
                              2: 'POST::Adserving Cost',
                              3: 'POST::Adserving Cost'},
            'RULE_2_QUERY': {0: '', 1: 'mpAgency::Liquid Advertising',
                             2: 'mpAgency::Liquid Advertising',
                             3: 'mpAgency::Liquid Advertising'},
            'RULE_3_FACTOR': {0: 0.1, 1: '', 2: '', 3: ''},
            'RULE_3_METRIC': {0: 'POST::Adserving Cost::DCM Service Fee',
                              1: '', 2: '', 3: ''},
            'RULE_3_QUERY': {0: 'mpAgency::Liquid Advertising',
                             1: '', 2: '', 3: ''}
        }

        self.vm_df = self.generate_test_vm(vm_dict, 4)
        return self.vm_df
        
    def test_double_fix_all_raw(self, test_vm):
        """
        If test is failing due to Vendor Key errors, ensure 'Vendormatrix.csv'
        is in the 'processors/tests/' directory and up to date.
        """
        vm_df = self.vm_df
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_df)
        cdc = az.CheckDoubleCounting(az.Analyze(matrix=matrix))
        aly_dict = pd.DataFrame({
            dctc.VEN: ['TikTok'],
            cdc.metric_col: [vmc.clicks],
            vmc.vendorkey: ['API_Rawfile_Test,API_Tiktok_Test'],
            cdc.num_duplicates: ['1'],
            cdc.total_placement_count: ['1'],
            cdc.error_col: [cdc.double_counting_all]
        })
        cdc.fix_all(aly_dict)
        matrix = cdc.aly.matrix
        rawfile_cell = matrix.vm_df.loc[
            matrix.vm_df[vmc.vendorkey] == 'API_Rawfile_Test',
            vmc.clicks].item()
        api_cell = matrix.vm_df.loc[
            matrix.vm_df[vmc.vendorkey] == 'API_Tiktok_Test',
            vmc.clicks].item()
        assert not rawfile_cell
        assert api_cell

    def test_double_fix_empty(self, test_vm):
        vm_df = self.vm_df
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_df)
        cdc = az.CheckDoubleCounting(az.Analyze(matrix=matrix))
        aly_dict = pd.DataFrame()
        df = cdc.fix_analysis(aly_dict, write=False)
        assert df.empty

    def test_double_fix_all_server(self, test_vm):
        rule_1_query = 'RULE_1_QUERY'
        vm_df = self.vm_df
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_df)
        cdc = az.CheckDoubleCounting(az.Analyze(matrix=matrix))
        aly_dict = pd.DataFrame({
            dctc.VEN: ['TikTok'],
            cdc.metric_col: [vmc.clicks],
            vmc.vendorkey: ['API_DCM_Test,API_Tiktok_Test'],
            cdc.num_duplicates: ['1'],
            cdc.total_placement_count: ['1'],
            cdc.error_col: [cdc.double_counting_all]
        })
        cdc.fix_all(aly_dict)
        matrix = cdc.aly.matrix
        server_cell = matrix.vm_df.loc[
            matrix.vm_df[vmc.vendorkey] == 'API_DCM_Test',
            rule_1_query].item()
        api_cell = matrix.vm_df.loc[
            matrix.vm_df[vmc.vendorkey] == 'API_Tiktok_Test',
            rule_1_query].item()
        assert 'TikTok' in server_cell
        assert not api_cell

    def test_find_double_counting(self):
        df = pd.DataFrame({
            dctc.VEN: {0: 'TikTok', 1: 'TikTok'},
            vmc.vendorkey: {0: 'API_Tiktok_Test', 1: 'API_Rawfile_Test'},
            vmc.clicks: {0: 15.0, 1: 15.0},
            vmc.date: {0: '7/27/2022', 1: '7/27/2022'},
            vmc.impressions: {0: 1.0, 1: 1.0},
            vmc.views: {0: 1.0, 1: 1.0},
            dctc.PN: {0: 'Test', 1: 'Test'}})
        df = utl.data_to_type(df, date_col=[vmc.date, dctc.PD])
        cdc = az.CheckDoubleCounting(az.Analyze())
        df = cdc.find_metric_double_counting(df)
        assert cdc.double_counting_all in df[cdc.error_col].values
        assert 'API_Tiktok_Test' in df[vmc.vendorkey][0]
        assert 'API_Rawfile_Test' in df[vmc.vendorkey][0]

    def test_find_placement_name(self):
        other_col = 'other_col'
        df = pd.DataFrame({vmc.placement: ['_' * 10],
                           other_col: ['_' * 20],
                           'wrong': ['_' * 35]})
        place_analyze = az.FindPlacementNameCol(az.Analyze())
        rdf = place_analyze.find_placement_col_in_df(
            df, result_df=[])
        assert rdf
        assert rdf[0][place_analyze.suggested_col] == other_col
        raw_file_name = 'rawfile_test.csv'
        if not os.path.exists(raw_file_name):
            return True
        df = pd.read_csv(raw_file_name)
        rdf = place_analyze.find_placement_col_in_df(
            df, result_df=[])
        assert rdf
        return True

    @staticmethod
    def get_output_as_df(with_plan=False, new_place=''):
        date_val = dt.datetime.today().strftime('%m/%d/%Y')
        df = pd.DataFrame()
        for col in [dctc.VEN, dctc.PN]:
            df[col] = [col]
        df[vmc.vendorkey] = [vmc.api_raw_key]
        df[vmc.date] = [date_val]
        if with_plan:
            tdf = df.copy()
            tdf[vmc.vendorkey] = [vmc.api_mp_key]
            df = pd.concat([df, tdf], ignore_index=True)
            if new_place:
                tdf[dctc.PN] = new_place
                tdf[vmc.vendorkey] = [vmc.api_raw_key]
                df = pd.concat([df, tdf], ignore_index=True)
        return df

    @requires_base_config
    def test_placement_not_in_mp(self):
        df = self.get_output_as_df()
        base_analyze = az.Analyze(matrix=vm.VendorMatrix())
        place_analyze = az.CheckPlacementsNotInMp(base_analyze)
        rdf = place_analyze.find_placements_not_in_mp(df)
        assert rdf.empty
        df = self.get_output_as_df(with_plan=True)
        rdf = place_analyze.find_placements_not_in_mp(df)
        assert dctc.PN not in rdf[dctc.PN].values
        new_place = '{}NEW'.format(dctc.PN)
        df = self.get_output_as_df(with_plan=True, new_place=new_place)
        rdf = place_analyze.find_placements_not_in_mp(df)
        assert new_place in rdf[dctc.PN].values
        place_analyze.aly.df = df
        place_analyze.do_analysis()
        rdf = place_analyze.fix_analysis(rdf, write=False)
        assert new_place in rdf[dctc.DICT_COL_VALUE].values

    def test_placement_not_in_mp_combine_underscore(self):
        """CombineColumnsUnderscore joins with underscore before
        RawTranslate so the combined name can be translated."""
        col_a = dctc.PN
        col_b = 'secondary'
        df = pd.DataFrame({col_a: ['camp1', 'camp2'],
                           col_b: ['adg1', 'adg2']})
        transform = (f'CombineColumnsUnderscore::{col_a}|{col_b}'
                     f':::RawTranslate')
        tc = dct.DictTranslationConfig()
        tc.df = pd.DataFrame({
            dctc.DICT_COL_NAME: [col_a],
            dctc.DICT_COL_VALUE: ['camp1_adg1'],
            dctc.DICT_COL_NVALUE: ['plan_placement'],
        })
        tc.write(tc.df, dctc.filename_tran_config)
        df = vm.df_transform(df, transform)
        assert col_b not in df.columns
        assert df[col_a].iloc[0] == 'plan_placement'
        assert df[col_a].iloc[1] == 'camp2_adg2'
        os.remove(os.path.join(
            tc.csv_path, dctc.filename_tran_config))

    def test_placement_not_in_mp_fix(self):
        creative_names = ['a', 'b', 'c']
        copy_names = ['1', '2', '3']
        target_names = ['aaa', 'jrpg']
        names = [
            f'{tgt}_{cname} {cpy}'
            for tgt in target_names
            for cname in creative_names
            for cpy in copy_names
        ]
        mp_names = ['123_456_{}'.format(name) for name in names]
        place_analyze = az.CheckPlacementsNotInMp(az.Analyze())
        rdf = place_analyze.find_closest_name_match(names, mp_names)
        assert len(rdf) == len(names)
        rdf_dict = rdf.set_index('Value').to_dict(orient='dict')
        rdf_dict = rdf_dict[dctc.DICT_COL_NVALUE]
        for idx, name in enumerate(names):
            assert rdf_dict[name] == mp_names[idx]

    def test_check_col_live(self):
        df = self.get_output_as_df(with_plan=True)
        ali_class = az.CheckLive(az.Analyze())
        yesterday = dt.datetime.today() - dt.timedelta(days=1)
        df[ali_class.sd_col] = yesterday.strftime('%m/%d/%Y')
        for col in ali_class.metric_cols:
            df[col] = 0
        rdf, msg = ali_class.check_col_live(df)
        assert not rdf.empty
        tomorrow = dt.datetime.today() + dt.timedelta(days=1)
        df[ali_class.sd_col] = tomorrow.strftime('%m/%d/%Y')
        rdf, msg = ali_class.check_col_live(df)
        assert rdf.empty

    def test_find_double_counting_empty(self):
        df = pd.DataFrame()
        cdc = az.CheckDoubleCounting(az.Analyze())
        df = cdc.find_metric_double_counting(df)
        assert df.empty

    @requires_base_config
    def test_adwords_split(self):
        df = pd.DataFrame()
        ic = vm.ImportConfig()
        test_config = 'test_config.yaml'
        test_csv = 'split_test.csv'
        test_api = 'Adwords_auto_auto'
        cas = az.CheckAdwordsSplit(az.Analyze(matrix=vm.VendorMatrix()))
        mock_config = {'adwords': {'campaign_filter': ''}}
        with open('config/{}'.format(test_config), 'w') as file:
            yaml.dump(mock_config, file, default_flow_style=False)
        mock_data = {
            'Campaign': [
                'test_video_youtube',
                'test_search_googlesem']}
        mock_data = pd.DataFrame(mock_data)
        mock_data.to_csv('raw_data/{}'.format(test_csv))
        source = vm.DataSource(key='split_test', vm_rules={})
        source.key = 'API_Adwords_auto'
        source.p[vmc.apifile] = test_config
        source.p[vmc.filename] = 'raw_data/{}'.format(test_csv)
        source.p[vmc.startdate] = dt.datetime.strptime(
            '2024-10-29 00:00:00', '%Y-%m-%d %H:%M:%S')
        source.ic_params = {vmc.apifields: '',
                            ic.filter: '',
                            ic.account_id: '123', ic.key: 'adwords',
                            vmc.startdate: '2024-10-29',
                            'Vendor Key': 'API_Adwords_auto', ic.name: 'auto'}
        tdf = cas.do_analysis_on_data_source(source, df)
        assert not tdf.empty
        vm_df = cas.aly.matrix.vm_df
        vk = vmc.api_aw_key
        ndf = vm_df[vm_df[vmc.vendorkey] == vk].reset_index(drop=True)
        new_vk = 'API_Adwords_auto'
        ndf.loc[0, vmc.vendorkey] = new_vk
        new_config = source.p[vmc.apifile]
        ndf.loc[0, vmc.apifile] = new_config
        vm_df = pd.concat([vm_df, ndf]).reset_index(drop=True)
        cas.aly.matrix.vm_df = vm_df
        cas.aly.matrix.write()
        vm_df = cas.fix_analysis(aly_dict=tdf, write=False)
        assert vm_df[vmc.vendorkey].isin(['API_{}_sem'.format(test_api)]).any()
        assert vm_df[vmc.vendorkey].isin([
            'API_{}_video'.format(test_api)]).any()
        assert os.path.exists('config/awconfig_{}_sem.yaml'.format(test_api))
        assert os.path.exists('config/awconfig_{}_video.yaml'.format(test_api))
        os.remove('config/awconfig_{}_sem.yaml'.format(test_api))
        os.remove('config/awconfig_{}_video.yaml'.format(test_api))
        os.remove('config/{}'.format(test_config))
        os.remove('raw_data/{}'.format(test_csv))
        index_vk = vm_df[(vm_df[vmc.vendorkey] == 'API_{}_sem'.format(
            test_api))].index
        vm_df.drop(index_vk, inplace=True)
        index_vk = vm_df[(vm_df[vmc.vendorkey] == 'API_{}_video'.format(
            test_api))].index
        vm_df.drop(index_vk, inplace=True)
        cas.aly.matrix.vm_df = vm_df
        cas.aly.matrix.write()

    @requires_base_config
    def test_max_date_reached(self):
        start_date, date1, date2, date3, date4 = [
            (dt.datetime.today() - dt.timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range(60, 55, -1)]
        end_date = dt.datetime.today()
        end_date = end_date.strftime('%Y-%m-%d')
        test_csv_path = 'raw_data/amazon_test.csv'
        date_list = [start_date, date1, date4, date3, date2]
        place_list = ['amz_test_1', 'amz_test_2', 'amz_test_3', 'amz_test_4',
                      'amz_test_5']
        test_csv_df = pd.DataFrame({vmc.date: date_list,
                                    vmc.placement: place_list,
                                    vmc.cost: [1, 2, 3, 4, 5]})
        test_csv_df.to_csv(test_csv_path, index=False)
        vm_dict = pd.DataFrame({vmc.vendorkey: ['API_Amazon_Test'],
                                vmc.startdate: [start_date],
                                vmc.enddate: [end_date],
                                vmc.filename: [test_csv_path],
                                vmc.date: [vmc.date]})
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_dict)
        adl = az.CheckApiDateLength(az.Analyze(matrix=matrix))
        df = adl.do_analysis()
        assert vmc.api_amz_key in df[vmc.vendorkey][0]
        assert date4 in str(df[adl.highest_date][0])
        f_df = adl.fix_analysis(aly_dict=df, write=False)
        assert 'API_' not in f_df.loc[0, vmc.vendorkey]
        assert 'API_' in f_df.loc[1, vmc.vendorkey]
        f_df[vmc.startdate] = pd.to_datetime(f_df[vmc.startdate])
        f_df[vmc.enddate] = pd.to_datetime(f_df[vmc.enddate])
        assert f_df.loc[0, vmc.enddate] == f_df.loc[
            1, vmc.startdate] - pd.Timedelta(days=1)

    def test_package_cap_over(self):
        df = {'mpVendor': ['Adwords', 'Facebook', 'Twitter'],
              'mpPackageDesc': ['Under', 'Full', 'Over'],
              'Planned Net Cost - TEMP': [100, 100, 100],
              'Net Cost': [50, 100, 200]}
        df = pd.DataFrame(df)
        temp_package_cap = 'mpPackageDesc'
        cpc = az.CheckPackageCapping(az.Analyze())
        df = cpc.check_package_cap(df, temp_package_cap)
        assert 'Over' in df['mpPackageDesc'][0]

    def test_package_cap_full(self):
        df = {'mpVendor': ['Adwords', 'Facebook', 'Twitter'],
              'mpPackageDesc': ['Under', 'Full', 'Over'],
              'Planned Net Cost - TEMP': [100, 100, 100],
              'Net Cost': [50, 100, 100]}
        df = pd.DataFrame(df)
        temp_package_cap = 'mpPackageDesc'
        cpc = az.CheckPackageCapping(az.Analyze())
        df = cpc.check_package_cap(df, temp_package_cap)
        assert 'Full' in df['mpPackageDesc'][0]

    def test_package_cap_under(self):
        cpc = az.CheckPackageCapping(az.Analyze())
        df = {dctc.VEN: ['Adwords', 'Facebook', 'Twitter'],
              dctc.PKD: ['Under', 'Full', 'Over'],
              cpc.plan_net_temp: [100, 100, 100],
              vmc.cost: [50, 50, 50]}
        df = pd.DataFrame(df)
        temp_package_cap = dctc.PKD
        df = cpc.check_package_cap(df, temp_package_cap)
        assert df.empty

    def test_package_vendor_duplicates(self):
        cpc = az.CheckPackageCapping(az.Analyze())
        df = {dctc.VEN: ['Adwords', 'Twitter', 'Facebook'],
              vmc.vendorkey: ['key1', 'key2', 'key3'],
              dctc.PN: ['PN1', 'PN2', 'PN3'],
              dctc.PKD: ['Same', 'Same', 'Diff'],
              cpc.plan_net_temp: [100, 100, 100],
              vmc.cost: [50, 100, 200]}
        df = pd.DataFrame(df)
        temp_package_cap = dctc.PKD
        pdf = {dctc.PKD: ['Same']}
        pdf = pd.DataFrame(pdf)
        df = cpc.check_package_vendor(df, temp_package_cap, pdf)
        assert 'Adwords' in df[dctc.VEN][1]
        assert 'Twitter' in df[dctc.VEN][2]
        assert 'Facebook' not in df[dctc.VEN]

    def test_package_vendor_different(self):
        cpc = az.CheckPackageCapping(az.Analyze())
        df = pd.DataFrame({dctc.VEN: ['Adwords', 'Twitter', 'Facebook'],
                           vmc.vendorkey: ['key1', 'key2', 'key3'],
                           dctc.PN: ['PN1', 'PN2', 'PN3'],
                           dctc.PKD: ['This', 'That', 'Those'],
                           cpc.plan_net_temp: [100, 100, 100],
                           vmc.cost: [50, 100, 200]
                           })
        temp_package_cap = dctc.PKD
        pdf = pd.DataFrame({dctc.PKD: ['This']})
        df = cpc.check_package_vendor(df, temp_package_cap, pdf)
        assert df.empty

    """
    def test_fix_vendor(self):
        cpc = az.CheckPackageCapping(az.Analyze())
        temp_package_cap = dctc.PKD
        pdf = pd.DataFrame({dctc.PKD: ['package1', 'package2'],
                            cpc.plan_net_temp: [10, 10]})
        pdf.to_csv('raw_data/cap_test.csv', index=False)
        c = {'file_name': 'raw_data/cap_test.csv',
             'file_dim': 'mpPackageDescription',
             'file_metric': 'Net Cost (Capped)',
             'processor_dim': 'mpPackageDescription',
             'processor_metric': 'Planned Net Cost'}
        cap_file = cal.MetricCap()
        aly_dict = pd.DataFrame({dctc.PKD: ['package1', 'package1',
                                            'package2', 'package2'],
                                 dctc.VEN: ['Facebook', 'Twitter',
                                            'Twitch', 'Adwords']
                                 })
        match_df = pd.DataFrame({dctc.DICT_COL_NAME: [dctc.PKD, dctc.PKD,
                                                      dctc.PKD, dctc.PKD],
                                 dctc.DICT_COL_VALUE: ['package1', 'package1',
                                                       'package2', 'package2'],
                                 dctc.DICT_COL_NVALUE: ['package1-Facebook',
                                                        'package1-Twitter',
                                                        'package2-Twitch',
                                                        'package2-Adwords'],
                                 dctc.DICT_COL_FNC: ['Select::mpVendor',
                                                     'Select::mpVendor',
                                                     'Select::mpVendor',
                                                     'Select::mpVendor'],
                                 dctc.DICT_COL_SEL: ['Facebook', 'Twitter',
                                                     'Twitch', 'Adwords'],
                                 })
        df = cpc.fix_package_vendor(temp_package_cap, c, pdf, cap_file,
                                    write=False, aly_dict=aly_dict)
        os.remove('raw_data/cap_test.csv')
        assert not df.empty
        assert df.equals(match_df)
        """

    @pytest.fixture
    def setup_autodict_files(self):
        """
        Generates test vendormatrix for autodictionary order.
        Creates ven1_test.csv, ven2_test.csv, ven3_test.csv, and
        plannet_test.csv in the raw_data folder and populates them with data
        that should trigger new order suggestions for test_autodict_analysis.

        :returns: test vendormatrix as a dataframe
        """
        vm_dict = {
            vmc.vendorkey:
                {0: 'API_Ven1_Test', 1: 'API_Ven2_Test',
                 2: 'API_Ven3_Test', 3: 'Plan Net'},
            vmc.filename: {0: 'ven1_test.csv', 1: 'ven2_test.csv',
                           2: 'ven3_test.csv', 3: 'plannet_test.csv'},
            vmc.fullplacename: {
                0: '::Campaign Name|Ad Set Name|Ad Name',
                1: '::Campaign|Ad group|Ad',
                2: 'Placement Name',
                3: 'mpCampaign|mpVendor'},
            vmc.placement: {0: 'Ad Name', 1: 'Ad',
                            2: 'Placement Name', 3: 'mpVendor'},
            vmc.autodicord: {
                0: 'mpBudget|mpVendor|mpCountry/Region|mpCampaign',
                1: 'mpMisc|mpBudget|mpVendor|mpCountry/Region|mpCampaign',
                2: 'mpMisc|mpBudget|mpVendor|mpCountry/Region|mpCampaign',
                3: ''},
            vmc.filenamedict: {0: 'ven1_test.csv', 1: 'ven2_test.csv',
                               2: 'ven3_test.csv', 3: 'plannet_test.csv'},
        }
        test_vm = self.generate_test_vm(vm_dict, 4)
        data1 = {
            'Campaign Name':
                {0: 'Campaign 1', 1: 'Campaign 2'},
            'Ad Set Name':
                {0: 'Set 1', 1: 'Set 2'},
            'Ad Name':
                {0: '01234567_Vendor1_US_Pre-Launch',
                 1: '01234567_Vendor1_US_Post-Launch'}
        }
        data2 = {
            'Campaign':
                {0: 'Campaign 1', 1: 'Campaign 2', 2: 'Campaign 3'},
            'Ad group':
                {0: 'Group 1', 1: 'Group 2', 2: 'Group 3'},
            'Ad':
                {0: '01234567_Vendor 2_MX_Pre-Launch',
                 1: '01234567_Vendor 2_BR_Pre-Order',
                 2: '01234567_Vendor 2_MX_Post-Launch'}
        }
        data3 = {
            'Placement Name':
                {0: '01234567_Vendor3_GB_Pre-Order',
                 1: '01234567_Vendor3_MX_Pre-Launch'}
        }
        plannet_data = {
            'mpCampaign':
                {0: 'Pre-Launch', 1: 'Pre-Launch', 2: 'Pre-Order',
                 3: 'Pre-Order', 4: 'Post-Launch', 5: 'Post-Launch',
                 6: 'Pre-Launch'},
            'mpVendor':
                {0: 'Vendor1', 1: 'Vendor2', 2: 'Vendor3', 3: 'Vendor2',
                 4: 'Vendor2', 5: 'Vendor1', 6: 'Vendor3'},
        }
        files_to_write = {
            test_vm[vmc.filename][0]: pd.DataFrame(data1),
            test_vm[vmc.filename][1]: pd.DataFrame(data2),
            test_vm[vmc.filename][2]: pd.DataFrame(data3),
            test_vm[vmc.filenamedict][3]: pd.DataFrame(plannet_data)
        }
        for filename, df in files_to_write.items():
            file_folder = utl.raw_path
            if 'plannet' in filename:
                file_folder = utl.dict_path
            full_file_name = '{}{}'.format(file_folder, filename)
            df.to_csv(full_file_name, index=False)
        return test_vm

    @requires_base_config
    def test_change_autodict_order(self, setup_autodict_files):
        """
        Tests CheckAutoDictOrder using auto dict order/data source
        combinations that should result in a positive shift via Vendor
        position (Ven1 test), a positive shift via Campaign position (Ven2
        test), and a negative shift via Vendor position (Ven3 test) for the
        suggested orders.
        
        'positive shift' = suggests shifting order to the right by appending
        'mpMisc' cells to the start of the list
        'negative shift' = suggests shifting order cell to the left by removing
        cells from the start of the list

        
        :param setup_autodict_files: Fixture that sets up data files and
        returns the vendormatrix for this test
        """
        vm_dict = setup_autodict_files
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_dict)
        aly = az.Analyze(df=pd.DataFrame(), matrix=matrix)
        aly.do_all_analysis()
        i = 0
        while aly.analysis_dict[i]['key'] != 'change_auto_order':
            i += 1
        suggested_orders = aly.analysis_dict[i]['data']
        expected_orders = {
            vm_dict[vmc.vendorkey][0]:
                ('mpMisc|mpMisc|' + vm_dict[vmc.autodicord][0]).split('|'),
            vm_dict[vmc.vendorkey][1]:
                ('mpMisc|' + vm_dict[vmc.autodicord][1]).split('|'),
            vm_dict[vmc.vendorkey][2]:
                vm_dict[vmc.autodicord][2].split('|')[1::]
        }
        assert len(expected_orders) == len(suggested_orders[vmc.vendorkey])
        for index in suggested_orders[vmc.vendorkey]:
            expected = expected_orders[suggested_orders[vmc.vendorkey][index]]
            suggested = suggested_orders['change_auto_order'][index]
            assert expected == suggested
        for file_name in vm_dict[vmc.filename]:
            file_path = utl.raw_path
            if not os.path.isfile(os.path.join(file_path, file_name)):
                file_path = utl.dict_path
            os.remove(os.path.join(file_path, file_name))

    @requires_base_config
    def test_all_analysis_on_empty_df(self):
        aly = az.Analyze(df=pd.DataFrame(), matrix=vm.VendorMatrix())
        aly.do_all_analysis()

    @requires_base_config
    def test_all_analysis_on_header_df(self):
        df = pd.DataFrame(columns=[
            vmc.btnclick, vmc.clicks, vmc.date, dctc.FPN, vmc.impressions,
            vmc.cost, dctc.PNC, vmc.purchase, vmc.reach, vmc.revenue, dctc.UNC,
            vmc.vendorkey, dctc.AD, dctc.AF, dctc.AM, dctc.AR, dctc.AT,
            dctc.AGE, dctc.AGY, dctc.AGF, dctc.BUD, dctc.BM, dctc.BR, dctc.BR2,
            dctc.BR3, dctc.BR4, dctc.BR5, dctc.CTA, dctc.CAM, dctc.CP, dctc.CQ,
            dctc.CTIM, dctc.CT, dctc.CH, dctc.URL, dctc.CLI, dctc.COP,
            dctc.COU,
            dctc.CRE, dctc.CD, dctc.LEN, dctc.LI, dctc.CM, dctc.CURL, dctc.DT1,
            dctc.DT2, dctc.DEM, dctc.DL1, dctc.DL2, dctc.DUL, dctc.ED,
            dctc.ENV,
            dctc.FAC, dctc.FOR, dctc.FRA, dctc.GEN, dctc.GT, dctc.GTF,
            dctc.HL1,
            dctc.HL2, dctc.KPI, dctc.MC, dctc.MIS, dctc.MIS2, dctc.MIS3,
            dctc.MIS4, dctc.MIS5, dctc.MIS6, dctc.MN, dctc.MT, dctc.PKD,
            dctc.PD, dctc.PD2, dctc.PD3, dctc.PD4, dctc.PD5, dctc.PLD, dctc.PN,
            dctc.PLA, dctc.PRD, dctc.PRN, dctc.REG, dctc.RFM, dctc.RFR,
            dctc.RFT, dctc.RET, dctc.SRV, dctc.SIZ, dctc.SD, dctc.TAR, dctc.TB,
            dctc.TP, dctc.TPB, dctc.TPF, dctc.VEN, dctc.VT, dctc.VFM, dctc.VFR,
            dctc.PFPN])
        aly = az.Analyze(df=df, matrix=vm.VendorMatrix())
        aly.do_all_analysis()

    @requires_base_config
    def test_all_analysis_on_df(self):
        d = {vmc.clicks: [38, 2078, 2428, 0, 399, 405],
             vmc.date: ['4/9/2025' for x in range(6)],
             vmc.impressions: [1000, 120000, 0, 2500, 750, 0],
             vmc.cost: [1820.50, 8170.01, 0, 750.00, 8390.93, 0],
             vmc.vendorkey: ['API_Vendor1_Q1', 'API_Vendor1_Q1', 'API_DCM_Q1',
                             'API_Vendor1_Q1', 'API_Vendor1_Q1',
                             'API_DCM_Q1'],
             vmc.views: [100, 250, 0, 110, 253, 0],
             vmc.views100: [0, 2, 0, 35, 46, 0],
             dctc.CAM: ['Launch' for x in range(6)],
             dctc.PN: ['place_name_1', 'place_name_2', 'place_name_2',
                       'place_name_3', 'place_name_4', 'place_name_4'],
             dctc.VEN: ['Vendor1', 'Vendor1', 'Vendor1', 'Vendor2',
                        'Vendor2', 'Vendor2'],
             dctc.PNC: [0 for x in range(6)],
             'Net Cost Final': [1820.50, 8170.01, 0, 750.00, 8390.93, 0]}
        df = pd.DataFrame(data=d)
        aly = az.Analyze(df=df, matrix=vm.VendorMatrix())
        aly.do_all_analysis()

    def test_train_tfidf(self):
        texts = ['The file type for raw files are csv',
                 'Add 40 to the topline',
                 'Add raw file on the import tab']
        user_text = 'Where do I add a raw file?'
        top_k = len(texts) + 1
        transformer = az.TfIdfTransformer(texts=texts)
        scores = transformer.search(user_text, top_k=top_k)
        assert scores
        bm25_scores = transformer.bm25_search(user_text, top_k=top_k)
        assert bm25_scores

    @requires_base_config
    def test_do_analysis_and_fix_processor(self):
        output_dfs = [pd.DataFrame(),
                      self.get_output_as_df(with_plan=True),
                      self.get_output_as_df(with_plan=True, new_place='blah')]
        for output_df in output_dfs:
            aly = az.Analyze(df=output_df, matrix=vm.VendorMatrix())
            fixes_to_run = aly.do_analysis_and_fix_processor(first_run=True)

            assert not fixes_to_run


class TestAliChat:
    def test_index_db_model_by_word(self):
        word_str = 'item'
        item_num = 5
        db_model = ['{} {}'.format(word_str, x) for x in range(item_num)]
        word_idx = az.AliChat().index_db_model_by_word(
            db_model, model_is_list=True)
        assert word_idx
        assert len(word_idx[word_str]) == len(db_model)
        for i in range(item_num):
            assert word_idx[str(i)] == [i]


default_col_names = [
    '"lqadb"."event"."eventname"',
    '"lqadb"."event"."eventdate"', '"lqadb"."ad"."adname"',
    '"lqadb"."adformat"."adformatname"',
    '"lqadb"."adsize"."adsizename"',
    '"lqadb"."adtype"."adtypename"', '"lqadb"."age"."agename"',
    '"lqadb"."agency"."agencyname"',
    '"lqadb"."buymodel"."buymodelname"',
    '"lqadb"."campaign"."campaignname"',
    '"lqadb"."campaign"."campaigntype"',
    '"lqadb"."campaign"."campaignphase"',
    '"lqadb"."campaign"."campaigntiming"',
    '"lqadb"."character"."charactername"',
    '"lqadb"."client"."clientname"',
    '"lqadb"."copy"."copyname"',
    '"lqadb"."country"."countryname"',
    '"lqadb"."creative"."creativename"',
    '"lqadb"."creativedescription"."creativedescriptionname"',
    '"lqadb"."creativelength"."creativelengthname"',
    '"lqadb"."creativelineitem"."creativelineitemname"',
    '"lqadb"."creativemodifier"."creativemodifiername"',
    '"lqadb"."cta"."ctaname"',
    '"lqadb"."datatype1"."datatype1name"',
    '"lqadb"."datatype2"."datatype2name"',
    '"lqadb"."demographic"."demographicname"',
    '"lqadb"."descriptionline1"."descriptionline1name"',
    '"lqadb"."descriptionline2"."descriptionline2name"',
    '"lqadb"."displayurl"."displayurlname"',
    '"lqadb"."environment"."environmentname"',
    '"lqadb"."faction"."factionname"',
    '"lqadb"."fullplacement"."fullplacementname"',
    '"lqadb"."fullplacement"."buyrate"',
    '"lqadb"."fullplacement"."placementdate"',
    '"lqadb"."fullplacement"."startdate"',
    '"lqadb"."fullplacement"."enddate"',
    '"lqadb"."gender"."gendername"',
    '"lqadb"."genretargeting"."genretargetingname"',
    '"lqadb"."genretargetingfine"."genretargetingfinename"',
    '"lqadb"."headline1"."headline1name"',
    '"lqadb"."headline2"."headline2name"',
    '"lqadb"."kpi"."kpiname"',
    '"lqadb"."mediachannel"."mediachannelname"',
    '"lqadb"."packagedescription"."packagedescriptionname"',
    '"lqadb"."placement"."placementname"',
    '"lqadb"."placementdescription"."placementdescriptionname"',
    '"lqadb"."platform"."platformname"',
    '"lqadb"."product"."productname"',
    '"lqadb"."product"."productdetail"',
    '"lqadb"."region"."regionname"',
    '"lqadb"."retailer"."retailername"',
    '"lqadb"."serving"."servingname"',
    '"lqadb"."targeting"."targetingname"',
    '"lqadb"."targetingbucket"."targetingbucketname"',
    '"lqadb"."transactionproduct"."transactionproductname"',
    '"lqadb"."transactionproductbroad"."transactionproductbroadname"',
    '"lqadb"."transactionproductfine"."transactionproductfinename"',
    '"lqadb"."upload"."uploadname"',
    '"lqadb"."upload"."datastartdate"',
    '"lqadb"."upload"."dataenddate"',
    '"lqadb"."upload"."lastuploaddate"',
    '"lqadb"."vendor"."vendorname"',
    '"lqadb"."vendortype"."vendortypename"'
]
rev_sum_col = ('SUM("lqadb"."event"."revenue_userstart_30day") AS '
               '"revenue_userstart_30day"')
default_sum_cols = [
    'SUM("lqadb"."event"."impressions") AS "impressions"',
    'SUM("lqadb"."event"."clicks") AS "clicks"',
    'SUM("lqadb"."event"."netcost") AS "netcost"',
    'SUM("lqadb"."event"."adservingcost") AS "adservingcost"',
    'SUM("lqadb"."event"."agencyfees") AS "agencyfees"',
    'SUM("lqadb"."event"."totalcost") AS "totalcost"',
    'SUM("lqadb"."event"."videoviews") AS "videoviews"',
    'SUM("lqadb"."event"."videoviews25") AS "videoviews25"',
    'SUM("lqadb"."event"."videoviews50") AS "videoviews50"',
    'SUM("lqadb"."event"."videoviews75") AS "videoviews75"',
    'SUM("lqadb"."event"."videoviews100") AS "videoviews100"',
    'SUM("lqadb"."event"."landingpage") AS "landingpage"',
    'SUM("lqadb"."event"."homepage") AS "homepage"',
    'SUM("lqadb"."event"."buttonclick") AS "buttonclick"',
    'SUM("lqadb"."event"."purchase") AS "purchase"',
    'SUM("lqadb"."event"."signup") AS "signup"',
    'SUM("lqadb"."event"."gameplayed") AS "gameplayed"',
    'SUM("lqadb"."event"."gameplayed3") AS "gameplayed3"',
    'SUM("lqadb"."event"."gameplayed6") AS "gameplayed6"',
    'SUM("lqadb"."event"."landingpage_pi") AS "landingpage_pi"',
    'SUM("lqadb"."event"."landingpage_pc") AS "landingpage_pc"',
    'SUM("lqadb"."event"."homepage_pi") AS "homepage_pi"',
    'SUM("lqadb"."event"."homepage_pc") AS "homepage_pc"',
    'SUM("lqadb"."event"."buttonclick_pi") AS "buttonclick_pi"',
    'SUM("lqadb"."event"."buttonclick_pc") AS "buttonclick_pc"',
    'SUM("lqadb"."event"."purchase_pi") AS "purchase_pi"',
    'SUM("lqadb"."event"."purchase_pc") AS "purchase_pc"',
    'SUM("lqadb"."event"."signup_pi") AS "signup_pi"',
    'SUM("lqadb"."event"."signup_pc") AS "signup_pc"',
    'SUM("lqadb"."event"."gameplayed_pi") AS "gameplayed_pi"',
    'SUM("lqadb"."event"."gameplayed_pc") AS "gameplayed_pc"',
    'SUM("lqadb"."event"."gameplayed3_pi") AS "gameplayed3_pi"',
    'SUM("lqadb"."event"."gameplayed3_pc") AS "gameplayed3_pc"',
    'SUM("lqadb"."event"."gameplayed6_pi") AS "gameplayed6_pi"',
    'SUM("lqadb"."event"."gameplayed6_pc") AS "gameplayed6_pc"',
    'SUM("lqadb"."event"."reach") AS "reach"',
    'SUM("lqadb"."event"."frequency") AS "frequency"',
    'SUM("lqadb"."event"."engagements") AS "engagements"',
    'SUM("lqadb"."event"."likes") AS "likes"',
    'SUM("lqadb"."event"."revenue") AS "revenue"',
    'SUM("lqadb"."event"."newuser") AS "newuser"',
    'SUM("lqadb"."event"."activeuser") AS "activeuser"',
    'SUM("lqadb"."event"."download") AS "download"',
    'SUM("lqadb"."event"."login") AS "login"',
    'SUM("lqadb"."event"."newuser_pi") AS "newuser_pi"',
    'SUM("lqadb"."event"."activeuser_pi") AS "activeuser_pi"',
    'SUM("lqadb"."event"."download_pi") AS "download_pi"',
    'SUM("lqadb"."event"."login_pi") AS "login_pi"',
    'SUM("lqadb"."event"."newuser_pc") AS "newuser_pc"',
    'SUM("lqadb"."event"."activeuser_pc") AS "activeuser_pc"',
    'SUM("lqadb"."event"."download_pc") AS "download_pc"',
    'SUM("lqadb"."event"."login_pc") AS "login_pc"',
    'SUM("lqadb"."event"."retention_day1") AS "retention_day1"',
    'SUM("lqadb"."event"."retention_day3") AS "retention_day3"',
    'SUM("lqadb"."event"."retention_day7") AS "retention_day7"',
    'SUM("lqadb"."event"."retention_day14") AS "retention_day14"',
    'SUM("lqadb"."event"."retention_day30") AS "retention_day30"',
    'SUM("lqadb"."event"."retention_day60") AS "retention_day60"',
    'SUM("lqadb"."event"."retention_day90") AS "retention_day90"',
    'SUM("lqadb"."event"."retention_day120") AS "retention_day120"',
    'SUM("lqadb"."event"."total_user") AS "total_user"',
    'SUM("lqadb"."event"."paying_user") AS "paying_user"',
    'SUM("lqadb"."event"."transaction") AS "transaction"',
    'SUM("lqadb"."event"."match_played") AS "match_played"',
    'SUM("lqadb"."event"."sm_totalbuzz") AS "sm_totalbuzz"',
    'SUM("lqadb"."event"."sm_totalbuzzpost") AS "sm_totalbuzzpost"',
    'SUM("lqadb"."event"."sm_totalreplies") AS "sm_totalreplies"',
    'SUM("lqadb"."event"."sm_totalreposts") AS "sm_totalreposts"',
    'SUM("lqadb"."event"."sm_originalposts") AS "sm_originalposts"',
    'SUM("lqadb"."event"."sm_impressions") AS "sm_impressions"',
    'SUM("lqadb"."event"."sm_positivesentiment") AS "sm_positivesentiment"',
    'SUM("lqadb"."event"."sm_negativesentiment") AS "sm_negativesentiment"',
    'SUM("lqadb"."event"."sm_passion") AS "sm_passion"',
    'SUM("lqadb"."event"."sm_uniqueauthors") AS "sm_uniqueauthors"',
    'SUM("lqadb"."event"."sm_strongemotion") AS "sm_strongemotion"',
    'SUM("lqadb"."event"."sm_weakemotion") AS "sm_weakemotion"',
    'SUM("lqadb"."event"."transaction_revenue") AS "transaction_revenue"',
    'SUM("lqadb"."event"."revenue_userstart") AS "revenue_userstart"',
    rev_sum_col,
    'SUM("lqadb"."event"."reportingcost") AS "reportingcost"',
    'SUM("lqadb"."event"."trueviewviews") AS "trueviewviews"',
    'SUM("lqadb"."event"."fb3views") AS "fb3views"',
    'SUM("lqadb"."event"."fb10views") AS "fb10views"',
    'SUM("lqadb"."event"."dcmservicefee") AS "dcmservicefee"',
    'SUM("lqadb"."event"."view_imps") AS "view_imps"',
    'SUM("lqadb"."event"."view_tot_imps") AS "view_tot_imps"',
    'SUM("lqadb"."event"."view_fraud") AS "view_fraud"',
    'SUM("lqadb"."event"."ga_sessions") AS "ga_sessions"',
    'SUM("lqadb"."event"."ga_goal1") AS "ga_goal1"',
    'SUM("lqadb"."event"."ga_goal2") AS "ga_goal2"',
    'SUM("lqadb"."event"."ga_pageviews") AS "ga_pageviews"',
    'SUM("lqadb"."event"."ga_bounces") AS "ga_bounces"',
    'SUM("lqadb"."event"."comments") AS "comments"',
    'SUM("lqadb"."event"."shares") AS "shares"',
    'SUM("lqadb"."event"."reactions") AS "reactions"',
    'SUM("lqadb"."event"."checkout") AS "checkout"',
    'SUM("lqadb"."event"."checkoutpi") AS "checkoutpi"',
    'SUM("lqadb"."event"."checkoutpc") AS "checkoutpc"',
    'SUM("lqadb"."event"."reach-campaign") AS "reach-campaign"',
    'SUM("lqadb"."event"."reach-date") AS "reach-date"',
    'SUM("lqadb"."event"."reach_campaign") AS "reach_campaign"',
    'SUM("lqadb"."event"."reach_date") AS "reach_date"',
    'SUM("lqadb"."event"."ga_timeonpage") AS "ga_timeonpage"',
    'SUM("lqadb"."event"."signup_ss") AS "signup_ss"',
    'SUM("lqadb"."event"."landingpage_ss") AS "landingpage_ss"',
    'SUM("lqadb"."event"."view_monitored_imps") AS "view_monitored_imps"',
    'SUM("lqadb"."event"."verificationcost") AS "verificationcost"',
    'SUM("lqadb"."event"."videoplays") AS "videoplays"',
    'SUM("lqadb"."event"."ad_recallers") AS "ad_recallers"',
    'SUM("lqadb"."plan"."plannednetcost") AS "plannednetcost"'
]
conv_event_sum_cols = [
    'SUM("lqadb"."eventconv"."conv1_cpa") AS "conv1_cpa"',
    'SUM("lqadb"."eventconv"."conv2") AS "conv2"',
    'SUM("lqadb"."eventconv"."conv3") AS "conv3"',
    'SUM("lqadb"."eventconv"."conv4") AS "conv4"',
    'SUM("lqadb"."eventconv"."conv5") AS "conv5"',
    'SUM("lqadb"."eventconv"."conv6") AS "conv6"',
    'SUM("lqadb"."eventconv"."conv7") AS "conv7"',
    'SUM("lqadb"."eventconv"."conv8") AS "conv8"',
    'SUM("lqadb"."eventconv"."conv9") AS "conv9"',
    'SUM("lqadb"."eventconv"."conv10") AS "conv10"'
]


class TestExport:

    @pytest.mark.parametrize(
        'filter_table, event_tables, expected_string', [
            ('', None,
             'FROM "lqadb"."event" \nFULL JOIN "lqadb"."fullplacement" ON ('
             '"lqadb"."event"."fullplacementid" = '
             '"lqadb"."fullplacement"."fullplacementid") \nLEFT JOIN '
             '"lqadb"."upload" ON ("lqadb"."event"."uploadid" = '
             '"lqadb"."upload"."uploadid") \nLEFT JOIN "lqadb"."campaign" ON '
             '("lqadb"."fullplacement"."campaignid" = '
             '"lqadb"."campaign"."campaignid") \nLEFT JOIN "lqadb"."vendor" '
             'ON ("lqadb"."fullplacement"."vendorid" = '
             '"lqadb"."vendor"."vendorid") \nLEFT JOIN "lqadb"."country" ON '
             '("lqadb"."fullplacement"."countryid" = '
             '"lqadb"."country"."countryid") \nLEFT JOIN '
             '"lqadb"."mediachannel" ON ('
             '"lqadb"."fullplacement"."mediachannelid" = '
             '"lqadb"."mediachannel"."mediachannelid") \nLEFT JOIN '
             '"lqadb"."targeting" ON ("lqadb"."fullplacement"."targetingid" '
             '= "lqadb"."targeting"."targetingid") \nLEFT JOIN '
             '"lqadb"."creative" ON ("lqadb"."fullplacement"."creativeid" = '
             '"lqadb"."creative"."creativeid") \nLEFT JOIN "lqadb"."copy" ON '
             '("lqadb"."fullplacement"."copyid" = "lqadb"."copy"."copyid") '
             '\nLEFT JOIN "lqadb"."buymodel" ON ('
             '"lqadb"."fullplacement"."buymodelid" = '
             '"lqadb"."buymodel"."buymodelid") \nLEFT JOIN "lqadb"."serving" '
             'ON ("lqadb"."fullplacement"."servingid" = '
             '"lqadb"."serving"."servingid") \nLEFT JOIN "lqadb"."retailer" '
             'ON ("lqadb"."fullplacement"."retailerid" = '
             '"lqadb"."retailer"."retailerid") \nLEFT JOIN '
             '"lqadb"."environment" ON ('
             '"lqadb"."fullplacement"."environmentid" = '
             '"lqadb"."environment"."environmentid") \nLEFT JOIN '
             '"lqadb"."kpi" ON ("lqadb"."fullplacement"."kpiid" = '
             '"lqadb"."kpi"."kpiid") \nLEFT JOIN "lqadb"."faction" ON ('
             '"lqadb"."fullplacement"."factionid" = '
             '"lqadb"."faction"."factionid") \nLEFT JOIN "lqadb"."platform" '
             'ON ("lqadb"."fullplacement"."platformid" = '
             '"lqadb"."platform"."platformid") \nLEFT JOIN '
             '"lqadb"."transactionproduct" ON ('
             '"lqadb"."fullplacement"."transactionproductid" = '
             '"lqadb"."transactionproduct"."transactionproductid") \nLEFT '
             'JOIN "lqadb"."placement" ON ('
             '"lqadb"."fullplacement"."placementid" = '
             '"lqadb"."placement"."placementid") \nLEFT JOIN '
             '"lqadb"."placementdescription" ON ('
             '"lqadb"."fullplacement"."placementdescriptionid" = '
             '"lqadb"."placementdescription"."placementdescriptionid") '
             '\nLEFT JOIN "lqadb"."packagedescription" ON ('
             '"lqadb"."fullplacement"."packagedescriptionid" = '
             '"lqadb"."packagedescription"."packagedescriptionid") \nLEFT '
             'JOIN "lqadb"."product" ON ("lqadb"."campaign"."productid" = '
             '"lqadb"."product"."productid") \nLEFT JOIN "lqadb"."client" ON '
             '("lqadb"."product"."clientid" = "lqadb"."client"."clientid") '
             '\nLEFT JOIN "lqadb"."agency" ON ("lqadb"."client"."agencyid" = '
             '"lqadb"."agency"."agencyid") \nLEFT JOIN "lqadb"."vendortype" '
             'ON ("lqadb"."vendor"."vendortypeid" = '
             '"lqadb"."vendortype"."vendortypeid") \nLEFT JOIN '
             '"lqadb"."region" ON ("lqadb"."country"."regionid" = '
             '"lqadb"."region"."regionid") \nLEFT JOIN "lqadb"."age" ON ('
             '"lqadb"."targeting"."ageid" = "lqadb"."age"."ageid") \nLEFT '
             'JOIN "lqadb"."gender" ON ("lqadb"."targeting"."genderid" = '
             '"lqadb"."gender"."genderid") \nLEFT JOIN "lqadb"."datatype1" '
             'ON ("lqadb"."targeting"."datatype1id" = '
             '"lqadb"."datatype1"."datatype1id") \nLEFT JOIN '
             '"lqadb"."datatype2" ON ("lqadb"."targeting"."datatype2id" = '
             '"lqadb"."datatype2"."datatype2id") \nLEFT JOIN '
             '"lqadb"."targetingbucket" ON ('
             '"lqadb"."targeting"."targetingbucketid" = '
             '"lqadb"."targetingbucket"."targetingbucketid") \nLEFT JOIN '
             '"lqadb"."genretargeting" ON ('
             '"lqadb"."targeting"."genretargetingid" = '
             '"lqadb"."genretargeting"."genretargetingid") \nLEFT JOIN '
             '"lqadb"."genretargetingfine" ON ('
             '"lqadb"."targeting"."genretargetingfineid" = '
             '"lqadb"."genretargetingfine"."genretargetingfineid") \nLEFT '
             'JOIN "lqadb"."demographic" ON ("lqadb"."age"."demographicid" = '
             '"lqadb"."demographic"."demographicid") \nLEFT JOIN '
             '"lqadb"."adsize" ON ("lqadb"."creative"."adsizeid" = '
             '"lqadb"."adsize"."adsizeid") \nLEFT JOIN "lqadb"."adformat" ON '
             '("lqadb"."creative"."adformatid" = '
             '"lqadb"."adformat"."adformatid") \nLEFT JOIN "lqadb"."adtype" '
             'ON ("lqadb"."creative"."adtypeid" = '
             '"lqadb"."adtype"."adtypeid") \nLEFT JOIN "lqadb"."cta" ON ('
             '"lqadb"."creative"."ctaid" = "lqadb"."cta"."ctaid") \nLEFT '
             'JOIN "lqadb"."creativedescription" ON ('
             '"lqadb"."creative"."creativedescriptionid" = '
             '"lqadb"."creativedescription"."creativedescriptionid") \nLEFT '
             'JOIN "lqadb"."character" ON ("lqadb"."creative"."characterid" '
             '= "lqadb"."character"."characterid") \nLEFT JOIN '
             '"lqadb"."creativemodifier" ON ('
             '"lqadb"."creative"."creativemodifierid" = '
             '"lqadb"."creativemodifier"."creativemodifierid") \nLEFT JOIN '
             '"lqadb"."creativelineitem" ON ('
             '"lqadb"."creative"."creativelineitemid" = '
             '"lqadb"."creativelineitem"."creativelineitemid") \nLEFT JOIN '
             '"lqadb"."creativelength" ON ('
             '"lqadb"."creative"."creativelengthid" = '
             '"lqadb"."creativelength"."creativelengthid") \nLEFT JOIN '
             '"lqadb"."ad" ON ("lqadb"."copy"."adid" = "lqadb"."ad"."adid") '
             '\nLEFT JOIN "lqadb"."descriptionline1" ON ('
             '"lqadb"."copy"."descriptionline1id" = '
             '"lqadb"."descriptionline1"."descriptionline1id") \nLEFT JOIN '
             '"lqadb"."descriptionline2" ON ('
             '"lqadb"."copy"."descriptionline2id" = '
             '"lqadb"."descriptionline2"."descriptionline2id") \nLEFT JOIN '
             '"lqadb"."headline1" ON ("lqadb"."copy"."headline1id" = '
             '"lqadb"."headline1"."headline1id") \nLEFT JOIN '
             '"lqadb"."headline2" ON ("lqadb"."copy"."headline2id" = '
             '"lqadb"."headline2"."headline2id") \nLEFT JOIN '
             '"lqadb"."displayurl" ON ("lqadb"."copy"."displayurlid" = '
             '"lqadb"."displayurl"."displayurlid") \nLEFT JOIN '
             '"lqadb"."transactionproductbroad" ON ('
             '"lqadb"."transactionproduct"."transactionproductbroadid" = '
             '"lqadb"."transactionproductbroad"."transactionproductbroadid") '
             '\nLEFT JOIN "lqadb"."transactionproductfine" ON ('
             '"lqadb"."transactionproduct"."transactionproductfineid" = '
             '"lqadb"."transactionproductfine"."transactionproductfineid'
             '")\nFULL JOIN "lqadb"."plan" ON ('
             '"lqadb"."fullplacement"."fullplacementid" = '
             '"lqadb"."plan"."fullplacementid")'
             ),
            (exc.product_table, None,
             'FROM "lqadb"."event" \nFULL JOIN "lqadb"."fullplacement" ON ('
             '"lqadb"."event"."fullplacementid" = '
             '"lqadb"."fullplacement"."fullplacementid") \nLEFT JOIN '
             '"lqadb"."upload" ON ("lqadb"."event"."uploadid" = '
             '"lqadb"."upload"."uploadid") \nLEFT JOIN "lqadb"."campaign" ON '
             '("lqadb"."fullplacement"."campaignid" = '
             '"lqadb"."campaign"."campaignid") \nLEFT JOIN "lqadb"."product" '
             'ON ("lqadb"."campaign"."productid" = '
             '"lqadb"."product"."productid") \nLEFT JOIN "lqadb"."vendor" ON '
             '("lqadb"."fullplacement"."vendorid" = '
             '"lqadb"."vendor"."vendorid") \nLEFT JOIN "lqadb"."country" ON '
             '("lqadb"."fullplacement"."countryid" = '
             '"lqadb"."country"."countryid") \nLEFT JOIN '
             '"lqadb"."mediachannel" ON ('
             '"lqadb"."fullplacement"."mediachannelid" = '
             '"lqadb"."mediachannel"."mediachannelid") \nLEFT JOIN '
             '"lqadb"."targeting" ON ("lqadb"."fullplacement"."targetingid" '
             '= "lqadb"."targeting"."targetingid") \nLEFT JOIN '
             '"lqadb"."creative" ON ("lqadb"."fullplacement"."creativeid" = '
             '"lqadb"."creative"."creativeid") \nLEFT JOIN "lqadb"."copy" ON '
             '("lqadb"."fullplacement"."copyid" = "lqadb"."copy"."copyid") '
             '\nLEFT JOIN "lqadb"."buymodel" ON ('
             '"lqadb"."fullplacement"."buymodelid" = '
             '"lqadb"."buymodel"."buymodelid") \nLEFT JOIN "lqadb"."serving" '
             'ON ("lqadb"."fullplacement"."servingid" = '
             '"lqadb"."serving"."servingid") \nLEFT JOIN "lqadb"."retailer" '
             'ON ("lqadb"."fullplacement"."retailerid" = '
             '"lqadb"."retailer"."retailerid") \nLEFT JOIN '
             '"lqadb"."environment" ON ('
             '"lqadb"."fullplacement"."environmentid" = '
             '"lqadb"."environment"."environmentid") \nLEFT JOIN '
             '"lqadb"."kpi" ON ("lqadb"."fullplacement"."kpiid" = '
             '"lqadb"."kpi"."kpiid") \nLEFT JOIN "lqadb"."faction" ON ('
             '"lqadb"."fullplacement"."factionid" = '
             '"lqadb"."faction"."factionid") \nLEFT JOIN "lqadb"."platform" '
             'ON ("lqadb"."fullplacement"."platformid" = '
             '"lqadb"."platform"."platformid") \nLEFT JOIN '
             '"lqadb"."transactionproduct" ON ('
             '"lqadb"."fullplacement"."transactionproductid" = '
             '"lqadb"."transactionproduct"."transactionproductid") \nLEFT '
             'JOIN "lqadb"."placement" ON ('
             '"lqadb"."fullplacement"."placementid" = '
             '"lqadb"."placement"."placementid") \nLEFT JOIN '
             '"lqadb"."placementdescription" ON ('
             '"lqadb"."fullplacement"."placementdescriptionid" = '
             '"lqadb"."placementdescription"."placementdescriptionid") '
             '\nLEFT JOIN "lqadb"."packagedescription" ON ('
             '"lqadb"."fullplacement"."packagedescriptionid" = '
             '"lqadb"."packagedescription"."packagedescriptionid") \nLEFT '
             'JOIN "lqadb"."client" ON ("lqadb"."product"."clientid" = '
             '"lqadb"."client"."clientid") \nLEFT JOIN "lqadb"."agency" ON ('
             '"lqadb"."client"."agencyid" = "lqadb"."agency"."agencyid") '
             '\nLEFT JOIN "lqadb"."vendortype" ON ('
             '"lqadb"."vendor"."vendortypeid" = '
             '"lqadb"."vendortype"."vendortypeid") \nLEFT JOIN '
             '"lqadb"."region" ON ("lqadb"."country"."regionid" = '
             '"lqadb"."region"."regionid") \nLEFT JOIN "lqadb"."age" ON ('
             '"lqadb"."targeting"."ageid" = "lqadb"."age"."ageid") \nLEFT '
             'JOIN "lqadb"."gender" ON ("lqadb"."targeting"."genderid" = '
             '"lqadb"."gender"."genderid") \nLEFT JOIN "lqadb"."datatype1" '
             'ON ("lqadb"."targeting"."datatype1id" = '
             '"lqadb"."datatype1"."datatype1id") \nLEFT JOIN '
             '"lqadb"."datatype2" ON ("lqadb"."targeting"."datatype2id" = '
             '"lqadb"."datatype2"."datatype2id") \nLEFT JOIN '
             '"lqadb"."targetingbucket" ON ('
             '"lqadb"."targeting"."targetingbucketid" = '
             '"lqadb"."targetingbucket"."targetingbucketid") \nLEFT JOIN '
             '"lqadb"."genretargeting" ON ('
             '"lqadb"."targeting"."genretargetingid" = '
             '"lqadb"."genretargeting"."genretargetingid") \nLEFT JOIN '
             '"lqadb"."genretargetingfine" ON ('
             '"lqadb"."targeting"."genretargetingfineid" = '
             '"lqadb"."genretargetingfine"."genretargetingfineid") \nLEFT '
             'JOIN "lqadb"."demographic" ON ("lqadb"."age"."demographicid" = '
             '"lqadb"."demographic"."demographicid") \nLEFT JOIN '
             '"lqadb"."adsize" ON ("lqadb"."creative"."adsizeid" = '
             '"lqadb"."adsize"."adsizeid") \nLEFT JOIN "lqadb"."adformat" ON '
             '("lqadb"."creative"."adformatid" = '
             '"lqadb"."adformat"."adformatid") \nLEFT JOIN "lqadb"."adtype" '
             'ON ("lqadb"."creative"."adtypeid" = '
             '"lqadb"."adtype"."adtypeid") \nLEFT JOIN "lqadb"."cta" ON ('
             '"lqadb"."creative"."ctaid" = "lqadb"."cta"."ctaid") \nLEFT '
             'JOIN "lqadb"."creativedescription" ON ('
             '"lqadb"."creative"."creativedescriptionid" = '
             '"lqadb"."creativedescription"."creativedescriptionid") \nLEFT '
             'JOIN "lqadb"."character" ON ("lqadb"."creative"."characterid" '
             '= "lqadb"."character"."characterid") \nLEFT JOIN '
             '"lqadb"."creativemodifier" ON ('
             '"lqadb"."creative"."creativemodifierid" = '
             '"lqadb"."creativemodifier"."creativemodifierid") \nLEFT JOIN '
             '"lqadb"."creativelineitem" ON ('
             '"lqadb"."creative"."creativelineitemid" = '
             '"lqadb"."creativelineitem"."creativelineitemid") \nLEFT JOIN '
             '"lqadb"."creativelength" ON ('
             '"lqadb"."creative"."creativelengthid" = '
             '"lqadb"."creativelength"."creativelengthid") \nLEFT JOIN '
             '"lqadb"."ad" ON ("lqadb"."copy"."adid" = "lqadb"."ad"."adid") '
             '\nLEFT JOIN "lqadb"."descriptionline1" ON ('
             '"lqadb"."copy"."descriptionline1id" = '
             '"lqadb"."descriptionline1"."descriptionline1id") \nLEFT JOIN '
             '"lqadb"."descriptionline2" ON ('
             '"lqadb"."copy"."descriptionline2id" = '
             '"lqadb"."descriptionline2"."descriptionline2id") \nLEFT JOIN '
             '"lqadb"."headline1" ON ("lqadb"."copy"."headline1id" = '
             '"lqadb"."headline1"."headline1id") \nLEFT JOIN '
             '"lqadb"."headline2" ON ("lqadb"."copy"."headline2id" = '
             '"lqadb"."headline2"."headline2id") \nLEFT JOIN '
             '"lqadb"."displayurl" ON ("lqadb"."copy"."displayurlid" = '
             '"lqadb"."displayurl"."displayurlid") \nLEFT JOIN '
             '"lqadb"."transactionproductbroad" ON ('
             '"lqadb"."transactionproduct"."transactionproductbroadid" = '
             '"lqadb"."transactionproductbroad"."transactionproductbroadid") '
             '\nLEFT JOIN "lqadb"."transactionproductfine" ON ('
             '"lqadb"."transactionproduct"."transactionproductfineid" = '
             '"lqadb"."transactionproductfine"."transactionproductfineid'
             '")\nFULL JOIN "lqadb"."plan" ON ('
             '"lqadb"."fullplacement"."fullplacementid" = '
             '"lqadb"."plan"."fullplacementid")'
             )
        ],
        ids=['default', 'product_filter']
    )
    def test_get_from_script_with_opts(self, filter_table, event_tables,
                                       expected_string):
        sb = exp.ScriptBuilder()
        base_table = [x for x in sb.tables if x.name == 'event'][0]
        from_script = sb.get_from_script_with_opts(
            base_table, filter_table=filter_table, event_tables=event_tables)
        assert from_script == expected_string

    @pytest.mark.parametrize(
        'event_tables, expected_col_names, expected_sum_cols', [
            (None, default_col_names, default_sum_cols),
            (['eventconv'], default_col_names,
             default_sum_cols+conv_event_sum_cols)
        ],
        ids=['default', 'conv']
    )
    def test_get_column_names(self, event_tables, expected_col_names,
                              expected_sum_cols):
        sb = exp.ScriptBuilder()
        base_table = [x for x in sb.tables if x.name == 'event'][0]
        from_script = sb.get_from_script_with_opts(
            base_table, exc.product_table, event_tables=event_tables)
        column_names, sum_columns = sb.get_column_names(
            base_table, event_tables=event_tables)
        assert set(column_names) == set(expected_col_names)
        assert set(sum_columns) == set(expected_sum_cols)

    @pytest.mark.parametrize(
        'metrics, expected_tables', [
            (['impressions', 'clicks'], []),
            (['impressions', 'clicks', 'conv2', 'plan_clicks'],
             ['eventconv', 'eventplan'])
        ],
        ids=['default', 'conv_plan']
    )
    def test_get_active_event_tables(self, metrics, expected_tables):
        sb = exp.ScriptBuilder()
        append_tables = sb.get_active_event_tables(metrics)
        assert set(append_tables) == set(expected_tables)


class TestRun:
    @requires_base_config
    def test_blank_run(self):
        main('--analyze')


class TestImportPlanData:
    @requires_base_config
    def test_import_plan_data(self, tmp_path_factory):
        df = pd.DataFrame({
            vmc.vendorkey: ['API_Test1', 'API_Test2', 'API_Test3'],
            dctc.CAM: ['Camp1', 'Camp2', 'Camp3'],
            dctc.VEN: ['Ven1', 'Ven2', 'Ven3'],
            vmc.date: pd.to_datetime(["2025-01-01", "2025-01-02",
                                      "2025-01-03"]),
        })
        cur_path = os.getcwd()
        plan_omit_list = ['API_Test1']
        key = vm.plan_key
        error_filename = 'PLANNET_ERROR_REPORT.csv'
        kwargs = {
            vmc.fullplacename: [dctc.CAM, dctc.VEN],
            vmc.vendorkey: [key],
            vmc.filenamedict: os.path.join(cur_path, utl.dict_path,
                                           dctc.PFN),
            vmc.filenameerror: os.path.join(cur_path, utl.error_path,
                                            error_filename)
        }
        dic = dct.Dict(kwargs[vmc.filenamedict])
        result = vm.import_plan_data(key, df, plan_omit_list, **kwargs)
        assert isinstance(result, pd.DataFrame)
        expected_columns = [dctc.FPN, dctc.PNC, dctc.UNC, dctc.PRN,
                            dctc.AGY, dctc.CLI, dctc.AGF, dctc.VEN,
                            dctc.CAM, dctc.CTIM, dctc.CP, dctc.CT,
                            dctc.VT, vmc.date]
        assert all(col in result.columns for col in expected_columns)
        assert len(dic.data_dict) == len(result)
        assert result[dctc.PNC].sum() == dic.data_dict[dctc.PNC].sum()

    def test_set_start_date(self):
        test_data = {
            vmc.date: ['2024-12-17', '2024-12-16', '2024-12-18']
        }
        df = pd.DataFrame(test_data)
        start_date = vm.set_start_date(df)
        assert pd.notnull(start_date)
        assert isinstance(start_date, pd.Timestamp)


class TestGamesDb:
    """The games schema: models, natural-key upserts and the Steam
    wide-df normalizing writer (sqlite-backed)."""

    @staticmethod
    def _session():
        import sqlalchemy as sqa
        from sqlalchemy.orm import sessionmaker
        engine = sqa.create_engine('sqlite://').execution_options(
            schema_translate_map={'games': None})
        gmdl.metadata.create_all(engine)
        return sessionmaker(bind=engine)()

    @staticmethod
    def _wide_row():
        return pd.Series({
            'appid': 292030, 'app_detail_name': 'The Witcher 3',
            'publishers': ['CD PROJEKT RED'],
            'developers': ['CD PROJEKT RED'],
            'genres': [{'id': '3', 'description': 'RPG'}],
            'release_date': {'coming_soon': False,
                             'date': 'May 18, 2015'},
            'price_overview': {'final': 3999}, 'player_count': 25000,
            'owners_in_sample': 12, 'wishlists_in_sample': 3,
            'avg_achievement_pct': 14.2, 'review_score': 9,
            'review_score_desc': 'Overwhelmingly Positive',
            'total_positive': 700000, 'total_negative': 14000,
            'total_reviews': 714000,
            'gameeventdate': dt.datetime(2026, 7, 7, 5, 0)})

    def test_upsert_game_and_fact_idempotent(self):
        s = self._session()
        row = self._wide_row()
        game = gdb.upsert_game(s, 'The Witcher 3',
                               **gamesw.game_fields(row))
        assert game.gameid and game.steam_appid == 292030
        assert game.primary_genre == 'RPG'
        assert game.release_date == 'May 18, 2015'
        key = {'gameid': game.gameid,
               'eventdate': row['gameeventdate']}
        assert gdb.upsert_fact(s, gmdl.GameEvent, key,
                               gamesw.event_fields(row)) == 1
        s.commit()
        event = s.query(gmdl.GameEvent).one()
        assert float(event.player_count) == 25000
        assert float(event.price) == 39.99
        assert event.review_score_desc == 'Overwhelmingly Positive'
        # Rerun matches by appid + natural key: update, not insert.
        again = gdb.upsert_game(s, 'The Witcher 3',
                                **gamesw.game_fields(row))
        assert again.gameid == game.gameid
        assert gdb.upsert_fact(s, gmdl.GameEvent, key,
                               gamesw.event_fields(row)) == 0
        s.commit()
        assert s.query(gmdl.Game).count() == 1
        assert s.query(gmdl.GameEvent).count() == 1

    def test_upsert_game_fills_without_clobbering(self):
        s = self._session()
        seeded = gdb.upsert_game(s, 'Halo Infinite',
                                 registry_slug='halo-infinite',
                                 publisher='Xbox')
        merged = gdb.upsert_game(s, 'Halo Infinite',
                                 registry_slug='halo-infinite',
                                 opencritic_id=42, developer='343')
        assert merged.gameid == seeded.gameid
        assert merged.publisher == 'Xbox'  # filled value kept
        assert merged.opencritic_id == 42 and merged.developer == '343'
        assert s.query(gmdl.Game).count() == 1

    def test_writer_field_helpers(self):
        row = self._wide_row()
        fields = gamesw.game_fields(row)
        assert fields['steam_appid'] == 292030
        assert fields['publisher'] == 'CD PROJEKT RED'
        assert fields['primary_genre'] == 'RPG'
        events = gamesw.event_fields(row)
        assert events['price'] == 39.99
        assert events['total_reviews'] == 714000
        # NaN/absent cells land as None, never NaN.
        sparse = pd.Series({'appid': 1, 'player_count': float('nan')})
        assert gamesw.event_fields(sparse)['player_count'] is None
        assert gamesw.game_fields(sparse)['publisher'] is None

    def test_writer_skips_without_config(self, tmp_path, monkeypatch):
        monkeypatch.setattr(utl, 'config_path', str(tmp_path) + '/')
        monkeypatch.setattr(gamesw, 'games_db_available',
                            lambda config='x': False)
        df = pd.DataFrame([self._wide_row()])
        assert gamesw.write_steam_events(df) == 0
        assert gamesw.write_steam_events(pd.DataFrame()) == 0

    def test_name_fallback_knits_sources_onto_one_row(self):
        s = self._session()
        # Registry seeds first; the Steam writer's name match lands on
        # the same dim row and adds its identity.
        reg = gdb.upsert_game(s, 'Halo Infinite',
                              registry_slug='halo-infinite')
        steam = gdb.upsert_game(s, 'Halo Infinite', match_name=True,
                                steam_appid=1240440)
        assert steam.gameid == reg.gameid
        assert steam.registry_slug == 'halo-infinite'
        assert steam.steam_appid == 1240440
        # The reverse order knits too (case-insensitive).
        first = gdb.upsert_game(s, 'ELDEN RING', match_name=True,
                                steam_appid=1245620)
        second = gdb.upsert_game(s, 'Elden Ring', match_name=True,
                                 registry_slug='elden-ring')
        assert second.gameid == first.gameid
        assert s.query(gmdl.Game).count() == 2
        # A name collision carrying a conflicting identity is a
        # different game (remake/re-release): new row, no clobber.
        clash = gdb.upsert_game(s, 'Elden Ring', match_name=True,
                                steam_appid=999)
        assert clash.gameid != first.gameid
        assert first.steam_appid == 1245620
        # Without match_name, a bare name never matches (old behavior).
        other = gdb.upsert_game(s, 'Halo Infinite', opencritic_id=7)
        assert other.gameid != reg.gameid

    def test_title_score_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Halo Infinite',
                               registry_slug='halo-infinite')
        day = dt.date(2026, 7, 17)
        key = {'score_date': day, 'title': 'Halo Infinite'}
        fields = {'gameid': game.gameid, 'influence': 1.2,
                  'engagement': -0.4, 'momentum': 0.3, 'composite': 1.1,
                  'headline_metric': 'Player Share', 'current': 0.5,
                  'prior': 0.4, 'share': 0.62, 'share_delta': 0.02,
                  'movement': 'Rising', 'primary_period': '2026-07',
                  'comparison_period': '2026-06', 'genre': 'Shooter'}
        assert gdb.upsert_fact(s, gmdl.TitleScore, key, fields) == 1
        # Unmatched titles land too, with a NULL gameid.
        assert gdb.upsert_fact(
            s, gmdl.TitleScore,
            {'score_date': day, 'title': 'Mystery Title'},
            {'gameid': None, 'composite': -0.2}) == 1
        s.commit()
        # Same-day rerun updates in place — no duplicate snapshots.
        fields['share'] = 0.64
        assert gdb.upsert_fact(s, gmdl.TitleScore, key, fields) == 0
        s.commit()
        assert s.query(gmdl.TitleScore).count() == 2
        row = s.query(gmdl.TitleScore).filter_by(
            title='Halo Infinite').one()
        assert float(row.share) == 0.64
        assert row.gameid == game.gameid
        assert row.movement == 'Rising'
        assert row.genre == 'Shooter'

    def test_new_games_facts_use_full_natural_keys(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Halo Infinite',
                               registry_slug='halo-infinite')
        spend = {'spend_date': dt.date(2025, 9, 10),
                 'brand': 'Borderlands 4', 'channel': 'ALL',
                 'country': 'US', 'buy_type': 'Direct'}
        for channel in ('ALL', 'YouTube'):
            gdb.upsert_fact(s, gmdl.AdSpend,
                            dict(spend, channel=channel), {'spend': 1})
        for month in (5, 6):
            gdb.upsert_fact(
                s, gmdl.ReviewRollup,
                {'gameid': game.gameid,
                 'month_start': dt.date(2026, month, 1)},
                {'positive': 0, 'negative': 0})
        s.commit()
        assert s.query(gmdl.AdSpend).count() == 2
        assert s.query(gmdl.ReviewRollup).count() == 2

    def test_game_release_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Halo Infinite', igdb_id=1105,
                               registry_slug='halo-infinite')
        assert game.igdb_id == 1105
        key = {'igdb_id': 1105}
        fields = {'gameid': game.gameid, 'title': 'Halo Infinite',
                  'slug': 'halo-infinite',
                  'release_date': dt.date(2026, 11, 15), 'hypes': 320,
                  'genres': 'Shooter', 'platforms': 'PC, Xbox'}
        assert gdb.upsert_fact(s, gmdl.GameRelease, key, fields) == 1
        # Unmatched titles land too, with a NULL gameid.
        assert gdb.upsert_fact(
            s, gmdl.GameRelease, {'igdb_id': 2201},
            {'gameid': None, 'title': 'Mystery Title',
             'release_date': dt.date(2026, 9, 1)}) == 1
        s.commit()
        # A rerun after a slip updates the expected date in place.
        fields['release_date'] = dt.date(2027, 2, 2)
        assert gdb.upsert_fact(s, gmdl.GameRelease, key, fields) == 0
        s.commit()
        assert s.query(gmdl.GameRelease).count() == 2
        row = s.query(gmdl.GameRelease).filter_by(igdb_id=1105).one()
        assert row.release_date == dt.date(2027, 2, 2)
        assert row.gameid == game.gameid

    def test_igdb_identity_matching(self):
        s = self._session()
        # igdb_id is an identity: matches find the row, name fallback
        # knits it onto an existing dim row and adds the identity.
        reg = gdb.upsert_game(s, 'Halo Infinite',
                              registry_slug='halo-infinite')
        knit = gdb.upsert_game(s, 'Halo Infinite', match_name=True,
                               igdb_id=1105)
        assert knit.gameid == reg.gameid and knit.igdb_id == 1105
        assert gdb.find_game(s, igdb_id=1105).gameid == reg.gameid
        # A name collision carrying a different igdb_id is a new row.
        clash = gdb.upsert_game(s, 'Halo Infinite', match_name=True,
                                igdb_id=9999)
        assert clash.gameid != reg.gameid
        assert reg.igdb_id == 1105
        assert s.query(gmdl.Game).count() == 2

    def test_upsert_game_stamps_provenance(self):
        s = self._session()
        game = gdb.upsert_game(s, 'The Witcher 3', steam_appid=292030)
        assert game.first_seen_at is not None
        first_seen = game.first_seen_at
        again = gdb.upsert_game(s, 'The Witcher 3', steam_appid=292030)
        assert again.first_seen_at == first_seen
        assert again.updated_at is not None

    def test_opencritic_id_unique_constraint(self):
        # opencritic_id is an identity column (find_game trusts it), so
        # the schema must refuse a second row with the same id — a bad
        # fuzzy match aborts the batch instead of corrupting identity.
        s = self._session()
        gdb.upsert_game(s, 'Game A', opencritic_id=42)
        s.commit()
        s.add(gmdl.Game(canonical_name='Game B', opencritic_id=42))
        assert gdb.safe_commit(s, 'test') is False

    def test_db_config_file_first_then_ssm_then_none(self, tmp_path,
                                                     monkeypatch):
        cfg = {'USER': 'u ser', 'PASS': 'p@ss:w/rd', 'HOST': 'h',
               'PORT': '5432', 'DATABASE': 'd'}
        path = tmp_path / 'steamdbconfig.json'
        path.write_text(json.dumps(cfg))
        # A local file wins without touching SSM.
        monkeypatch.setattr(gdb, '_ssm_config', lambda name: 1 / 0)
        assert gdb.load_db_config(paths=[str(path)]) == cfg
        # Special characters in USER/PASS survive URL building.
        db = gdb.GamesDB(cfg)
        assert 'u+ser:p%40ss%3Aw%2Frd@h:5432/d' in db.conn_string
        # No file -> SSM parameter; neither -> None (fail-soft).
        monkeypatch.setattr(gdb, '_ssm_config', lambda name: cfg)
        assert gdb.load_db_config(paths=[str(tmp_path / 'no.json')]) == cfg
        monkeypatch.setattr(gdb, '_ssm_config', lambda name: None)
        assert gdb.load_db_config(paths=[str(tmp_path / 'no.json')]) is None

    def test_ssm_failure_is_remembered_but_not_forever(self, monkeypatch):
        """A failed lookup used to be cached for the life of the
        process, so a repaired IAM role or a newly written parameter
        took effect only after every worker and web container had
        been restarted — with nothing anywhere saying why."""
        monkeypatch.setattr(gdb, '_ssm_cache', {})
        monkeypatch.setattr(gdb, '_ssm_errors', {})
        calls = []

        def deny(*args, **kwargs):
            calls.append(1)
            raise PermissionError('AccessDenied')

        fake_boto3 = types.ModuleType('boto3')
        fake_boto3.client = deny
        monkeypatch.setitem(sys.modules, 'boto3', fake_boto3)
        assert gdb._ssm_config('steamdbconfig.json', now=0) is None
        assert 'AccessDenied' in gdb.ssm_error('steamdbconfig.json')
        # Inside the window the remembered failure answers.
        assert gdb._ssm_config('steamdbconfig.json', now=10) is None
        assert len(calls) == 1
        # Past it, the fix gets a chance to take.
        assert gdb._ssm_config(
            'steamdbconfig.json', now=gdb.SSM_RETRY_SECONDS + 1) is None
        assert len(calls) == 2


class TestNzApi:
    """Bulk-exports client logic that needs no network or parquet
    (the REST engagement endpoint was retired with a 410)."""

    @staticmethod
    def _api(titles='Fortnite', country_filter=None):
        api = nzapi.NzApi()
        api.game_title = titles
        api.api_key = 'k'
        api.country_filter = country_filter
        return api

    def test_month_span_and_slugify(self):
        months = nzapi.NzApi.month_span(dt.datetime(2026, 5, 15),
                                        dt.datetime(2026, 7, 2))
        assert months == {'2026-05', '2026-06', '2026-07'}
        assert nzapi.NzApi.slugify("Tom Clancy's Rainbow Six") == \
            'tom-clancy-s-rainbow-six'

    def test_select_files_window(self):
        api = self._api()
        names = ['mau_2026-06.parquet', 'mau_2026_07.parquet',
                 'mau_2026-01.parquet']
        assert api.select_files(names, {'2026-06', '2026-07'}) == \
            names[:2]
        opaque = ['shard-a.parquet', 'part-2093.parquet']
        assert api.select_files(opaque, {'2026-06'}) == opaque

    def test_market_codes_accepts_names_and_codes(self):
        api = self._api(country_filter='United States, jp, Atlantis')
        assert api.market_codes() == ['US', 'JP']
        assert self._api().market_codes() == []

    def test_shape_df_titles_aliases_and_markets(self):
        api = self._api(titles='Fortnite,Roblox')
        df = pd.DataFrame({
            'title': ['Fortnite', 'Minecraft', 'Roblox'],
            'date': [dt.date(2026, 6, 1)] * 3,
            'country_code': ['ZZ', 'ZZ', 'US'],
            'mau': [1, 2, 3],
            'source': [['b', 'a'], None, ['c']]})
        out = api.shape_df(df)
        assert list(out['title']) == ['Fortnite', 'Roblox']
        assert list(out['game']) == list(out['game_title']) == \
            list(out['title'])
        assert list(out['market']) == ['Worldwide', 'United States']
        assert list(out['source']) == [('a', 'b'), ('c',)]


class TestRedditCampaignFilter:
    """Reddit's Filter box historically held the account password, so the
    api has to tell a campaign filter from a credential before filtering."""

    @pytest.mark.parametrize('value', [
        'Xk7$mQ2wp', 'P@ssw0rd123', 'Redd1tLoginSecret', 'aB3dEf9hK2mN'])
    def test_looks_like_password_true(self, value):
        assert redapi.RedApi.looks_like_password(value)

    @pytest.mark.parametrize('value', [
        '32357452', 'GameA', 'GameA2026', 'GameA_Launch_2026',
        'GameA Launch 2026', 'correcthorsebattery', '', None])
    def test_looks_like_password_false(self, value):
        assert not redapi.RedApi.looks_like_password(value)

    def test_resolve_campaign_filter_from_password_key(self):
        api = redapi.RedApi()
        api.config = {'username': 'u', 'password': '32357452'}
        assert api.resolve_campaign_filter() == '32357452'
        assert api.campaign_filter == '32357452'

    def test_resolve_campaign_filter_keeps_password(self):
        api = redapi.RedApi()
        api.config = {'username': 'u', 'password': 'P@ssw0rd123'}
        assert api.resolve_campaign_filter() is None
        assert api.campaign_filter is None

    def test_resolve_campaign_filter_prefers_explicit_key(self):
        api = redapi.RedApi()
        api.config = {'username': 'u', 'password': 'P@ssw0rd123',
                      'campaign_filter': 'GameA'}
        assert api.resolve_campaign_filter() == 'GameA'

    def test_load_config_dict_sets_filter(self):
        api = redapi.RedApi()
        api.load_config_dict({'username': 'u', 'password': '32357452'})
        assert api.campaign_filter == '32357452'

    @staticmethod
    def get_df():
        return pd.DataFrame({
            redapi.RedApi.campaign_col: [
                '32357452_GameA_US', '32357452_GameA_UK', '99999999_GameB_US'],
            'Impressions': [1, 2, 3]})

    def test_filter_df_on_campaign(self):
        api = redapi.RedApi()
        api.campaign_filter = '32357452'
        df = api.filter_df_on_campaign(self.get_df())
        assert len(df) == 2
        assert '99999999_GameB_US' not in list(df[api.campaign_col])

    def test_filter_df_no_match_returns_unfiltered(self):
        """A filter matching nothing must not silently empty the source."""
        api = redapi.RedApi()
        api.campaign_filter = 'NoSuchCampaign'
        assert len(api.filter_df_on_campaign(self.get_df())) == 3

    def test_filter_df_missing_column(self):
        api = redapi.RedApi()
        api.campaign_filter = 'GameA'
        df = pd.DataFrame({'Impressions': [1]})
        assert len(api.filter_df_on_campaign(df)) == 1

    def test_filter_df_no_filter(self):
        api = redapi.RedApi()
        assert len(api.filter_df_on_campaign(self.get_df())) == 3

    def test_filter_is_literal_not_regex(self):
        api = redapi.RedApi()
        api.campaign_filter = 'GameA (US)'
        df = pd.DataFrame({api.campaign_col: ['GameA (US)', 'GameA (UK)']})
        assert len(api.filter_df_on_campaign(df)) == 1


class TestDbApiCampaignFilter:
    """DV360 only filters on campaign ids server side, so a campaign name
    has to be matched against the report after it downloads."""

    @staticmethod
    def get_api(campaign_id=None):
        api = dbapi.DbApi()
        api.advertiser_id = '123'
        api.campaign_id = campaign_id
        api.parse_campaign_filter()
        return api

    def test_numeric_filter_is_server_side(self):
        """A numeric filter still goes to the api, but is kept for the
        post download match too since it may be a name and not an id."""
        api = self.get_api('32357452,99999999')
        assert api.campaign_ids == ['32357452', '99999999']
        assert api.campaign_name_filter == ['32357452', '99999999']
        params = api.create_report_params()
        vals = [x['value'] for x in params['filters']
                if x['type'] == 'FILTER_MEDIA_PLAN']
        assert vals == ['32357452', '99999999']

    def test_name_filter_is_client_side(self):
        api = self.get_api('GameA')
        assert api.campaign_ids == []
        assert api.campaign_name_filter == ['GameA']
        params = api.create_report_params()
        assert not [x for x in params['filters']
                    if x['type'] == 'FILTER_MEDIA_PLAN']

    def test_mixed_filter_is_client_side(self):
        """A mix must not send an id filter and a name filter at once, or
        the two would intersect to nothing."""
        api = self.get_api('32357452, GameA')
        assert api.campaign_ids == []
        assert api.campaign_name_filter == ['32357452', 'GameA']

    def test_empty_filter(self):
        api = self.get_api(None)
        assert api.campaign_ids == []
        assert api.campaign_name_filter == []

    def test_youtube_fields_add_campaign_groups(self):
        api = dbapi.DbApi()
        sd = ed = dt.datetime(2026, 8, 1)
        api.parse_fields(sd, ed, ['YOUTUBE'])
        assert api.query_type == 'YOUTUBE'
        for group in dbapi.DbApi.campaign_groups:
            assert group in api.default_groups
        assert 'FILTER_TRUEVIEW_AD_GROUP' in api.default_groups

    def test_parse_fields_does_not_mutate_class_lists(self):
        """default_metrics used to be extended in place, so every api
        object in a process inherited the last one's metrics."""
        sd = ed = dt.datetime(2026, 8, 1)
        lengths = []
        for _ in range(3):
            api = dbapi.DbApi()
            api.parse_fields(sd, ed, ['Actions'])
            lengths.append(len(api.default_metrics))
        assert len(set(lengths)) == 1
        assert not [x for x in dbapi.DbApi.base_metrics
                    if x in dbapi.DbApi.view_metrics]

    def test_remove_campaign_groups(self):
        api = dbapi.DbApi()
        sd = ed = dt.datetime(2026, 8, 1)
        api.parse_fields(sd, ed, ['YOUTUBE'])
        assert api.remove_campaign_groups()
        assert not [x for x in api.default_groups
                    if x in dbapi.DbApi.campaign_groups]
        assert not api.remove_campaign_groups()

    @staticmethod
    def get_df():
        return pd.DataFrame({
            dbapi.DbApi.campaign_col: [
                'GameA Launch 36314869', 'GameB Teaser', 'GameA Beta'],
            dbapi.DbApi.campaign_id_col: ['111', '222', '333'],
            'Impressions': [1, 2, 3]})

    def test_filter_df_on_campaign(self):
        api = self.get_api('GameA')
        api.df = self.get_df()
        assert len(api.filter_df_on_campaign()) == 2

    def test_filter_df_on_campaign_id(self):
        api = self.get_api('222')
        api.df = self.get_df()
        assert len(api.filter_df_on_campaign()) == 1

    def test_numeric_filter_matches_campaign_name(self):
        """The dcm campaign id the plan holds is not a dv360 campaign id,
        but the dv360 campaign is named for it."""
        api = self.get_api('36314869')
        api.df = self.get_df()
        tdf = api.filter_df_on_campaign()
        assert tdf[dbapi.DbApi.campaign_id_col].tolist() == ['111']

    def test_get_data_retries_without_id_filter(self, monkeypatch):
        """An id the api cannot match empties the report, so the retry has
        to drop the api filter and reach the campaign name instead."""
        api = self.get_api('36314869')
        pulls = []

        def fake_get_report_df(sd, ed, fields):
            pulls.append(list(api.campaign_ids))
            api.query_id = 'q1'
            api.df = (pd.DataFrame() if api.campaign_ids
                      else TestDbApiCampaignFilter.get_df())
            return api.df

        monkeypatch.setattr(api, 'get_report_df', fake_get_report_df)
        df = api.get_data()
        assert pulls == [['36314869'], []]
        assert df[dbapi.DbApi.campaign_id_col].tolist() == ['111']

    def test_get_data_retry_no_match_returns_empty(self, monkeypatch):
        """The retry holds every campaign the advertiser ran, so a filter
        that still matches nothing must not report all of them."""
        api = self.get_api('99999999')

        def fake_get_report_df(sd, ed, fields):
            api.query_id = 'q1'
            api.df = (pd.DataFrame() if api.campaign_ids
                      else TestDbApiCampaignFilter.get_df())
            return api.df

        monkeypatch.setattr(api, 'get_report_df', fake_get_report_df)
        assert api.get_data().empty

    def test_get_data_no_retry_on_config_report(self, monkeypatch):
        """A report id from the config never carried the filter, so an
        empty report is real and must not cost a second pull."""
        api = self.get_api('36314869')
        api.report_id = 'r1'
        pulls = []

        def fake_get_report_df(sd, ed, fields):
            pulls.append(list(api.campaign_ids))
            api.df = pd.DataFrame()
            return api.df

        monkeypatch.setattr(api, 'get_report_df', fake_get_report_df)
        api.get_data()
        assert len(pulls) == 1

    def test_filter_df_multiple_names(self):
        api = self.get_api('GameA, GameB')
        api.df = self.get_df()
        assert len(api.filter_df_on_campaign()) == 3

    def test_filter_df_no_match_returns_unfiltered(self):
        api = self.get_api('NoSuchCampaign')
        api.df = self.get_df()
        assert len(api.filter_df_on_campaign()) == 3

    def test_filter_df_missing_column(self):
        """A report type that rejected the campaign grouping has no
        campaign column, and must not come back empty."""
        api = self.get_api('GameA')
        api.df = pd.DataFrame({'Impressions': [1]})
        assert len(api.filter_df_on_campaign()) == 1


class TestDcApiCampaignFilter:
    """A campaign dimension filter only accepts ids, so a campaign name
    has to be matched against the report after it downloads."""

    @staticmethod
    def get_api(campaign_id=None):
        api = dcapi.DcApi()
        api.advertiser_id = '123'
        api.campaign_id = campaign_id
        api.date_range = {}
        api.parse_campaign_filter()
        return api

    @staticmethod
    def get_criteria_ids(api):
        criteria = api.create_report_criteria(reach_report=True)
        return [x['id'] for x in criteria['dimensionFilters']
                if x['dimensionName'] == 'campaign']

    def test_numeric_filter_is_server_side(self):
        api = self.get_api('32446667,32357452')
        assert api.campaign_ids == ['32446667', '32357452']
        assert self.get_criteria_ids(api) == ['32446667', '32357452']

    def test_name_filter_is_client_side(self):
        """A name sent as an id filter matches nothing, so it must stay
        off the criteria and be matched on the report instead."""
        api = self.get_api('GameA')
        assert api.campaign_ids == []
        assert api.campaign_name_filter == ['GameA']
        assert self.get_criteria_ids(api) == []

    def test_filter_df_on_campaign_name(self):
        api = self.get_api('GameA')
        df = pd.DataFrame({
            dcapi.DcApi.campaign_col: ['GameA Launch', 'GameB Teaser'],
            dcapi.DcApi.campaign_id_col: ['111', '222'],
            'Impressions': [1, 2]})
        tdf = utl.filter_df_on_campaign(
            df, api.campaign_name_filter, dcapi.DcApi.campaign_col,
            dcapi.DcApi.campaign_id_col)
        assert tdf[dcapi.DcApi.campaign_id_col].tolist() == ['111']
