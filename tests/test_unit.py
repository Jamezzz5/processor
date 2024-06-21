import os
import string
import pytest
import numpy as np
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vendormatrix as vm
import reporting.vmcolumns as vmc
import reporting.dictionary as dct
import reporting.dictcolumns as dctc
import reporting.calc as cal
import reporting.analyze as az
import reporting.export as exp
import reporting.expcolumns as exc
import reporting.azapi as azapi


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
                    '2022-01-01 00:00:00']
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
                   int_col: [np.int32(x) for x in float_list]}
        df = pd.DataFrame(df_dict)
        df[int_col] = df[int_col].astype('int32')
        for col in [str_col, float_col, date_col, int_col]:
            assert pd.testing.assert_series_equal(df[col], ndf[col]) is None

    def test_selenium_wrapper(self):
        sw = utl.SeleniumWrapper()
        assert sw.co.headless is True
        test_url = 'https://www.google.com/'
        sw.go_to_url(test_url, sleep=1)
        assert sw.browser.current_url == test_url
        sw.quit()

    def test_screenshot(self):
        sw = utl.SeleniumWrapper(headless=False)
        test_url = 'https://www.gamespot.com/'
        file_name = 'test.png'
        sw.take_screenshot(test_url, file_name=file_name)
        assert os.path.isfile(file_name)
        os.remove(file_name)
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


class TestApis:

    def make_fake_config(self, key_list, tmp_path_factory):
        json_data = {}
        for cur_key in key_list:
            json_data[cur_key] = '{} - value'.format(cur_key)
        file_name = '{}/config.json'.format(tmp_path_factory.mktemp("config"))
        return file_name, json_data

    def test_azapi(self, tmp_path_factory):
        api = azapi.AzApi()
        file_name, json_data = self.make_fake_config(
            api.key_list, tmp_path_factory)
        api.input_config()
        df = pd.DataFrame({'uploadid': ['a'], 'productname': ['b']})
        api.s3_write_file(df)


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
            if type(sorted_cols[key]) != dict:
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


class TestAnalyze:
    vm_df = None

    def test_check_flat(self):
        df = pd.DataFrame({
            vmc.clicks: [1],
            vmc.date: [44755],
            vmc.cost: [0],
            vmc.vendorkey: ['API_DCM_PoT2022BrandCampaign'],
            dctc.PN: [
                '28091057_IMGN_US_All_0_0_0_Flat_0_44768_Click Tracker_0.013_0_'
                'CPE_Brand Page_Brand_0.1_0_V_Cross Device_1080x1080_Video '
                'SK_IG In-Feed_Social Post_Social_All'],
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

    def test_flat_fix(self):
        first_click_date = '2022-07-25'
        cfs = az.CheckFlatSpends(az.Analyze())
        translation = dct.DictTranslationConfig()
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
            'Vendor Key':
                {0: 'API_DCM_Test', 1: 'API_Tiktok_Test',
                 2: 'API_Rawfile_Test', 3: 'Plan Net'},
            'FILENAME': {0: 'dcm_Test', 1: 'tiktok_Test.csv',
                         2: 'Rawfile_Test.csv', 3: 'plannet.csv'},
            'FIRSTROW': {0: 0, 1: 0, 2: 0, 3: 0},
            'LASTROW': {0: 0, 1: 0, 2: 0, 3: 0},
            'Full Placement Name': {0: 'Placement', 1: 'ad_name',
                                    2: 'ad_name', 3: 'mpCampaign|mpVendor'},
            'Placement Name': {0: 'Placement', 1: 'ad_name',
                               2: 'ad_name', 3: 'mpVendor'},
            'FILENAME_DICTIONARY': {0: 'dcm_dictionary_Test',
                                    1: 'tiktok_dictionary_Test.csv',
                                    2: 'Rawfile_dictionary_Test.csv',
                                    3: 'plannet_dictionary.csv'},
            'FILENAME_ERROR': {0: 'DCM_ERROR_REPORT.csv',
                               1: 'TIKTOK_ERROR_REPORT.csv',
                               2: 'Rawfile_ERROR_REPORT.csv',
                               3: 'PLANNET_ERROR_REPORT.csv'},
            'START DATE': {0: '7/18/2022', 1: '7/1/2022',
                           2: '7/1/2022', 3: ''},
            'END DATE': {0: '', 1: '7/27/2022', 2: '7/27/2022', 3: ''},
            'DROP_COLUMNS': {0: 'ALL', 1: 'ALL', 2: 'ALL', 3: ''},
            'AUTO DICTIONARY PLACEMENT': {0: 'Full Placement Name',
                                          1: 'Full Placement Name',
                                          2: 'Full Placement Name',
                                          3: 'Full Placement Name'},
            'AUTO DICTIONARY ORDER': {
                0: 'mpCampaign|mpVendor', 1: 'mpCampaign|mpVendor',
                2: 'mpCampaign|mpVendor', 3: 'mpCampaign|mpVendor'},
            'API_FILE': {0: 'dcapi_Test.json', 1: 'tikapi_Test.json',
                         2: 'tikapi_Test.json', 3: ''},
            'API_FIELDS': {0: '', 1: '', 2: '', 3: ''},
            'API_MERGE': {0: '', 1: '', 2: '', 3: ''},
            'TRANSFORM': {0: '', 1: '', 2: '', 3: ''},
            'HEADER': {0: '', 1: '', 2: '', 3: ''},
            'OMIT_PLAN': {0: '', 1: '', 2: '', 3: ''},
            'Date': {0: 'Date', 1: 'stat_datetime', 2: 'stat_datetime', 3: ''},
            'Impressions': {0: 'Impressions', 1: 'show_cnt',
                            2: 'show_cnt', 3: ''},
            'Clicks': {0: 'Clicks', 1: 'click_cnt', 2: 'click_cnt', 3: ''},
            'Net Cost': {0: '', 1: 'stat_cost', 2: 'stat_cost', 3: ''},
            'Video Views': {0: 'TrueView Views', 1: 'total_play',
                            2: 'total_play', 3: ''},
            'Video Views 25': {0: 'Video First Quartile Completions',
                               1: 'play_first_quartile',
                               2: 'play_first_quartile', 3: ''},
            'Video Views 50': {0: 'Video Midpoints', 1: 'play_midpoint',
                               2: 'play_midpoint', 3: ''},
            'Video Views 75': {0: 'Video Third Quartile Completions',
                               1: 'play_third_quartile',
                               2: 'play_third_quartile', 3: ''},
            'Video Views 100': {0: 'Video Completions', 1: 'play_over',
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
                             1: '', 2: '', 3: ''},
            'RULE_4_FACTOR': {0: '', 1: '', 2: '', 3: ''},
            'RULE_4_METRIC': {0: '', 1: '', 2: '', 3: ''},
            'RULE_4_QUERY': {0: '', 1: '', 2: '', 3: ''},
            'RULE_5_FACTOR': {0: '', 1: '', 2: '', 3: ''},
            'RULE_5_METRIC': {0: '', 1: '', 2: '', 3: ''},
            'RULE_5_QUERY': {0: '', 1: '', 2: '', 3: ''},
            'RULE_6_FACTOR': {0: '', 1: '', 2: '', 3: ''},
            'RULE_6_METRIC': {0: '', 1: '', 2: '', 3: ''},
            'RULE_6_QUERY': {0: '', 1: '', 2: '', 3: ''}
        }
        for key in vmc.datacol:
            if key not in vm_dict:
                vm_dict[key] = {0: '', 1: '', 2: ''}
        vm_df = pd.DataFrame(vm_dict)
        self.vm_df = vm_df
        return vm_df

    def test_double_fix_all_raw(self, test_vm):
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
            matrix.vm_df[vmc.vendorkey] == 'API_Tiktok_Test', vmc.clicks].item()
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

    def test_placement_not_in_mp(self):
        df = pd.DataFrame({
            dctc.VEN: {0: 'TikTok', 1: 'TikTok', 2: 'TikTok'},
            vmc.vendorkey: {0: 'API_Tiktok_Test', 1: 'API_Tiktok_Test',
                            2: vmc.api_mp_key},
            vmc.clicks: {0: 15.0, 1: 15.0, 2: 0},
            vmc.date: {0: '7/27/2022', 1: '7/27/2022', 2: '7/27/2022'},
            dctc.PN: {0: 'Test', 1: 'Test1', 2: 'Test'}})
        df = utl.data_to_type(df, date_col=[vmc.date, dctc.PD])
        cpmp = az.CheckPlacementsNotInMp(az.Analyze())
        df = cpmp.find_placements_not_in_mp(df)
        assert 'Test1' in df[dctc.PN].values
        assert 'Test' not in df[dctc.PN].values

    def test_placement_not_in_mp_empty(self):
        df = pd.DataFrame({
            dctc.VEN: {0: 'TikTok', 1: 'TikTok'},
            vmc.vendorkey: {0: 'API_Tiktok_Test', 1: 'API_Tiktok_Test'},
            vmc.clicks: {0: 15.0, 1: 15.0},
            vmc.date: {0: '7/27/2022', 1: '7/27/2022'},
            dctc.PN: {0: 'Test', 1: 'Test1'}})
        df = utl.data_to_type(df, date_col=[vmc.date, dctc.PD])
        cpmp = az.CheckPlacementsNotInMp(az.Analyze())
        df = cpmp.find_placements_not_in_mp(df)
        assert df.empty

    def test_all_placement_in_mp(self):
        df = pd.DataFrame({
            dctc.VEN: {0: 'TikTok', 1: 'TikTok', 2: 'TikTok', 3: 'TikTok'},
            vmc.vendorkey: {0: 'API_Tiktok_Test', 1: 'API_Tiktok_Test',
                            2: vmc.api_mp_key, 3: vmc.api_mp_key},
            vmc.clicks: {0: 15.0, 1: 15.0, 2: 0, 3: 0},
            vmc.date: {0: '7/27/2022', 1: '7/27/2022', 2: '7/27/2022',
                       3: '7/27/2022'},
            dctc.PN: {0: 'Test', 1: 'Test1', 2: 'Test', 3: 'Test1'}})
        df = utl.data_to_type(df, date_col=[vmc.date, dctc.PD])
        cpmp = az.CheckPlacementsNotInMp(az.Analyze())
        df = cpmp.find_placements_not_in_mp(df)
        assert 'Test' not in df[dctc.PN].values
        assert 'Test1' not in df[dctc.PN].values

    def test_find_double_counting_empty(self):
        df = pd.DataFrame()
        cdc = az.CheckDoubleCounting(az.Analyze())
        df = cdc.find_metric_double_counting(df)
        assert df.empty
        
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

    def test_all_analysis_on_empty_df(self):
        aly = az.Analyze(df=pd.DataFrame(), matrix=vm.VendorMatrix())
        aly.do_all_analysis()

    def test_all_analysis_on_header_df(self):
        df = pd.DataFrame(columns=[
            vmc.btnclick, vmc.clicks, vmc.date, dctc.FPN, vmc.impressions,
            vmc.cost, dctc.PNC, vmc.purchase, vmc.reach, vmc.revenue, dctc.UNC,
            vmc.vendorkey, dctc.AD, dctc.AF, dctc.AM, dctc.AR, dctc.AT,
            dctc.AGE, dctc.AGY, dctc.AGF, dctc.BUD, dctc.BM, dctc.BR, dctc.BR2,
            dctc.BR3, dctc.BR4, dctc.BR5, dctc.CTA, dctc.CAM, dctc.CP, dctc.CQ,
            dctc.CTIM, dctc.CT, dctc.CH, dctc.URL, dctc.CLI, dctc.COP, dctc.COU,
            dctc.CRE, dctc.CD, dctc.LEN, dctc.LI, dctc.CM, dctc.CURL, dctc.DT1,
            dctc.DT2, dctc.DEM, dctc.DL1, dctc.DL2, dctc.DUL, dctc.ED, dctc.ENV,
            dctc.FAC, dctc.FOR, dctc.FRA, dctc.GEN, dctc.GT, dctc.GTF, dctc.HL1,
            dctc.HL2, dctc.KPI, dctc.MC, dctc.MIS, dctc.MIS2, dctc.MIS3,
            dctc.MIS4, dctc.MIS5, dctc.MIS6, dctc.MN, dctc.MT, dctc.PKD,
            dctc.PD, dctc.PD2, dctc.PD3, dctc.PD4, dctc.PD5, dctc.PLD, dctc.PN,
            dctc.PLA, dctc.PRD, dctc.PRN, dctc.REG, dctc.RFM, dctc.RFR,
            dctc.RFT, dctc.RET, dctc.SRV, dctc.SIZ, dctc.SD, dctc.TAR, dctc.TB,
            dctc.TP, dctc.TPB, dctc.TPF, dctc.VEN, dctc.VT, dctc.VFM, dctc.VFR,
            dctc.PFPN])
        aly = az.Analyze(df=df, matrix=vm.VendorMatrix())
        aly.do_all_analysis()


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
    'SUM("lqadb"."event"."revenue_userstart_30day") AS "revenue_userstart_30day"',
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


class TestExport():

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
