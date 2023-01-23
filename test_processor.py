import os
import string

import pandas
import pytest
import numpy as np
import pandas as pd

import reporting.analyze
import reporting.utils as utl
import reporting.vendormatrix as vm
import reporting.vmcolumns as vmc
import reporting.dictionary as dct
import reporting.dictcolumns as dctc
import reporting.calc as cal
import reporting.analyze as aly


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


class TestApis:
    pass


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
            (['mpTargeting:::0:::_', 'mpData Type 1:::1:::_',
              'mpTargeting:::2:::_'],
             {dctc.TAR: {dctc.TB: ['mpTargeting:::0:::_'],
                         dctc.GT: ['mpTargeting:::2:::_']},
              'bad_values': ['mpData Type 1:::1:::_']},
             True, False, True)
        ],
        ids=['empty', 'standard', 'bad_delim', 'no_bad_delim', 'missing',
             'bad_value']
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
             {dctc.TAR: ['b_0_c']})
        ],
        ids=['empty', 'bad_delim', 'missing', 'bad_value', 'non_relation',
             'duplicate']
    )
    def test_auto_combine(self, columns, expected_data):
        df = pd.DataFrame()
        for i, col in enumerate(columns):
            df[col] = [string.ascii_lowercase[i]]
        output = self.dic.auto_combine(df, self.mock_rc_auto)
        expected = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(output, expected, check_like=True)


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
        cfs = aly.CheckFlatSpends(aly.Analyze())
        df = cfs.find_missing_flat_spend(df)
        assert cfs.placement_date_error in df[cfs.error_col].values
        assert cfs.missing_rate_error in df[cfs.error_col].values

    def test_empty_flat(self):
        df = pd.DataFrame()
        analyze = aly.Analyze()
        cfs = aly.CheckFlatSpends(analyze)
        df = cfs.find_missing_flat_spend(df)
        assert df.empty

    def test_flat_fix(self):
        first_click_date = '2022-07-25'
        cfs = aly.CheckFlatSpends(aly.Analyze())
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
        cfs = aly.CheckFlatSpends(aly.Analyze())
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
        cdc = aly.CheckDoubleCounting(aly.Analyze(matrix=matrix))
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
        cdc = aly.CheckDoubleCounting(aly.Analyze(matrix=matrix))
        aly_dict = pd.DataFrame()
        df = cdc.fix_analysis(aly_dict, write=False)
        assert df.empty

    def test_double_fix_all_server(self, test_vm):
        rule_1_query = 'RULE_1_QUERY'
        vm_df = self.vm_df
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_df)
        cdc = aly.CheckDoubleCounting(aly.Analyze(matrix=matrix))
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
        cdc = aly.CheckDoubleCounting(aly.Analyze())
        df = cdc.find_metric_double_counting(df)
        assert cdc.double_counting_all in df[cdc.error_col].values
        assert 'API_Tiktok_Test' in df[vmc.vendorkey][0]
        assert 'API_Rawfile_Test' in df[vmc.vendorkey][0]

    def test_find_double_counting_empty(self):
        df = pd.DataFrame()
        cdc = aly.CheckDoubleCounting(aly.Analyze())
        df = cdc.find_metric_double_counting(df)
        assert df.empty

    def test_check_package_capping_base(self):
        cap_path = 'raw_data/cap_test.csv'
        cfg_path = 'config/cap_config.csv'
        cfg_df = {'file_name': ['raw_data/cap_test.csv'],
                  'file_dim': ['mpAgency'],
                  'file_metric': ['Net Cost (Capped)'],
                  'processor_dim': ['mpAgency'],
                  'processor_metric': ['Planned Net Cost']}
        cfg_df = pd.DataFrame(cfg_df)
        cfg_df.to_csv(cfg_path, index=False)
        cap_df = {'mpAgency': ['Liquid Advertising'],
                  'Net Cost (Capped)': [300.0]}
        cap_df = pd.DataFrame(cap_df)
        cap_df.to_csv(cap_path, index=False)
        os.system('python main.py --noprocess --analyze')
        cap_test = pandas.read_csv(cap_path)
        assert not cap_test.duplicated(keep=False).empty

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
