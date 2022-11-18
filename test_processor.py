import os
import string
import pytest
import numpy as np
import pandas as pd
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.dictionary as dct
import reporting.dictcolumns as dctc


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