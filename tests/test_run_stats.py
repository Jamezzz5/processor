"""The run-step recorder: what ``config/run_stats.json`` holds after
each shape of import step, and the cross-module filter-stat record.
"""
import json
import logging
import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import pandas as pd

import processor.reporting.utils as utl
import processor.reporting.importhandler as ih
import processor.reporting.vmcolumns as vmc
import reporting.utils as other_utl


def records(path):
    with open(os.path.join(str(path), utl.run_stats_file)) as f:
        return json.load(f)


class FakeMatrix(object):
    def vendor_set(self, vk):
        return {vmc.apifile: 'cfg.json',
                vmc.startdate: pd.Timestamp('2026-10-01'),
                vmc.enddate: pd.Timestamp('2026-10-02'),
                vmc.apifields: ['nan'], vmc.apimerge: float('nan'),
                vmc.filename: f'{vk}.csv', vmc.firstrow: 0,
                vmc.lastrow: 0, vmc.date: ['Date']}


class OkApi(object):
    sw = None

    def __init__(self):
        self.calls = 0

    def input_config(self, config):
        pass

    def get_data(self, sd=None, ed=None, fields=None):
        self.calls += 1
        logging.warning('Rate limit reached, pausing for 60s')
        return pd.DataFrame({'Date': ['2026-10-01'], 'Impressions': [1]})


class SecondCallBoomApi(OkApi):
    def get_data(self, sd=None, ed=None, fields=None):
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError('boom')
        return pd.DataFrame({'Date': ['2026-10-01'], 'Impressions': [1]})


class FilterThenBoomApi(OkApi):
    """Filters through its own ``reporting.utils``, then dies."""

    def get_data(self, sd=None, ed=None, fields=None):
        df = pd.DataFrame({'Campaign': ['A One', 'B Two']})
        other_utl.filter_df_on_campaign(df, ['A'], 'Campaign')
        raise RuntimeError('boom')


class ExitApi(OkApi):
    def __init__(self, message):
        super().__init__()
        self.message = message

    def get_data(self, sd=None, ed=None, fields=None):
        if self.message:
            logging.error(self.message)
        sys.exit(0)


class SeleniumApi(OkApi):
    def __init__(self):
        super().__init__()
        self.sw = Mock()

    def get_data(self, sd=None, ed=None, fields=None):
        raise RuntimeError('page gone')


class NoConfigApi(OkApi):
    def input_config(self, config):
        raise FileNotFoundError('cfg.json missing')


@pytest.fixture
def run_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path / utl.config_path


class TestImportSteps:
    def test_ok_step_records_time_rows_and_rate_limit(self, run_dir):
        ih.ImportHandler('all', FakeMatrix()).api_calls(
            ['API_Facebook_a'], OkApi())
        (step,) = records(run_dir)
        assert (step['step'], step['api_key'], step['vendor_key'],
                step['status'], step['rows']) == (
            'import', 'Facebook', 'API_Facebook_a', 'ok', 1)
        assert step['seconds'] >= 0
        assert (step['warnings'], step['rate_limited']) == (1, 1)
        assert step['pid'] == os.getpid()
        assert 'error_type' not in step

    def test_a_key_that_raises_leaves_no_filter_record(self, run_dir):
        with pytest.raises(RuntimeError, match='boom'):
            ih.ImportHandler('all', FakeMatrix()).api_calls(
                ['API_Facebook_a'], FilterThenBoomApi())
        assert utl.drain_campaign_filter_stats() == {}

    def test_failure_keeps_earlier_steps_and_still_raises(self, run_dir):
        handlers = list(logging.getLogger().handlers)
        with pytest.raises(RuntimeError, match='boom'):
            ih.ImportHandler('all', FakeMatrix()).api_calls(
                ['API_Facebook_a', 'API_Facebook_b'], SecondCallBoomApi())
        first, second = records(run_dir)
        assert first['status'] == 'ok'
        assert (second['status'], second['error_type'],
                second['error_text']) == ('failed', 'RuntimeError', 'boom')
        assert second['seconds'] is not None
        assert logging.getLogger().handlers == handlers

    @pytest.mark.parametrize('message', [
        'Facebook Access Token invalid. Aborting.', ''])
    def test_system_exit_is_recorded_and_still_propagates(self, run_dir,
                                                           message):
        with pytest.raises(SystemExit):
            ih.ImportHandler('all', FakeMatrix()).api_calls(
                ['API_Facebook_a'], ExitApi(message))
        (step,) = records(run_dir)
        assert (step['status'], step['error_type'], step['error_text']) == (
            'failed', 'SystemExit', message)

    def test_selenium_branch_records_failure_and_carries_on(self, run_dir):
        api = SeleniumApi()
        ih.ImportHandler('all', FakeMatrix()).api_calls(
            ['API_DV_a', 'API_DV_b'], api)
        assert api.sw.quit.called
        first, second = records(run_dir)
        assert first['status'] == second['status'] == 'failed'
        assert first['error_text'] == 'page gone'
        assert first['rows'] == 0

    def test_missing_config_is_skipped_with_the_reason(self, run_dir):
        ih.ImportHandler('all', FakeMatrix()).api_calls(
            ['API_Tiktok_a'], NoConfigApi())
        (step,) = records(run_dir)
        assert step['status'] == 'skipped'
        assert 'cfg.json' in step['error_text']
        assert step['seconds'] is not None


class TestTimedStep:
    def test_phase_and_vendor_steps_share_one_file(self, tmp_path):
        config = str(tmp_path)
        with utl.TimedStep('import', config_path_dir=config):
            with utl.TimedStep('import', 'Facebook', 'API_Facebook_a',
                               config_path_dir=config) as inner:
                inner.rows = 7
        phase, vendor = records(tmp_path)
        assert (phase['vendor_key'], vendor['vendor_key']) == (
            '', 'API_Facebook_a')
        assert phase['seconds'] >= vendor['seconds']
        assert vendor['rows'] == 7
        assert phase['token'] != vendor['token']

    def test_a_step_is_on_disk_while_it_runs(self, tmp_path):
        with utl.TimedStep('process', config_path_dir=str(tmp_path)):
            (step,) = records(tmp_path)
        assert (step['status'], step['seconds']) == ('running', None)

    def test_reset_removes_the_previous_run(self, tmp_path):
        config = str(tmp_path)
        with utl.TimedStep('export', config_path_dir=config):
            pass
        utl.reset_run_stats(config_path_dir=config)
        assert not os.path.exists(os.path.join(config, utl.run_stats_file))
        utl.reset_run_stats(config_path_dir=config)

    @pytest.mark.parametrize('attrs, status', [
        ({'response': SimpleNamespace(status_code=403)}, 403),
        ({'http_status': lambda: 400}, 400),
        ({'resp': SimpleNamespace(status=429)}, 429),
        ({}, None),
        ({'http_status': lambda: 'bad'}, None)])
    def test_error_http_status_reads_each_sdk_shape(self, attrs, status):
        error = RuntimeError('http')
        vars(error).update(attrs)
        assert utl.error_http_status(error) == status

    def test_rate_limit_tokens_are_whole_words(self):
        counter = utl.StepLogCounter()
        for text in ('Too Many Requests', 'rate-limit hit', '429 returned',
                     'throttled', 'imported 1429 rows'):
            counter.emit(logging.LogRecord(
                'x', logging.WARNING, '', 0, text, (), None))
        assert (counter.warnings, counter.rate_limited) == (5, 4)


def test_filter_record_crosses_the_two_module_objects():
    """The API classes import ``reporting.utils``; the import handler
    imports ``processor.reporting.utils``."""
    assert other_utl is not utl
    utl.drain_campaign_filter_stats()
    df = pd.DataFrame({'Campaign': ['A One', 'B Two']})
    other_utl.filter_df_on_campaign(df, ['A'], 'Campaign')
    stat = utl.drain_campaign_filter_stats()
    assert (stat['matched'], stat['total']) == (1, 2)
    assert utl.drain_campaign_filter_stats() == {}
