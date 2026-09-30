import io
import os
import re
import sys
import json
import time
import yaml
import types
import socket
import string
import pytest
import logging
import threading
import subprocess
import http.server
import numpy as np
import pandas as pd
import datetime as dt
from unittest.mock import Mock
import urllib3.exceptions as url_ex
from PIL import Image, ImageDraw
import selenium.common.exceptions as ex
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
import psycopg2
import processor.reporting.export as exp
import processor.reporting.expcolumns as exc
import processor.reporting.azapi as azapi
import processor.reporting.redapi as redapi
import processor.reporting.awapi as awapi
import processor.reporting.amzapi as amzapi
import processor.reporting.gaapi as gaapi
import processor.reporting.fbapi as fbapi
import processor.reporting.ssapi as ssapi
import processor.reporting.pmapi as pmapi
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
AD_CREATIVE = (
    '<html><body><script>window.clicked = 0;</script>'
    '<a href="https://ad.doubleclick.net/ddm/clk/1;2;adurl='
    'https%3A%2F%2Fwww.landing.example.com%2Fbuy"'
    ' onclick="window.clicked = 1"><img alt="Buy Game X" src="data:image/'
    'gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7">'
    '</a></body></html>')
AD_PAGE = (
    '<html><body style="margin:0"><script>window.clicked = 0;'
    "document.addEventListener('click', () => { window.clicked += 1; });"
    '</script><div style="height:1600px"></div>'
    '<iframe id="google_ads_iframe_1" src="creative.html" width="300"'
    ' height="250" style="border:0"></iframe>'
    '<iframe id="plain" src="creative.html" width="400" height="200"'
    ' style="border:0"></iframe>'
    '<div id="div-gpt-ad-1" style="width:0;height:0"></div>'
    '</body></html>')
TALL_PAGE = (
    '<html><body><div style="height:5000px"></div><script>'
    'window.scrolls = 0;'
    "window.addEventListener('scroll', () => { window.scrolls += 1; });"
    '</script></body></html>')
NATIVE_PAGE = (
    '<html><body style="margin:0"><script>window.clicked = 0;'
    "document.addEventListener('click', () => { window.clicked += 1; });"
    '</script><div id="taboola-below-article" style="width:600px;'
    'height:300px"><iframe src="creative.html" width="600" height="280"'
    ' style="border:0"></iframe></div>'
    '<div class="ad-slot" style="width:300px;height:250px"><img alt="Play '
    'Game Y" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAA'
    'ALAAAAAABAAEAAAIBRAA7" width="300" height="250"></div>'
    '<div class="advert-wrapper" style="width:1600px;height:2000px"></div>'
    '<video width="640" height="360" style="display:block"></video>'
    '</body></html>')
STORY_PARAGRAPH = ('<p style="font-size:40px;color:#000">The quick brown '
                   'fox jumps over the lazy dog and keeps running.</p>')
LONG_ARTICLE = ('<html><head><title>Story</title></head>'
                f'<body style="margin:0">{STORY_PARAGRAPH * 120}'
                '</body></html>')
SMOOTH_PAGE = (
    '<html style="scroll-behavior:smooth"><body style="margin:0">'
    '<h1 style="font-size:90px;margin:0">Top of the page</h1>'
    f'{STORY_PARAGRAPH * 200}<script>window.deepest = 0;'
    "window.addEventListener('scroll', () => {"
    ' window.deepest = Math.max(window.deepest, window.scrollY); });'
    '</script></body></html>')
CF_INTERSTITIAL = (
    '<html><head><title>Just a moment...</title></head><body>'
    '<h1>games.example.net</h1><p>Performing security verification</p>'
    '<p>This website uses a security service to protect itself.</p>'
    '</body></html>')
BLOCKED_PAGE = (
    '<html><head><title>Attention Required!</title></head><body>'
    '<h1>Sorry, you have been blocked</h1>'
    '<p>You are unable to access example.com</p></body></html>')
LOGIN_PAGE = (
    '<html><head><title>Log in</title></head><body>'
    '<h1>Log in to continue</h1><input type="password"></body></html>')
CONSENT_MODAL = (
    f'<html><body><h1>Real news</h1>{"<p>An article paragraph.</p>" * 20}'
    '<div id="onetrust-banner-sdk" role="dialog" style="position:fixed;'
    'inset:0;background:#fff"><p>We and our partners use cookies. By '
    'clicking Agree you consent to our privacy policy.</p>'
    '<button id="onetrust-accept-btn-handler" onclick="window.picked='
    '\'cmp\';document.getElementById(\'onetrust-banner-sdk\').remove()">'
    'Accept all</button></div></body></html>')
PAY_OR_CONSENT = (
    f'<html><body><h1>Real news</h1>{"<p>An article paragraph.</p>" * 20}'
    '<style>.jad_cmp_paywall_button-cookies::after '
    '{content: "J\\2019 accepte"}</style>'
    '<div role="dialog" id="wall" style="position:fixed;inset:0;'
    'background:#fff"><p>Exprimez vos choix. Accéder au site '
    'gratuitement en acceptant les cookies publicitaires.</p>'
    '<button class="jad_cmp_paywall_button '
    'jad_cmp_paywall_button-subscription" onclick="window.picked='
    '\'subscribe\'"></button>'
    '<button class="jad_cmp_paywall_button jad_cmp_paywall_button-cookies"'
    ' style="width:120px;height:40px" onclick="window.picked=\'cookies\';'
    'document.getElementById(\'wall\').remove()"></button></div>'
    '</body></html>')
LATE_PAINT = (
    '<html><body style="margin:0;background:#eef2f7"><script>'
    "setTimeout(() => { document.body.innerHTML = `<h1 style=\"font-size:"
    "120px;color:#000\">Painted at last</h1><p>${'words '.repeat(200)}"
    "</p>`; }, 1500);"
    '</script></body></html>')
CLEARING_CHECK = (
    '<html><head><title>Just a moment...</title></head><body>'
    '<h1>games.example.net</h1><p>Performing security verification</p>'
    '<script>window.touched = 0;'
    "['click', 'keydown'].forEach(t => document.addEventListener(t,"
    ' () => { window.touched += 1; }));'
    "setTimeout(() => { document.title = 'Example Games';"
    ' document.body.innerHTML = `<h1 style="font-size:120px;color:#000">'
    "Real news</h1><p>${'words '.repeat(200)}</p>`; }, 3000);"
    '</script></body></html>')
PARTIAL_HEAD = (
    '<html><head><title>Slow news</title></head><body style="margin:0">'
    '<h1 style="font-size:120px;color:#000">Partly here</h1>'
    f'{STORY_PARAGRAPH * 5}')
THIN_BANNER = (
    f'<html><body style="margin:0"><h1>Real news</h1>{STORY_PARAGRAPH * 5}'
    '<div role="dialog" style="position:fixed;left:0;right:0;bottom:0;'
    'height:60px;background:#eee"><p>We use cookies on this site.</p>'
    '</div></body></html>')
SIGNUP_PROMPT = (
    f'<html><body style="margin:0"><h1>Real news</h1>{STORY_PARAGRAPH * 5}'
    '<div role="dialog" style="position:fixed;inset:20%;background:#fff;'
    'border:2px solid #000"><p>Sign up to keep reading</p>'
    '<button onclick="window.picked=\'account\'">Continue with Google'
    '</button><button aria-label="Close" onclick="window.picked=\'closed\';'
    'this.parentNode.remove()">x</button></div></body></html>')
SKIP_PROMPT = (
    f'<html><body style="margin:0"><h1>Real news</h1>{STORY_PARAGRAPH * 5}'
    '<div role="dialog" style="position:fixed;left:10px;top:10px;'
    'width:80px;height:30px;background:#eee">Unmute</div>'
    '<div role="dialog" aria-modal="true" style="position:fixed;left:40%;'
    'top:30%;width:20%;height:40%;background:#fff;border:2px solid #000">'
    '<div role="button" onclick="window.picked=\'skipped\';'
    'this.parentNode.remove()">Skip</div><h2>Log in to TikTok</h2>'
    '<button onclick="window.picked=\'account\'">Continue with Google'
    '</button></div></body></html>')
ARTICLE_PAGE = (
    '<html><head><base href="https://www.example.com/news/">'
    '<title>Tab title | Example</title>'
    '<meta property="og:title" content="Open graph title">'
    '<meta property="og:description" content="Open   graph summary">'
    '<meta property="og:image" content="https://cdn.example.com/og.jpg">'
    '<meta property="og:site_name" content="Example Site">'
    '<meta name="twitter:title" content="Twitter title">'
    '<meta property="article:section" content="Reviews">'
    '<meta property="article:tag" content="rpg">'
    '<meta property="article:tag" content="indie">'
    '<meta name="description" content="Plain summary">'
    '<meta name="author" content="Meta Author">'
    '<link rel="canonical" href="/news/big-story">'
    '<link rel="alternate" type="application/rss+xml" href="/feed.xml">'
    '<link rel="alternate" type="application/atom+xml"'
    ' href="https://www.example.com/atom.xml">'
    '<link rel="alternate" hreflang="fr" href="/fr/news/big-story">'
    '<script type="application/ld+json">{not json</script>'
    '<script type="application/ld+json">{"@context": "https://schema.org",'
    ' "@graph": [{"@type": "WebSite", "name": "Example"},'
    ' {"@type": "NewsArticle", "headline": "Structured headline",'
    ' "datePublished": "2026-09-01T08:00:00Z",'
    ' "author": [{"@type": "Person", "name": "Ada Writer"},'
    ' {"@type": "Person", "name": "Bo Editor"}],'
    ' "articleBody": "never read"}]}</script>'
    '</head><body><h1>Structured headline</h1>'
    '<time datetime="2026-08-30">30 August</time>'
    f'{STORY_PARAGRAPH}</body></html>')
HOME_PAGE = (
    '<html><head><base href="https://www.example.com/"></head><body>'
    '<h2><a href="/articles/big-story">The biggest   story of the day</a>'
    '</h2><h2><a href="/tag/rpg">Every role playing game</a></h2>'
    '<h3><a href="/articles/second-story#comments">A second story worth '
    'reading</a></h3></body></html>')


def _write_page(tmp_path, name, html):
    """Write an html fixture and return it as a file:// url."""
    page = tmp_path / name
    page.write_text(html, encoding='utf-8')
    return page.as_uri()


class _StallingHandler(http.server.BaseHTTPRequestHandler):
    """Sends ``body`` then holds the connection open, as a stalled
    origin does."""

    body = PARTIAL_HEAD.encode('utf-8')

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(self.body)
        self.wfile.flush()
        self.server.release.wait(60)

    def log_message(self, format, *args):
        pass


class _SilentHandler(_StallingHandler):
    """Holds the connection open without answering at all."""

    def do_GET(self):
        self.server.release.wait(60)


class _EchoHandler(_StallingHandler):
    """Answers with a page and keeps each request's headers on
    ``seen``."""

    seen = []

    def do_GET(self):
        _EchoHandler.seen.append(dict(self.headers))
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(b'<html><body><h1>Seen</h1></body></html>')


def _start_server(handler):
    """A local http server on a free port, serving from a thread."""
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
    server.daemon_threads = True
    server.release = threading.Event()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _stop_server(server):
    server.release.set()
    server.shutdown()
    server.server_close()


def _server_url(server):
    return f'http://127.0.0.1:{server.server_address[1]}/'


def _closed_port_url():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return f'http://127.0.0.1:{sock.getsockname()[1]}/'


class _ScriptedBrowser(object):
    """Driver answering every script with ``answer`` and every shot
    with ``png``; it has nothing to click."""

    def __init__(self, answer, png=b''):
        self.answer = answer
        self.png = png
        self.scripts = []
        self.shots = 0

    def execute_script(self, script, *args):
        self.scripts.append(script)
        return self.answer

    def get_screenshot_as_png(self):
        self.shots += 1
        return self.png


class _IdentityBrowser(object):
    """Driver answering ``agent`` and ``hints``, keeping every devtools
    command and page it was sent."""

    def __init__(self, agent, hints=None):
        self.agent = agent
        self.hints = hints
        self.commands = []
        self.pages = []
        self.capabilities = {}

    def execute_cdp_cmd(self, name, params):
        self.commands.append((name, params))
        return {}

    def execute_script(self, script, *args):
        return self.agent

    def execute_async_script(self, script, *args):
        if isinstance(self.hints, Exception):
            raise self.hints
        return self.hints

    def get(self, url):
        self.pages.append(url)

    def named(self, name):
        return [params for sent, params in self.commands if sent == name]


LOCKED_PROFILE = ('session not created: probably user data directory is '
                  'already in use, please specify a unique value for '
                  '--user-data-dir argument')


class _ProfileLauncher(object):
    """``launch_browser`` refusing with ``error`` the first ``refusals``
    launches that name a profile folder."""

    def __init__(self, browser, error=LOCKED_PROFILE, refusals=2):
        self.browser = browser
        self.error = error
        self.refusals = refusals
        self.launches = []

    def __call__(self, co):
        self.launches.append(list(co.arguments))
        named = any(x.startswith('--user-data-dir=') for x in co.arguments)
        if named and self.refusals:
            self.refusals -= 1
            raise ex.SessionNotCreatedException(self.error)
        return self.browser


def _bare_wrapper(**attrs):
    """A ``SeleniumWrapper`` that never launched a browser, carrying
    ``attrs`` over its class defaults."""
    sw = utl.SeleniumWrapper.__new__(utl.SeleniumWrapper)
    vars(sw).update(attrs)
    return sw


class _FakeProcess(object):
    """Xvfb stand-in printing a display number and recording stops."""

    def __init__(self, line=b'99\n', error=None):
        self.stdout = io.BytesIO(line)
        self.error = error
        self.stops = []

    def terminate(self):
        self.stops.append('terminate')
        if self.error:
            raise self.error

    def wait(self, timeout=None):
        self.stops.append('wait')


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

    def execute_cdp_cmd(self, *args, **kwargs):
        return {}

    def execute_script(self, *args, **kwargs):
        raise ValueError('Configure failed.')

    def quit(self):
        self.quit_calls += 1


def func(x):
    return x + 1


def test_example():
    assert func(3) == 4


class _FakeSweepBrowser(object):
    """Stand-in for the sweep's browser: a shot becomes a one-byte file
    and the ad scan answers one canned slot, so the sweep class is the
    only real thing under test."""

    instances = []
    verdicts = []
    url_host = utl.SeleniumWrapper.url_host
    host_under = utl.SeleniumWrapper.host_under

    def __init__(self, *args, **kwargs):
        self.mobile = kwargs.get('mobile', False)
        self.capture = kwargs.get('capture')
        self.browser_errors = (ValueError,)
        self.shots = []
        _FakeSweepBrowser.instances.append(self)

    def take_screenshot(self, url=None, file_name=None, max_attempts=2,
                        scroll_first=False, sleep=5, full_file_name=None):
        with open(file_name, 'wb') as f:
            f.write(b'x')
        self.shots.append((url, file_name, scroll_first))
        self.last_full_shot = ('', None)
        queue = _FakeSweepBrowser.verdicts
        return queue.pop(0) if queue else ('ok', '')

    def capture_verdict(self, png_bytes=None):
        return 'ok', ''

    def scan_ad_slots(self, shot_prefix='', **kwargs):
        path = '{}_ad0.png'.format(shot_prefix)
        with open(path, 'wb') as f:
            f.write(b'y')
        return [{'shot_path': path, 'width': 300, 'height': 250,
                 'top': 100, 'left': 50,
                 'frame_host': 'tpc.googlesyndication.com',
                 'landing_domain': 'landing.example.com',
                 'text': 'Buy Game X',
                 'evidence': ['marker google_ads_iframe']}]

    def page_vitals(self):
        return {'lab_lcp_ms': 1800, 'lab_cls': 0.05, 'lab_ttfb_ms': 300,
                'page_kilobytes': 2048, 'third_party_hosts': 7,
                'ad_frames': 2, 'viewport': {'w': 1000, 'h': 500}}

    def restart_browser(self):
        pass

    def quit(self):
        pass


class _FakeWalkingBrowser(_FakeSweepBrowser):
    """The sweep browser that also finds one article per home page and
    writes a full-page jpeg for every shown page."""

    def take_screenshot(self, url=None, file_name=None, max_attempts=2,
                        scroll_first=False, sleep=5, full_file_name=None):
        verdict = super().take_screenshot(url, file_name, max_attempts,
                                          scroll_first, sleep)
        if full_file_name and verdict[0] == 'ok':
            with open(full_file_name, 'wb') as f:
                f.write(b'z')
            self.last_full_shot = (full_file_name, 5000)
        return verdict

    @staticmethod
    def find_article_links(base_url, limit):
        return [f'{base_url}/articles/story-{n}'
                for n in range(1, limit + 1)]


class _FakeEditorialBrowser(_FakeWalkingBrowser):
    """The walking browser that also reads page meta and headlines."""

    @staticmethod
    def read_page_meta():
        return {'title': 'Structured headline', 'author': 'Ada Writer',
                'feeds': ['https://www.example.org/feed.xml'],
                'sources': {'title': 'ld', 'author': 'ld'}}

    @staticmethod
    def read_headlines(base_url, limit):
        return [{'href': f'{base_url}/articles/lead-{n}',
                 'text': f'The lead story of the day, part {n}'}
                for n in range(1, limit + 1)]


class _FakeRetryBrowser(_FakeSweepBrowser):
    """The sweep browser writing its verdict as the shot, and no shot
    for an unreachable page."""

    def take_screenshot(self, url=None, file_name=None, max_attempts=2,
                        scroll_first=False, sleep=5, full_file_name=None):
        self.shots.append((url, file_name, scroll_first))
        self.last_full_shot = ('', None)
        queue = _FakeSweepBrowser.verdicts
        kind, detail = queue.pop(0) if queue else ('ok', '')
        if not detail.startswith('page unreachable'):
            with open(file_name, 'wb') as f:
                f.write(f'{kind} {detail}'.encode('utf-8'))
        return kind, detail


class _FailingBrowser(_FakeSweepBrowser):
    """The sweep browser whose driver never answers."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.restarts = 0

    def take_screenshot(self, *args, **kwargs):
        raise ValueError('driver went away')

    def restart_browser(self):
        self.restarts += 1


class _FakeS3(object):
    """Bucket stand-in recording every upload by key."""

    def __init__(self):
        self.uploads = {}

    def s3_upload_file_obj(self, file_object, key):
        self.uploads[key] = file_object.read()
        return 'https://b.s3.amazonaws.com/{}'.format(key)


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

    def test_import_read_csv_title_rows(self):
        file_name = 'test_utf16.csv'
        cols = ['a', 'b', 'c']
        rows = [['Report Title'], ['All Time'], cols, ['1', '2', '3']]
        lines = [','.join(x) for x in rows]
        with open(file_name, 'w', encoding='utf-16', newline='') as f:
            f.write('\n'.join(lines))
        ndf = utl.import_read_csv(file_name)
        assert len(ndf) == len(rows) - 1
        ndf = utl.first_last_adj(ndf, 2, 0)
        assert list(ndf.columns) == cols
        assert len(ndf) == 1
        os.remove(file_name)

    def test_import_read_csv_bad_line(self):
        file_name = 'test_bad_line.csv'
        with open(file_name, 'w', newline='') as f:
            f.write('a,b\n1,2\n3,4,5\n6,7\n')
        ndf = utl.import_read_csv(file_name)
        assert list(ndf.columns) == ['a', 'b']
        assert len(ndf) == 2
        os.remove(file_name)

    def test_import_read_csv_narrow_title_row(self):
        """A title row narrower than the data makes pandas move the leading
        columns into an index; they come back as data so the header search
        and first_last_adj see every column."""
        file_name = 'test_narrow_title.csv'
        cols = ['a', 'b', 'c', 'd', 'e']
        with open(file_name, 'w', newline='') as f:
            f.write('Report Title\n' + ','.join(cols) + '\n1,2,3,4,5\n')
        df = utl.import_read_csv(file_name)
        assert isinstance(df.index, pd.RangeIndex)
        assert len(df.columns) == len(cols)
        for idx in range(len(df)):
            tdf = utl.first_last_adj(df, idx, 0)
            if 'a' in tdf.columns:
                break
        assert list(tdf.columns) == cols
        assert len(tdf) == 1
        assert list(df.columns) != cols
        os.remove(file_name)

    def test_import_read_csv_utf16_file_object(self):
        """An uploaded utf-16 csv arrives as a file object, not a path; the
        encoding sniff and the retry read it in place."""
        text = 'a,b\n1,2\n'
        mem = io.BytesIO(text.encode('utf-16'))
        df = utl.import_read_csv(mem, file_check=False, file_type='.csv')
        assert list(df.columns) == ['a', 'b']
        assert df.iloc[0].tolist() == [1, 2]
        mem = io.BytesIO('Report\na,b,c\n1,2,3\n'.encode('utf-16'))
        df = utl.import_read_csv(mem, file_check=False, file_type='.csv')
        assert len(df.columns) == 3

    def test_first_last_adj_ignores_index_labels(self):
        df = pd.DataFrame({'x': ['a', 1], 'y': ['b', 2]}, index=[5, 9])
        adj = utl.first_last_adj(df, 1, 0)
        assert list(adj.columns) == ['a', 'b']
        assert list(df.columns) == ['x', 'y']

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

    def test_remove_date_suffix(self):
        base = 'API_Tiktok_Client'
        assert utl.remove_date_suffix(base) == base
        assert utl.remove_date_suffix(
            '{}_2026-01-29'.format(base)) == base
        stacked = '{}_2026-01-29_2026-02-26_2026-03-26'.format(base)
        assert utl.remove_date_suffix(stacked) == base
        keep = 'API_Amazon_2026'
        assert utl.remove_date_suffix(keep) == keep

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
        test_url = 'https://example.com/'
        sw.go_to_url(test_url, sleep=1)
        assert test_url in sw.browser.current_url
        assert sw.headless is True
        sw.quit()

    @requires_local_browser
    def test_screenshot(self):
        sw = utl.SeleniumWrapper(headless=False)
        test_url = 'https://example.com/'
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
            assert sw.go_to_url('https://example.com/', sleep=1)
            assert sw.browser is not first_browser
        finally:
            sw.quit()

    def test_quit_survives_dead_session(self):
        """Callers quit from a ``finally``, so quit must never raise.

        A driver that stopped answering would otherwise mask the real
        exception and strand the chrome process it meant to reap.
        """
        assert _bare_wrapper(browser=_DeadBrowser()).quit() is None

    @staticmethod
    def _idle_process():
        return subprocess.Popen(
            [sys.executable, '-c', 'import time; time.sleep(60)'])

    def test_a_dead_driver_has_its_browser_killed(self, tmp_path,
                                                  monkeypatch):
        """Only the browser on the wrapper's own profile is killed, not
        its renderers or another profile's, and the folder goes."""
        child, other = self._idle_process(), self._idle_process()
        profile = tmp_path / 'scoped_dir1'
        (profile / 'Default').mkdir(parents=True)
        lines = [
            (child.pid, f'chrome.exe --user-data-dir="{profile}"'),
            (999999991, f'chrome.exe --type=renderer '
                        f'--user-data-dir="{profile}"'),
            (other.pid, 'chrome.exe --user-data-dir="C:\\elsewhere"')]
        monkeypatch.setattr(utl.SeleniumWrapper, 'chrome_commands',
                            staticmethod(lambda: lines))
        sw = _bare_wrapper(browser=_DeadBrowser(),
                           browser_profile=str(profile))
        try:
            assert sw.quit() is None
            child.wait(timeout=10)
            time.sleep(0.5)
            assert other.poll() is None
        finally:
            child.kill()
            other.kill()
        assert not profile.exists() and sw.browser_profile == ''
        assert sw.reap_browser() == 0

    def test_a_kept_profile_outlives_its_browser(self, tmp_path,
                                                 monkeypatch):
        child = self._idle_process()
        profile = tmp_path / 'kept'
        profile.mkdir()
        monkeypatch.setattr(
            utl.SeleniumWrapper, 'chrome_commands', staticmethod(
                lambda: [(child.pid, f'chrome --user-data-dir={profile}')]))
        sw = _bare_wrapper(
            browser_profile=str(profile),
            capture=utl.CaptureOptions(profile_dir=str(profile)))
        try:
            assert sw.reap_browser() == 1
            child.wait(timeout=10)
        finally:
            child.kill()
        assert profile.is_dir()

    def test_a_driver_that_quits_kills_nothing(self, monkeypatch):
        monkeypatch.setattr(
            utl.SeleniumWrapper, 'chrome_commands', staticmethod(
                lambda: pytest.fail('looked for a browser to kill')))
        closer = Mock()
        _bare_wrapper(
            browser=types.SimpleNamespace(close=closer, quit=closer),
            browser_profile='C:\\scoped_dir9').quit()
        assert closer.call_count == 2

    def test_a_browser_already_gone_is_not_an_error(self, tmp_path,
                                                    monkeypatch):
        child = self._idle_process()
        child.kill()
        child.wait(timeout=10)
        monkeypatch.setattr(
            utl.SeleniumWrapper, 'chrome_commands', staticmethod(
                lambda: [(child.pid, f'chrome --user-data-dir={tmp_path}')]))
        sw = _bare_wrapper(browser_profile=str(tmp_path))
        assert sw.reap_browser() == 0
        assert _bare_wrapper().reap_browser() == 0

    @requires_local_browser
    def test_a_killed_driver_leaves_no_chrome(self):
        """A driver killed under a live browser still leaves no Chrome
        and no profile behind once quit."""
        sw = utl.SeleniumWrapper()
        profile = sw.browser_profile
        try:
            assert len(utl.SeleniumWrapper.profile_pids(profile)) == 1
            sw.browser.service.process.kill()
            sw.browser.service.process.wait(timeout=10)
        finally:
            sw.quit()
        for _ in range(40):
            if not utl.SeleniumWrapper.profile_pids(profile):
                break
            time.sleep(0.25)
        assert utl.SeleniumWrapper.profile_pids(profile) == []

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
                {'ad_id': 'https://example.com/'})
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

    def test_mobile_emulation_names_no_device(self, monkeypatch):
        """Mobile spells out its metrics rather than naming a device.

        chromedriver prunes its device list; 154 dropped "iPhone X" and
        refused every mobile session that named it.
        """
        launched = []
        monkeypatch.setattr(utl.SeleniumWrapper, 'create_browser',
                            lambda self, co: launched.append(co) or 1 / 0)
        with pytest.raises(ZeroDivisionError):
            utl.SeleniumWrapper(mobile=True)
        emulation = launched[0].experimental_options['mobileEmulation']
        assert 'deviceName' not in emulation
        assert emulation['deviceMetrics']['mobile']
        assert 'iPhone' in emulation['userAgent']

    @pytest.mark.parametrize('extra', [-1, 0])
    def test_launch_retries_until_its_attempts_run_out(self, monkeypatch,
                                                       extra):
        """A slow chrome is retried; one that never answers still raises."""
        attempts = utl.SeleniumWrapper.launch_attempts
        launch = Mock(side_effect=[url_ex.ReadTimeoutError(
            None, None, 'timed out')] * (attempts + extra) + ['browser'])
        monkeypatch.setattr(utl.SeleniumWrapper, 'create_browser', launch)
        monkeypatch.setattr(utl.SeleniumWrapper, 'launch_pause', 0)
        sw = utl.SeleniumWrapper.__new__(utl.SeleniumWrapper)
        if extra:
            assert sw.launch_browser('options') == 'browser'
        else:
            with pytest.raises(url_ex.ReadTimeoutError):
                sw.launch_browser('options')
        assert launch.call_count == attempts

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

    @requires_local_browser
    def test_accept_cookies_reads_a_typeset_apostrophe(self, tmp_path):
        """A typeset apostrophe still matches, and subscribe is not
        pressed."""
        html = ('<html><body><button onclick="window.picked=\'pay\'">'
                'Je m’abonne</button><button onclick="window.picked='
                '\'accept\'">J’accepte</button></body></html>')
        url = _write_page(tmp_path, 'french.html', html)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            found = sw.find_accept_buttons(sw.get_accept_xpath())
            assert [x.text for x in found] == ['J’accepte']
            sw.accept_cookies()
            assert sw.browser.execute_script('return window.picked;') == (
                'accept')
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

    def test_scan_ad_slots_reads_without_clicking(self, tmp_path):
        """The marked frame is shot and read -- marker, size, landing
        page off the click url's adurl, alt text -- the plain frame and
        the empty container are not slots, nothing is clicked in either
        document, and the page is left on its default content."""
        _write_page(tmp_path, 'creative.html', AD_CREATIVE)
        url = _write_page(tmp_path, 'adpage.html', AD_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            slots = sw.scan_ad_slots(
                shot_prefix=str(tmp_path / 'adpage_Desktop'))
            assert len(slots) == 1
            slot = slots[0]
            assert slot['evidence'] == ['marker google_ads_iframe',
                                        'size 300x250']
            assert slot['landing_domain'] == 'landing.example.com'
            assert 'Buy Game X' in slot['text']
            assert (slot['width'], slot['height']) == (300, 250)
            assert os.path.isfile(slot['shot_path'])
            assert sw.browser.execute_script('return window.clicked;') == 0
            frame = sw.browser.find_element(By.ID, 'google_ads_iframe_1')
            sw.switch_to_frame(frame)
            assert sw.browser.execute_script('return window.clicked;') == 0
        finally:
            sw.quit()

    def test_slot_shot_is_the_slot_and_leaves_the_browser_well(
            self, tmp_path):
        """A slot's shot is the slot below the fold, creative drawn, and
        the browser still shoots the next page."""
        _write_page(tmp_path, 'creative.html', (
            '<html><body style="margin:0;background:#b00020">'
            '<h1 style="color:#fff">Buy Game X</h1></body></html>'))
        url = _write_page(tmp_path, 'adpage.html', AD_PAGE)
        after = _write_page(tmp_path, 'article.html', LONG_ARTICLE)
        shot = tmp_path / 'slot.png'
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=1)
            frame = sw.browser.find_element(By.ID, 'google_ads_iframe_1')
            assert sw.shoot_elem(frame, str(shot)) == str(shot)
            hidden = sw.browser.find_element(By.ID, 'div-gpt-ad-1')
            assert sw.shoot_elem(hidden, str(tmp_path / 'no.png')) == ''
            verdict = sw.take_screenshot(after,
                                         str(tmp_path / 'after.png'))
        finally:
            sw.quit()
        with Image.open(shot) as img:
            assert img.size == (300, 250)
            red, green, blue = img.convert('RGB').getpixel((290, 240))
        assert red > 150 and green < 40 and blue < 60
        assert not utl.SeleniumWrapper.is_solid_png(shot.read_bytes())
        assert not (tmp_path / 'no.png').exists()
        assert verdict == ('ok', '')
        assert not utl.SeleniumWrapper.is_solid_png(
            (tmp_path / 'after.png').read_bytes())

    def test_slot_shot_survives_a_driver_that_cannot(self, tmp_path):
        shot = tmp_path / 'slot.png'
        dead = types.SimpleNamespace(
            browser=types.SimpleNamespace(
                execute_script=_raise_read_timeout),
            elem_box_script='', browser_errors=(url_ex.HTTPError,))
        assert utl.SeleniumWrapper.shoot_elem(dead, None, str(shot)) == ''
        mute = _bare_wrapper(slot_settle=0, browser=types.SimpleNamespace(
            execute_script=lambda script, elem: {
                'x': 0, 'y': 0, 'width': 10, 'height': 10},
            execute_cdp_cmd=lambda name, args: {}))
        assert mute.shoot_elem(None, str(shot)) == ''
        assert not shot.exists()

    def test_page_vitals_reads_the_shown_page(self, tmp_path):
        """A page loaded after the browser started answers every lab
        field without a raise."""
        _write_page(tmp_path, 'creative.html', AD_CREATIVE)
        url = _write_page(tmp_path, 'adpage.html', AD_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=1)
            vitals = sw.page_vitals()
            assert vitals['lab_lcp_ms'] is None or vitals['lab_lcp_ms'] >= 0
            assert vitals['lab_cls'] is None or vitals['lab_cls'] >= 0
            assert vitals['lab_ttfb_ms'] >= 0
            assert vitals['page_kilobytes'] >= 0
            assert vitals['third_party_hosts'] == 0
            assert vitals['viewport']['w'] > 0
            assert vitals['ad_frames'] is None or vitals['ad_frames'] >= 0
            cands = sw.find_ad_candidates()
            assert all(c['top'] >= 0 and c['left'] >= 0 for c in cands)
        finally:
            sw.quit()

    def test_count_ad_frames_walks_the_tree(self):
        tree = {'frame': {'id': '1'},
                'childFrames': [
                    {'frame': {'id': '2', 'adFrameStatus': {
                        'adFrameType': 'root'}},
                     'childFrames': [{'frame': {
                         'id': '3', 'adFrameStatus': {
                             'adFrameType': 'child'}}}]},
                    {'frame': {'id': '4', 'adFrameStatus': {
                        'adFrameType': 'none'}}}]}
        assert utl.SeleniumWrapper.count_ad_frames(tree) == 2
        assert utl.SeleniumWrapper.count_ad_frames({}) == 0

    def test_scroll_through_only_when_asked(self, tmp_path):
        """The default shot never moves the page; ``scroll_first``
        walks it and comes back to the top."""
        url = _write_page(tmp_path, 'tall.html', TALL_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            sw.take_screenshot(file_name=str(tmp_path / 'a.png'))
            assert sw.browser.execute_script('return window.scrolls;') == 0
            sw.take_screenshot(file_name=str(tmp_path / 'b.png'),
                               scroll_first=True)
            assert sw.browser.execute_script('return window.scrolls;') > 0
            assert sw.browser.execute_script('return window.scrollY;') == 0
        finally:
            sw.quit()

    @requires_local_browser
    def test_the_shot_is_of_the_top_of_a_smooth_scrolling_page(
            self, tmp_path):
        """``scroll-behavior: smooth`` does not leave the shot mid-way;
        the walk still reaches the bottom."""
        url = _write_page(tmp_path, 'smooth.html', SMOOTH_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            sw.browser.execute_script(sw.scroll_part_script, 0.8)
            far = sw.browser.execute_script('return window.scrollY;')
            sw.prepare_shot(None, False)
            back = sw.browser.execute_script('return window.scrollY;')
            sw.scroll_through(steps=2, pause=0.1)
            walked = sw.browser.execute_script(
                'return [window.scrollY, window.deepest,'
                ' document.body.scrollHeight - window.innerHeight];')
        finally:
            sw.quit()
        assert far > 5000 and back == 0
        assert walked[0] == 0 and walked[1] >= walked[2] - 1

    def test_landing_domain_parsing(self):
        ld = utl.SeleniumWrapper.landing_domain
        assert ld(['https://ad.doubleclick.net/ddm/clk/1;2;adurl='
                   'https%3A%2F%2Fwww.shop.example%2Fbuy']) == 'shop.example'
        assert ld(['https://www.googleadservices.com/pagead/aclk?sa=L&ai=x'
                   '&adurl=https://store.example.org/x']) == (
            'store.example.org')
        assert ld(['https://securepubads.g.doubleclick.net/pcs/click?x=1']
                  ) == ''
        assert ld(['https://www.rival.example/landing',
                   'https://other.example']) == 'rival.example'
        assert ld([]) == ''

    def test_slot_evidence_rules(self):
        """A host or marker signal makes a slot; a standard size alone
        does only for a frame."""
        ev = utl.SeleniumWrapper.slot_evidence
        assert ev({'tag': 'iframe', 'id': 'x', 'attrs': '', 'width': 1,
                   'height': 1, 'src': 'https://tpc.googlesyndication.com/'
                   'safeframe/1-0-40/html/container.html'}) == [
            'src host tpc.googlesyndication.com']
        assert ev({'tag': 'div', 'id': 'div-gpt-ad-123', 'src': '',
                   'width': 300, 'height': 250,
                   'attrs': 'div-gpt-ad-123'}) == [
            'marker div-gpt-ad', 'size 300x250']
        assert ev({'tag': 'iframe', 'id': '', 'attrs': '', 'width': 728,
                   'height': 90, 'src': 'https://cdn.example.com/w.html'}
                  ) == ['size 728x90']
        assert ev({'tag': 'div', 'id': 'hero', 'src': '', 'width': 728,
                   'height': 90, 'attrs': 'hero'}) == []

    def test_slot_evidence_native_and_container_markers(self):
        """A native widget's name or an ad-container name is evidence;
        a wrapper bigger than a slot and a bare video are not."""
        ev = utl.SeleniumWrapper.slot_evidence
        assert ev({'tag': 'div', 'id': 'taboola-below-article', 'src': '',
                   'width': 600, 'height': 300,
                   'attrs': 'taboola-below-article id style'}) == [
            'native taboola']
        assert ev({'tag': 'div', 'id': '', 'src': '', 'width': 300,
                   'height': 250, 'attrs': ' ad-slot class style'}) == [
            'container ad-slot', 'size 300x250']
        assert ev({'tag': 'div', 'id': 'ad-top', 'src': '', 'width': 728,
                   'height': 90, 'attrs': 'ad-top  id'}) == [
            'container ad-top', 'size 728x90']
        assert ev({'tag': 'div', 'id': '', 'src': '', 'width': 1600,
                   'height': 2000, 'attrs': ' advert-wrapper class'}) == []
        assert ev({'tag': 'video', 'id': '', 'src': '', 'width': 640,
                   'height': 360, 'attrs': '  width height'}) == []
        assert ev({'tag': 'video', 'id': 'preroll', 'width': 640,
                   'height': 360, 'attrs': 'preroll  src',
                   'src': 'https://cdn.connatix.com/v.mp4'}) == [
            'src host cdn.connatix.com', 'size 640x360']

    def test_pick_article_links_keeps_same_host_articles(self):
        """Only deep same-host stories with a headline survive; section,
        account and off-site links are dropped and paths dedupe."""
        pick = utl.SeleniumWrapper.pick_article_links
        base = 'https://www.example.org'
        links = [
            {'href': 'https://www.example.org/',
             'text': 'Home page of Example News'},
            {'href': 'https://www.example.org/tag/rpg',
             'text': 'Role playing'},
            {'href': 'https://www.example.org/login',
             'text': 'Log in to Example News'},
            {'href': 'https://www.example.org/reviews', 'text': 'All reviews'},
            {'href': 'https://social.example.net/news/status/1',
             'text': 'x' * 30},
            {'href': 'https://www.example.org/articles/big-story',
             'text': 'x'},
            {'href': 'https://www.example.org/articles/big-story#c',
             'text': 'The biggest story of the day'},
            {'href': 'https://www.example.org/articles/big-story/',
             'text': 'The biggest story of the day again'},
            {'href': 'https://www.example.org/games/another-long-slug-here-x',
             'text': 'Another long story headline'},
            {'href': 'https://uk.example.org/articles/third-story',
             'text': 'A third story from the uk edition'},
            {'href': 'https://www.example.org/videos/trailer-drop',
             'text': 'Watch the trailer drop'},
            {'href': 'mailto:tips@example.org', 'text': 'Send us your tips'}]
        assert pick(links, base, 5) == [
            'https://www.example.org/articles/big-story',
            'https://www.example.org/games/another-long-slug-here-x',
            'https://uk.example.org/articles/third-story']
        assert pick(links, base, 1) == [
            'https://www.example.org/articles/big-story']
        assert pick([], base, 3) == []

    def test_scan_ad_slots_finds_native_widget(self, tmp_path):
        """A Taboola container and an ad-slot div are read as slots; the
        oversized wrapper and the bare video are not, and nothing is
        clicked."""
        _write_page(tmp_path, 'creative.html', AD_CREATIVE)
        url = _write_page(tmp_path, 'native.html', NATIVE_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            slots = sw.scan_ad_slots(
                shot_prefix=str(tmp_path / 'native_Desktop'))
            assert [s['evidence'] for s in slots] == [
                ['native taboola'], ['container ad-slot', 'size 300x250']]
            assert all(os.path.isfile(s['shot_path']) for s in slots)
            assert sw.browser.execute_script('return window.clicked;') == 0
        finally:
            sw.quit()

    def test_full_page_shot_is_capped(self, tmp_path):
        """A shown page gets a full-page jpeg no taller than the cap
        beside its viewport png, named on ``last_full_shot``; a shot
        that did not ask for one resets it."""
        url = _write_page(tmp_path, 'story.html', LONG_ARTICLE)
        png, jpg = str(tmp_path / 'a.png'), str(tmp_path / 'a_full.jpg')
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            assert sw.take_screenshot(file_name=png, full_file_name=jpg) == (
                'ok', '')
            assert sw.last_full_shot[0] == jpg
            assert sw.last_full_shot[1] > sw.full_shot_max_height
            with Image.open(png) as image:
                viewport_height = image.height
            with Image.open(jpg) as image:
                assert image.format == 'JPEG'
                assert viewport_height < image.height <= \
                    sw.full_shot_max_height
                assert image.width > 0
            sw.take_screenshot(file_name=png)
            assert sw.last_full_shot == ('', None)
        finally:
            sw.quit()

    @pytest.mark.parametrize('state, expected', [
        ({'title': 'Just a moment...', 'text': 'Performing security '
          'verification', 'url': 'https://games.example.net/', 'len': 60},
         ('bot_check', 'performing security verification')),
        ({'title': 'x', 'text': "You've been blocked by network security",
          'url': 'https://forum.example.com/r/gaming', 'len': 40},
         ('blocked', 'blocked by network security')),
        ({'title': 'Privacy error', 'text': 'Your connection is not '
          'private NET::ERR_CERT_DATE_INVALID', 'url': 'https://example.net/',
          'proto': 'chrome-error:', 'len': 80},
         ('ssl_error', 'your connection is not private')),
        ({'title': 'Log in', 'text': 'Log in to continue',
          'url': 'https://social.example.net/', 'len': 18},
         ('login_wall', 'log in')),
        ({'title': 'Home', 'text': f'Sign in {"news " * 200}',
          'url': 'https://www.example.org/', 'len': 1000}, ('ok', '')),
        ({'title': 'Account', 'text': 'x' * 900,
          'url': 'https://stream.example.com/login?next=/', 'len': 900},
         ('login_wall', '/login')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'We and our partners use cookies', 'len': 900},
         ('consent_wall', 'we and our partners use cookies')),
        ({'title': 'Oops', 'text': 'Something went wrong. Try again.',
          'url': 'https://video.example.com/', 'len': 32},
         ('error_page', 'something went wrong')),
        ({'title': 'Site', 'text': f'Something went wrong {"x" * 900}',
          'url': 'https://a.com/', 'len': 920}, ('ok', '')),
        ({'title': 'Site', 'text': f'Something went wrong {"x" * 500}',
          'url': 'https://a.com/', 'len': 520},
         ('error_page', 'something went wrong')),
        ({'title': '502 Bad Gateway', 'text': 'x' * 900,
          'url': 'https://a.com/', 'len': 900},
         ('error_page', '502 bad gateway')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'Sign up  to keep reading. We use cookies.',
          'cover': 0.36, 'len': 900},
         ('login_wall', 'sign up to keep reading. we use cookies.')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'Continue with recommended cookies', 'cover': 0.5,
          'len': 900},
         ('consent_wall', 'continue with recommended cookies')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'Sign up for our newsletter', 'cover': 0.05,
          'len': 900}, ('ok', '')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'We and our partners use cookies', 'cover': 0.08,
          'len': 900, 'status': 200, 'nodes': 40, 'buttons': ['Accept']},
         ('ok', '')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'We and our partners use cookies', 'cover': 0.2,
          'len': 900}, ('consent_wall', 'we and our partners use cookies')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://example.net/',
          'dialog': 'Nous respectons vos choix', 'cover': 0.6, 'len': 900},
         ('consent_wall', 'nous respectons vos choix')),
        ({'title': 'Clips', 'text': 'x' * 900, 'len': 900,
          'url': 'https://clips.example.com/', 'cover': 0.13, 'modal': True,
          'dialog': 'Skip Log in to Clips Use QR code'},
         ('login_wall', 'skip log in to clips use qr code')),
        ({'title': 'Clips', 'text': 'x' * 900, 'len': 900,
          'url': 'https://clips.example.com/', 'cover': 0.13, 'modal': False,
          'dialog': 'Skip Log in to Clips Use QR code'}, ('ok', '')),
        ({'title': 'Site', 'text': 'x' * 900, 'url': 'https://a.com/',
          'dialog': 'We and our partners use cookies', 'cover': 0.08,
          'modal': True, 'len': 900}, ('ok', '')),
    ])
    def test_classify_page_names_each_interstitial(self, state, expected):
        """Header sign-in links, error phrases deep in a long page and
        thin cookie banners are not walls."""
        assert utl.SeleniumWrapper.classify_page(state) == expected

    @staticmethod
    def _png(color, size=(64, 48), mark=None):
        img = Image.new('RGB', size, color)
        if mark:
            ImageDraw.Draw(img).rectangle(mark, fill=(200, 30, 30))
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        return buffer.getvalue()

    def test_is_solid_png_reads_solid_and_drawn_images(self):
        """Any solid shade is blank, a thin drawn strip is not, and
        unreadable bytes count as blank."""
        solid = utl.SeleniumWrapper.is_solid_png
        assert solid(self._png((0, 0, 0)))
        assert solid(self._png((238, 238, 238)))
        assert not solid(self._png((255, 255, 255), mark=(0, 0, 63, 3)))
        assert solid(b'not a png')
        state = {'title': 'Site', 'text': 'x' * 900, 'len': 900,
                 'url': 'https://a.com/'}
        assert utl.SeleniumWrapper.classify_page(
            state, self._png((244, 244, 244))) == (
            'blank', 'single-colour page')

    def test_vendor_of_names_the_serving_network(self):
        """The creative's hosts outrank the frame's."""
        vendor = utl.SeleniumWrapper.vendor_of
        assert vendor(['s0.2mdn.net', 'www.landing.example'],
                      'tpc.googlesyndication.com') == 'Google'
        assert vendor(['www.landing.example'],
                      'aax-us-east.amazon-adsystem.com') == 'Amazon DSP'
        assert vendor(['cdn.taboola.com'], 'www.example.org') == 'Taboola'
        assert vendor([], '', ['marker div-gpt-ad']) == 'Google'
        assert vendor(['www.example.org'], 'www.example.org') == 'Site direct'
        assert vendor([], '') == ''

    def test_capture_verdict_reads_fixture_pages(self, tmp_path):
        """Real Chrome names each interstitial and reports no headless
        user agent or webdriver flag."""
        pages = (('cf.html', CF_INTERSTITIAL, 'bot_check'),
                 ('blocked.html', BLOCKED_PAGE, 'blocked'),
                 ('login.html', LOGIN_PAGE, 'login_wall'),
                 ('consent.html', CONSENT_MODAL, 'consent_wall'),
                 ('signup.html', SIGNUP_PROMPT, 'login_wall'),
                 ('thin.html', THIN_BANNER, 'ok'),
                 ('banner.html', COOKIE_BANNER, 'ok'))
        sw = utl.SeleniumWrapper()
        try:
            for name, html, expected in pages:
                sw.go_to_url(_write_page(tmp_path, name, html), sleep=0)
                assert sw.capture_verdict()[0] == expected, name
            assert 'HeadlessChrome' not in sw.browser.execute_script(
                'return navigator.userAgent;')
            assert sw.browser.execute_script(
                'return navigator.webdriver;') is None
        finally:
            sw.quit()

    def test_take_screenshot_reshoots_a_late_painting_page(self, tmp_path):
        """The re-shoot after a blank first shot is the file written."""
        url = _write_page(tmp_path, 'late.html', LATE_PAINT)
        seen = []
        sw = utl.SeleniumWrapper()
        verdict = sw.capture_verdict
        sw.capture_verdict = lambda png=None: seen.append(
            verdict(png)) or seen[-1]
        try:
            sw.go_to_url(url, sleep=0)
            kind, detail = sw.take_screenshot(
                file_name=str(tmp_path / 'late.png'))
        finally:
            sw.quit()
        assert seen[0][0] == 'blank'
        assert (kind, detail) == ('ok', '')
        assert not utl.SeleniumWrapper.is_solid_png(
            (tmp_path / 'late.png').read_bytes())

    def test_consent_selector_dismisses_a_cmp_modal(self, tmp_path):
        """A consent platform's button is found by id, not text."""
        url = _write_page(tmp_path, 'cmp.html', CONSENT_MODAL)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            assert sw.capture_verdict()[0] == 'consent_wall'
            sw.accept_cookies()
            assert sw.browser.execute_script('return window.picked;') == (
                'cmp')
            assert sw.capture_verdict()[0] == 'ok'
        finally:
            sw.quit()

    @requires_local_browser
    def test_a_button_with_no_text_is_found_by_its_class(self, tmp_path):
        """A label drawn by CSS is found by class, and subscribe is not
        pressed."""
        url = _write_page(tmp_path, 'wall.html', PAY_OR_CONSENT)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            assert sw.capture_verdict()[0] == 'consent_wall'
            sw.accept_cookies()
            assert sw.browser.execute_script('return window.picked;') == (
                'cookies')
            assert sw.capture_verdict()[0] == 'ok'
        finally:
            sw.quit()

    def test_launch_args_follow_the_capture_options(self):
        """Options drop by prefix, add extras and bring a profile's cache
        cap; a phone is spelled out, never a named device."""
        shared = ['--lang=en-US', '--window-size=1920,1080',
                  '--start-maximized', '--no-sandbox', '--disable-gpu',
                  '--disable-blink-features=AutomationControlled']
        sw = _bare_wrapper(mobile=False, page_load_strategy='')
        assert sw.launch_args(True) == [
            '--headless=new', '--window-position=-32000,-32000'] + shared
        assert sw.launch_args(False) == shared
        co = sw.chrome_options(sw.launch_args(True), 'tmp')
        assert co.arguments == sw.launch_args(True)
        assert 'mobileEmulation' not in co.experimental_options
        sw.mobile = True
        phone = sw.chrome_options([], 'tmp').experimental_options[
            'mobileEmulation']
        assert 'deviceName' not in phone and 'iPhone' in phone['userAgent']
        sw.capture = utl.CaptureOptions(
            extra_args=('--disable-http2',),
            drop_args=('--disable-gpu', '--window-position'),
            profile_dir='profiles/desktop')
        assert sw.launch_args(True) == [
            '--headless=new', *shared[:4], shared[5], '--disable-http2',
            '--user-data-dir=profiles/desktop',
            '--disk-cache-size=52428800']

    HEADLESS_AGENT = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) HeadlessChrome/154.0.0.0 '
                      'Safari/537.36')
    HINTS = {'brands': [{'brand': 'Google Chrome', 'version': '154'}],
             'platform': 'Linux', 'mobile': False}
    OVERRIDE = 'Emulation.setUserAgentOverride'
    ON_NEW_DOCUMENT = 'Page.addScriptToEvaluateOnNewDocument'

    @staticmethod
    def _identity(browser, mobile=False, native=True):
        sw = _bare_wrapper(mobile=mobile)
        if native:
            sw.set_native_identity(browser)
        else:
            sw.set_patched_identity(browser)
        return browser

    def test_native_identity_keeps_the_client_hints(self):
        """Only the headless mark comes off; the hints Chrome would send
        go with the agent and no script patches ``navigator``."""
        hints = dict(self.HINTS, secure=True)
        browser = self._identity(
            _IdentityBrowser(self.HEADLESS_AGENT, hints))
        assert browser.pages == [utl.SeleniumWrapper.client_hints_page]
        assert browser.named(self.OVERRIDE) == [{
            'userAgent': self.HEADLESS_AGENT.replace('HeadlessChrome',
                                                     'Chrome'),
            'userAgentMetadata': self.HINTS}]
        assert browser.named(self.ON_NEW_DOCUMENT) == [
            {'source': utl.SeleniumWrapper.webdriver_guard_script}]
        assert browser.named('Network.setUserAgentOverride') == []

    def test_native_identity_leaves_a_true_agent_alone(self):
        """A windowed agent and a phone are not overridden, and unread
        hints cost only the hints."""
        windowed = self.HEADLESS_AGENT.replace('HeadlessChrome', 'Chrome')
        for agent, mobile in ((windowed, False),
                              (self.HEADLESS_AGENT, True)):
            browser = self._identity(_IdentityBrowser(agent, self.HINTS),
                                     mobile=mobile)
            assert browser.named(self.OVERRIDE) == [] and not browser.pages
        for hints in (None, ex.WebDriverException('no such page')):
            sent = self._identity(_IdentityBrowser(
                self.HEADLESS_AGENT, hints)).named(self.OVERRIDE)
            assert len(sent) == 1 and 'userAgentMetadata' not in sent[0]

    def test_patched_identity_is_what_every_caller_had(self):
        browser = self._identity(_IdentityBrowser(self.HEADLESS_AGENT),
                                 native=False)
        assert browser.commands == [
            (self.ON_NEW_DOCUMENT,
             {'source': utl.SeleniumWrapper.stealth_script}),
            ('Network.setUserAgentOverride', {
                'userAgent': self.HEADLESS_AGENT.replace(
                    'HeadlessChrome', 'Chrome'),
                'acceptLanguage': 'en-US,en;q=0.9'})]
        phone = self._identity(_IdentityBrowser(self.HEADLESS_AGENT),
                               mobile=True, native=False)
        assert [name for name, _ in phone.commands] == [
            self.ON_NEW_DOCUMENT]

    def test_configure_browser_follows_the_options(self, monkeypatch):
        """The options pick the identity and the page load timeout, held
        under the command timeout; a kept profile starts cache-empty."""
        monkeypatch.setattr(
            utl.SeleniumWrapper, 'enable_download_in_headless_chrome',
            Mock())
        sw = _bare_wrapper(mobile=False)
        plain = Mock()
        plain.execute_script.return_value = self.HEADLESS_AGENT
        sw.configure_browser(plain, False, 'tmp')
        plain.maximize_window.assert_called_once_with()
        plain.set_page_load_timeout.assert_called_once_with(10)
        sent = [c.args[0] for c in plain.execute_cdp_cmd.call_args_list]
        assert 'Network.setUserAgentOverride' in sent
        assert 'Network.clearBrowserCache' not in sent
        sw.capture = utl.CaptureOptions(native_identity=True,
                                        page_load_timeout=90,
                                        profile_dir='profiles/desktop')
        kept = _IdentityBrowser(self.HEADLESS_AGENT, self.HINTS)
        kept.set_window_size, kept.set_script_timeout = Mock(), Mock()
        kept.set_page_load_timeout = Mock()
        sw.configure_browser(kept, True, 'tmp')
        kept.set_window_size.assert_called_once_with(1920, 1080)
        kept.set_page_load_timeout.assert_called_once_with(45)
        assert len(kept.named(self.OVERRIDE)) == 1
        assert kept.named('Network.clearBrowserCache') == [{}]
        assert sw.browser_profile == 'profiles/desktop'

    @requires_local_browser
    def test_native_identity_reads_as_an_ordinary_chrome(self):
        """Agent, hints and header order agree with Chrome's own, and
        ``navigator`` carries no patches."""
        _EchoHandler.seen = []
        server = _start_server(_EchoHandler)
        sw = utl.SeleniumWrapper(capture=ssapi.SsApi.page_capture())
        try:
            sw.go_to_url(_server_url(server), sleep=0)
            seen = sw.browser.execute_script(
                "return {agent: navigator.userAgent,"
                " brands: navigator.userAgentData.brands.map(b => b.brand),"
                " webdriver: navigator.webdriver,"
                " own: Object.getOwnPropertyNames(navigator)};")
        finally:
            sw.quit()
            _stop_server(server)
        headers = {k.lower(): v for k, v in _EchoHandler.seen[0].items()}
        assert headers['user-agent'] == seen['agent']
        assert 'Headless' not in seen['agent']
        assert all(b in headers['sec-ch-ua'] for b in seen['brands'])
        assert list(headers)[-1] == 'accept-language'
        assert seen['webdriver'] is False and seen['own'] == []

    def test_fresh_cookies_are_cleared_before_the_page_loads(self):
        """Cookies go before each navigation, never for a shot of the
        page already shown, and a failed clear costs nothing else."""
        sw = _bare_wrapper(browser=_IdentityBrowser('Chrome'))
        sw.go_to_url = lambda url, **kwargs: sw.browser.pages.append(
            (url, len(sw.browser.commands))) and False
        assert sw.take_screenshot('https://a.example')[0] == 'error_page'
        assert sw.browser.commands == []
        sw.capture = utl.CaptureOptions(fresh_cookies=True)
        sw.take_screenshot('https://a.example')
        assert sw.browser.commands == [('Network.clearBrowserCookies', {})]
        assert sw.browser.pages[-1] == ('https://a.example', 1)
        sw.browser = types.SimpleNamespace(
            execute_cdp_cmd=_raise_read_timeout)
        assert sw.forget_visits() is False
        sw.browser = _ScriptedBrowser({}, png=self._png((9, 9, 9)))
        sw.forget_visits = Mock()
        sw.retry_settles = {}
        assert sw.take_screenshot()[0] == 'blank'
        assert not sw.forget_visits.called

    def test_describe_nav_error_names_class_and_code(self):
        describe = utl.SeleniumWrapper.describe_nav_error
        assert describe(ex.TimeoutException('timed out')) == (
            'TimeoutException')
        assert describe(ex.WebDriverException(
            'unknown error: net::ERR_CONNECTION_RESET\n  (Session info)')
        ) == 'WebDriverException net::ERR_CONNECTION_RESET'

    def test_keep_partial_needs_a_timeout_and_a_document(self):
        """Only a timed-out navigation over its own real document is
        kept, and only when the options ask."""
        timeout = ex.TimeoutException('timed out')
        page = {'proto': 'http:', 'url': 'http://a.com/', 'len': 40,
                'origin': 1000500}
        sw = _bare_wrapper(browser=_ScriptedBrowser(page))
        assert not sw.keep_partial(timeout)
        sw.capture = utl.CaptureOptions(shoot_partial=True)
        assert not sw.keep_partial(ex.WebDriverException('net::ERR_FAILED'))
        assert sw.browser.scripts == []
        sw.nav_started = 1000
        assert sw.keep_partial(timeout) and sw.partial_load
        assert sw.browser.scripts[0] == 'window.stop();'
        for change in ({'proto': 'chrome-error:'}, {'url': 'about:blank'},
                       {'len': 0}, {'origin': 999500}):
            sw.browser = _ScriptedBrowser({**page, **change})
            assert not sw.keep_partial(timeout), change

    @requires_local_browser
    def test_go_to_url_keeps_a_partial_page(self, tmp_path):
        server = _start_server(_StallingHandler)
        shot = tmp_path / 'partial.png'
        sw = utl.SeleniumWrapper(
            page_load_strategy='eager', capture=utl.CaptureOptions(
                page_load_timeout=3, shoot_partial=True))
        try:
            verdict = sw.take_screenshot(_server_url(server), str(shot))
        finally:
            sw.quit()
            _stop_server(server)
        assert verdict == ('ok', 'partial load')
        assert sw.last_nav_error == 'TimeoutException'
        assert not utl.SeleniumWrapper.is_solid_png(shot.read_bytes())

    @requires_local_browser
    def test_unreachable_page_stays_error_page(self, tmp_path):
        """A page that sent nothing leaves the one before it, which is
        not kept; a refused host and an unasked stall are unreachable."""
        stalled, silent = (_start_server(_StallingHandler),
                           _start_server(_SilentHandler))
        sw = utl.SeleniumWrapper(
            page_load_strategy='eager', capture=utl.CaptureOptions(
                page_load_timeout=2, shoot_partial=True))
        try:
            sw.browser.get(_write_page(tmp_path, 'story.html', LONG_ARTICLE))
            assert not sw.go_to_url(_server_url(silent), sleep=0,
                                    max_attempts=1)
            assert not sw.partial_load
            assert sw.take_screenshot(_closed_port_url()) == (
                'error_page', 'page unreachable: WebDriverException '
                'net::ERR_CONNECTION_REFUSED')
            sw.capture = utl.CaptureOptions(page_load_timeout=2)
            assert sw.take_screenshot(_server_url(stalled)) == (
                'error_page', 'page unreachable: TimeoutException')
        finally:
            sw.quit()
            _stop_server(stalled)
            _stop_server(silent)

    def test_wait_painted_gives_up_without_paint(self):
        """No text and no paint is never shot; text over a blank shot is
        polled to the deadline; a drawn shot is returned."""
        sw = _bare_wrapper(paint_poll=0.01,
                           browser=_ScriptedBrowser({'lcp': 0, 'len': 0}))
        assert sw.wait_painted(0.1) is None and sw.browser.shots == 0
        sw.browser = _ScriptedBrowser({'lcp': 0, 'len': 40},
                                      png=self._png((255, 255, 255)))
        assert sw.wait_painted(0.1) is None and sw.browser.shots > 1
        drawn = self._png((255, 255, 255), mark=(0, 0, 63, 3))
        sw.browser = _ScriptedBrowser({'lcp': 120, 'len': 0}, png=drawn)
        assert sw.wait_painted(0.1) == drawn

    @requires_local_browser
    def test_take_screenshot_waits_for_paint_when_asked(self, tmp_path):
        """The paint wait replaces the fixed settle and the first shot is
        already drawn."""
        url = _write_page(tmp_path, 'late.html', LATE_PAINT)
        sw = utl.SeleniumWrapper(capture=utl.CaptureOptions(paint_wait=10))
        sw.capture_verdict = Mock(return_value=('ok', ''))
        sw.accept_cookies = Mock()
        try:
            start = time.time()
            sw.take_screenshot(url, str(tmp_path / 'late.png'), sleep=30)
            png = sw.capture_verdict.call_args.args[0]
        finally:
            sw.quit()
        assert time.time() - start < 20
        assert sw.capture_verdict.call_count == 1
        assert not utl.SeleniumWrapper.is_solid_png(png)

    @requires_local_browser
    def test_take_screenshot_tags_a_cleared_check(self, tmp_path):
        """A check that clears by itself is waited out, never touched,
        and the page behind it is shot."""
        url = _write_page(tmp_path, 'check.html', CLEARING_CHECK)
        shot = tmp_path / 'check.png'
        sw = utl.SeleniumWrapper(
            capture=utl.CaptureOptions(challenge_wait=20))
        sw.accept_cookies = Mock()
        try:
            kind, detail = sw.take_screenshot(url, str(shot), sleep=0)
            title = sw.browser.title
            touched = sw.browser.execute_script('return window.touched;')
        finally:
            sw.quit()
        assert kind == 'ok' and title == 'Example Games' and touched == 0
        assert re.fullmatch(r'cleared after \d+s', detail)
        assert sw.accept_cookies.call_count == 2
        assert not utl.SeleniumWrapper.is_solid_png(shot.read_bytes())

    def test_wait_challenge_gives_up_at_the_deadline(self):
        state = {'title': 'Just a moment...', 'len': 60,
                 'text': 'Performing security verification',
                 'url': 'https://games.example.net/'}
        first = self._png((255, 255, 255), mark=(0, 0, 63, 3))
        sw = _bare_wrapper(
            capture=utl.CaptureOptions(challenge_wait=0.2),
            challenge_poll=0.05,
            browser=_ScriptedBrowser(state, png=b'later shot'))
        assert sw.wait_challenge() == (
            'bot_check', 'performing security verification', 0)
        assert set(sw.browser.scripts) == {sw.page_state_script}
        sw.browser = _ScriptedBrowser(state, png=b'later shot')
        assert sw.settle_and_reshoot(first, 'bot_check', 'just a moment') == (
            first, 'bot_check', 'performing security verification')
        assert set(sw.browser.scripts) == {sw.page_state_script}

    def test_settle_keeps_the_fixed_wait_without_a_challenge_wait(
            self, monkeypatch):
        state = {'title': 'Just a moment...', 'len': 60, 'text': '',
                 'url': 'https://games.example.net/'}
        sleeps = Mock()
        monkeypatch.setattr(utl.time, 'sleep', sleeps)
        sw = _bare_wrapper(wait_challenge=Mock(), browser=_ScriptedBrowser(
            state, png=b'second shot'))
        assert sw.settle_and_reshoot(b'first', 'bot_check', 'x') == (
            b'second shot', 'bot_check', 'just a moment')
        assert [c.args for c in sleeps.call_args_list] == [(8,)]
        assert not sw.wait_challenge.called

    def test_locked_profile_falls_back(self, monkeypatch, tmp_path):
        """A profile another Chrome holds costs the profile, not the
        capture, and never fetches a new driver; a driver Chrome refuses
        is still replaced with the profile kept."""
        browser = types.SimpleNamespace(window_handles=['w0'])
        launcher = _ProfileLauncher(browser)
        recovery = Mock(return_value='1.2.3.4')
        monkeypatch.setattr(utl.SeleniumWrapper, 'launch_browser',
                            launcher)
        monkeypatch.setattr(utl.SeleniumWrapper, 'configure_browser',
                            Mock())
        monkeypatch.setattr(utl.SeleniumWrapper, 'get_chrome_version',
                            recovery)
        monkeypatch.setattr(utl.SeleniumWrapper, 'get_chromedriver_version',
                            recovery)
        monkeypatch.setattr(utl.SeleniumWrapper, 'download_chromedriver',
                            recovery)
        profile = str(tmp_path / 'profile')
        options = utl.CaptureOptions(profile_dir=profile)
        sw = utl.SeleniumWrapper(capture=options)
        assert sw.browser is browser and not recovery.called
        assert f'--user-data-dir={profile}' in launcher.launches[0]
        assert launcher.launches[1] == _bare_wrapper().launch_args(True)
        stale = _ProfileLauncher(
            browser, 'session not created: This version of ChromeDriver '
                     'only supports Chrome version 150', refusals=1)
        monkeypatch.setattr(utl.SeleniumWrapper, 'launch_browser', stale)
        utl.SeleniumWrapper(capture=options)
        assert recovery.call_count == 3
        assert [f'--user-data-dir={profile}' in x
                for x in stale.launches] == [True, True]

    def test_virtual_display_absent_is_none(self, monkeypatch):
        monkeypatch.setattr(utl.shutil, 'which', Mock(return_value=None))
        launches = Mock(return_value=(
            types.SimpleNamespace(window_handles=['w0']), None))
        monkeypatch.setattr(utl.SeleniumWrapper, 'init_browser', launches)
        sw = utl.SeleniumWrapper(headless=False,
                                 capture=utl.CaptureOptions(headed=True))
        assert sw.display is None and sw.headless is True
        assert launches.call_args_list[0].args[0] is True
        assert utl.SeleniumWrapper(headless=False).headless is False

    def test_virtual_display_rides_the_driver_environment(
            self, monkeypatch):
        """The display reaches Chrome through the driver's environment
        only, and is given up on quit."""
        stops, service = Mock(), Mock(return_value='service')
        chrome = Mock(return_value=types.SimpleNamespace(
            window_handles=['w0'], close=Mock(), quit=Mock()))
        monkeypatch.setattr(utl.VirtualDisplay, 'available',
                            Mock(return_value=True))
        monkeypatch.setattr(utl.VirtualDisplay, 'start',
                            lambda self: setattr(self, 'name', ':99'))
        monkeypatch.setattr(utl.VirtualDisplay, 'stop', stops)
        monkeypatch.setattr(utl.wd.chrome.service, 'Service', service)
        monkeypatch.setattr(utl.wd, 'Chrome', chrome)
        monkeypatch.setattr(utl.SeleniumWrapper, 'configure_browser',
                            Mock())
        before = os.environ.get('DISPLAY')
        sw = utl.SeleniumWrapper(capture=utl.CaptureOptions(headed=True))
        assert sw.headless is False
        assert '--headless=new' not in sw.co.arguments
        assert service.call_args.kwargs['env']['DISPLAY'] == ':99'
        assert os.environ.get('DISPLAY') == before
        sw.quit()
        assert stops.call_count == 1 and sw.display is None

    def test_virtual_display_reads_its_number(self, monkeypatch):
        """The display is the number Xvfb prints; one that prints none
        is stopped, and stopping a dead one is quiet."""
        process = _FakeProcess()
        monkeypatch.setattr(utl.subprocess, 'Popen',
                            Mock(return_value=process))
        monkeypatch.setattr(utl.select, 'select',
                            lambda r, w, x, t: (r, [], []))
        display = utl.VirtualDisplay()
        assert display.start() == ':99'
        display.stop()
        display.stop()
        assert process.stops == ['terminate', 'wait'] and not display.name
        dead = _FakeProcess(error=ProcessLookupError('no such process'))
        monkeypatch.setattr(utl.subprocess, 'Popen',
                            Mock(return_value=dead))
        display.start()
        assert display.stop() is None
        silent = _FakeProcess()
        monkeypatch.setattr(utl.subprocess, 'Popen',
                            Mock(return_value=silent))
        monkeypatch.setattr(utl.select, 'select',
                            lambda r, w, x, t: ([], [], []))
        with pytest.raises(OSError):
            display.start()
        assert silent.stops == ['terminate', 'wait']

    @requires_local_browser
    def test_page_state_measures_the_dialog(self, tmp_path):
        states = []
        sw = utl.SeleniumWrapper()
        try:
            for name, html in (('cmp.html', CONSENT_MODAL),
                               ('thin.html', THIN_BANNER),
                               ('story.html', LONG_ARTICLE)):
                sw.go_to_url(_write_page(tmp_path, name, html), sleep=0)
                states.append(sw.browser.execute_script(
                    sw.page_state_script))
        finally:
            sw.quit()
        wall, thin, bare = states
        assert wall['cover'] == 1 and wall['buttons'] == ['Accept all']
        assert 0 < thin['cover'] < 0.2 and thin['buttons'] == []
        assert bare['cover'] == 0 and bare['nodes'] > 120

    @requires_local_browser
    def test_login_dialog_is_dismissed(self, tmp_path):
        """A sign-up prompt is closed by its close control, never its
        account button; a page with no dialog gets no key press."""
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(_write_page(tmp_path, 'check.html',
                                     CLEARING_CHECK), sleep=0)
            sw.dismiss_dialog()
            assert sw.browser.execute_script(
                'return window.touched;') == 0
            sw.go_to_url(_write_page(tmp_path, 'signup.html',
                                     SIGNUP_PROMPT), sleep=0)
            assert sw.capture_verdict()[0] == 'login_wall'
            verdict = sw.take_screenshot(
                file_name=str(tmp_path / 'signup.png'))
            picked = sw.browser.execute_script('return window.picked;')
        finally:
            sw.quit()
        assert verdict == ('ok', '') and picked == 'closed'

    @requires_local_browser
    def test_a_modal_prompt_is_read_past_a_tooltip(self, tmp_path):
        """The largest dialog counts; a modal sign-in prompt is a wall at
        any size, left by the control worded as the way out."""
        url = _write_page(tmp_path, 'skip.html', SKIP_PROMPT)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            state = sw.browser.execute_script(sw.page_state_script)
            first = sw.capture_verdict()
            verdict = sw.take_screenshot(file_name=str(tmp_path / 'a.png'))
            picked = sw.browser.execute_script('return window.picked;')
        finally:
            sw.quit()
        assert state['modal'] is True and 0.05 < state['cover'] < 0.2
        assert first[0] == 'login_wall'
        assert verdict == ('ok', '') and picked == 'skipped'

    def test_headline_links_returns_text_with_hrefs(self):
        links = [
            {'href': 'https://www.example.org/tag/rpg',
             'text': 'Role playing'},
            {'href': 'https://www.example.org/articles/big-story#c',
             'text': ' The biggest   story\nof the day '},
            {'href': 'https://www.example.org/articles/short', 'text': 'x'},
            {'href': 'https://www.example.org/articles/second-story',
             'text': 'A second story worth reading'}]
        found = utl.SeleniumWrapper.headline_links(
            links, 'https://www.example.org', 5)
        assert found == [
            {'href': 'https://www.example.org/articles/big-story',
             'text': 'The biggest story of the day'},
            {'href': 'https://www.example.org/articles/second-story',
             'text': 'A second story worth reading'}]
        assert utl.SeleniumWrapper.pick_article_links(
            links, 'https://www.example.org', 1) == [found[0]['href']]

    def test_clean_page_meta_takes_the_best_source(self):
        """JSON-LD outranks Open Graph, Twitter and plain meta, a field
        only a lesser source names still lands, and ``sources`` says
        where each came from."""
        raw = {
            'ld': [{'@type': 'WebSite', 'name': 'Site node'},
                   {'@type': ['Thing', 'NewsArticle'],
                    'headline': 'Structured  headline',
                    'publisher': {'name': 'Example Media'},
                    'author': [{'name': 'Ada Writer'}, 'Bo Editor'],
                    'image': [{'url': 'https://cdn.example.com/ld.jpg'}],
                    'keywords': 'rpg, indie ,, launch'}],
            'og': {'title': 'Open graph title',
                   'description': 'Open graph summary'},
            'article': {'published_time': '2026-09-01T08:00:00Z'},
            'meta': {'title': 'Tab title', 'time': '2026-08-30',
                     'canonical': 'https://www.example.com/big-story'},
            'feeds': ['https://www.example.com/feed.xml']}
        assert utl.SeleniumWrapper.clean_page_meta(raw) == {
            'title': 'Structured headline',
            'description': 'Open graph summary',
            'image': 'https://cdn.example.com/ld.jpg',
            'published': '2026-09-01T08:00:00Z',
            'author': 'Ada Writer, Bo Editor',
            'tags': ['rpg', 'indie', 'launch'],
            'canonical': 'https://www.example.com/big-story',
            'site_name': 'Example Media',
            'feeds': ['https://www.example.com/feed.xml'],
            'sources': {'title': 'ld', 'description': 'og', 'image': 'ld',
                        'published': 'article', 'author': 'ld',
                        'tags': 'ld', 'canonical': 'meta',
                        'site_name': 'ld'}}
        lesser = {'twitter': {'title': 'Twitter title'},
                  'ld': [{'@type': 'WebSite', 'headline': 'Not a story'}]}
        assert utl.SeleniumWrapper.clean_page_meta(lesser) == {
            'title': 'Twitter title', 'sources': {'title': 'twitter'}}

    @pytest.mark.parametrize('raw, expected', [
        (None, {}),
        ({'ld': 'junk', 'og': ['junk'], 'meta': {'title': '  '},
          'feeds': 'junk'}, {}),
        ({'og': {'title': 'x' * 400}},
         {'title': 'x' * 300, 'sources': {'title': 'og'}}),
        ({'article': {'tag': [f'{n}{"t" * 100}' for n in range(15)]}},
         {'tags': [f'{n}{"t" * 79}' for n in range(10)],
          'sources': {'tags': 'article'}}),
        ({'og': {'image': 'data:image/png;base64,AAAA'},
          'twitter': {'image': 'https://cdn.example.com/t.jpg'},
          'feeds': ['file:///feed.xml', 'https://a.com/1',
                    'https://a.com/1', 'http://a.com/2',
                    'https://a.com/3', 'https://a.com/4']},
         {'image': 'https://cdn.example.com/t.jpg',
          'feeds': ['https://a.com/1', 'http://a.com/2',
                    'https://a.com/3'],
          'sources': {'image': 'twitter'}}),
    ])
    def test_clean_page_meta_caps_and_drops(self, raw, expected):
        assert utl.SeleniumWrapper.clean_page_meta(raw) == expected

    @requires_local_browser
    def test_read_page_meta_reads_a_fixture_article(self, tmp_path):
        """A real page's JSON-LD is found inside a graph past a broken
        block, with its tags and absolute feed urls."""
        url = _write_page(tmp_path, 'article.html', ARTICLE_PAGE)
        home = _write_page(tmp_path, 'home.html', HOME_PAGE)
        sw = utl.SeleniumWrapper()
        try:
            sw.go_to_url(url, sleep=0)
            meta = sw.read_page_meta()
            sw.go_to_url(home, sleep=0)
            headlines = sw.read_headlines('https://www.example.com', 5)
        finally:
            sw.quit()
        assert meta == {
            'title': 'Structured headline',
            'description': 'Open graph summary',
            'image': 'https://cdn.example.com/og.jpg',
            'published': '2026-09-01T08:00:00Z',
            'author': 'Ada Writer, Bo Editor', 'section': 'Reviews',
            'tags': ['rpg', 'indie'],
            'canonical': 'https://www.example.com/news/big-story',
            'site_name': 'Example Site',
            'feeds': ['https://www.example.com/feed.xml',
                      'https://www.example.com/atom.xml'],
            'sources': {'title': 'ld', 'description': 'og', 'image': 'og',
                        'published': 'ld', 'author': 'ld',
                        'section': 'article', 'tags': 'article',
                        'canonical': 'meta', 'site_name': 'og'}}
        assert [x['href'] for x in headlines] == [
            'https://www.example.com/articles/big-story',
            'https://www.example.com/articles/second-story']

    def test_read_page_meta_survives_a_dead_driver(self):
        sw = _bare_wrapper(browser=types.SimpleNamespace(
            execute_script=_raise_read_timeout,
            find_elements=_raise_read_timeout))
        assert sw.read_page_meta() == {}
        assert sw.read_headlines('https://www.example.org', 5) == []
        assert sw.dismiss_dialog() is None

    def test_ad_clickers_are_gone(self):
        """The old frame walk clicked live ads to learn their landing
        pages, which registers clicks on them -- ours included."""
        for name in ('get_all_iframe_ads', 'get_all_iframes',
                     'take_screenshot_get_ads'):
            assert not hasattr(utl.SeleniumWrapper, name)
        assert not hasattr(ssapi.SsApi, 'take_screenshots_get_ads')

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


class TestSsApi:
    """The capture sweep: string site names, per-row ad slots, the run
    manifest and the in-process seam."""

    @staticmethod
    def _api(tmp_path, monkeypatch, **kwargs):
        monkeypatch.chdir(tmp_path)
        return ssapi.SsApi(sites=[{'url': 'example.org',
                                   'partner': 'Example News'}],
                           s3=_FakeS3(), ss_file_path_date='260101_08',
                           **kwargs)

    def test_sites_seam_bypasses_csv(self, tmp_path, monkeypatch):
        api = self._api(tmp_path, monkeypatch)
        rows = list(api.config.values())
        assert sorted(r['device'] for r in rows) == ['Desktop', 'Mobile']
        assert all(r['site'] == 'example.org' for r in rows)
        assert all(r['partner'] == 'Example News' for r in rows)
        assert api.get_site(0).name == 'example.org'
        assert api.get_site(0).url == 'https://www.example.org'

    def test_default_constructor_reads_csv_and_leaves_s3_unset(
            self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'config').mkdir()
        csv = tmp_path / 'config' / 'site_config.csv'
        csv.write_text('url\ngames.example.net\n')
        api = ssapi.SsApi(ss_file_path_date='260101_08')
        assert api.s3 is None
        assert api.ss_file_path == 'screenshots'
        assert [r['site'] for r in api.config.values()] == (
            ['games.example.net'] * 2)
        # The import loop builds one per processor, list or no list.
        csv.unlink()
        assert ssapi.SsApi(ss_file_path_date='260101_08').config == {}

    def test_viewport_density_counts_only_the_first_screen(self):
        """Only the part of a slot on the first screen counts."""
        slots = [{'width': 300, 'height': 250, 'top': 100, 'left': 50},
                 {'width': 728, 'height': 90, 'top': 455, 'left': 0},
                 {'width': 300, 'height': 600, 'top': 900, 'left': 0},
                 {'width': 300, 'height': 250, 'top': None, 'left': None}]
        density = ssapi.SsApi.viewport_density(slots, {'w': 1000, 'h': 500})
        assert density == round((300 * 250 + 728 * 45) / 500000.0, 4)
        assert ssapi.SsApi.viewport_density(slots, {}) is None
        assert ssapi.SsApi.viewport_density([], {'w': 10, 'h': 10}) == 0.0
        overlap = [
            {'width': 50, 'height': 50, 'top': 0, 'left': 0},
            {'width': 50, 'height': 50, 'top': 25, 'left': 25}]
        assert ssapi.SsApi.viewport_density(
            overlap, {'w': 100, 'h': 100}) == 0.4375
        assert ssapi.SsApi.viewport_density(
            overlap + overlap, {'w': 100, 'h': 100}) == 0.4375

    def test_page_vitals_ride_the_manifest_only(self, tmp_path,
                                                monkeypatch):
        """Shown rows carry the lab reading in the manifest only; a
        page that was not shown carries none."""
        api = self._api(tmp_path, monkeypatch)
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper',
                            _FakeSweepBrowser)
        _FakeSweepBrowser.instances = []
        _FakeSweepBrowser.verdicts = [('ok', ''), ('bot_check', 'captcha')]
        df = api.get_data(None, None, None)
        assert not set(ssapi.SsApi.vitals_fields) & set(df.columns)
        rows = api.manifest_rows()
        shown = next(r for r in rows if r['capture_status'] == 'ok')
        blocked = next(r for r in rows if r['capture_status'] != 'ok')
        assert shown['lab_lcp_ms'] == 1800
        assert shown['ad_frames'] == 2
        assert shown['ad_viewport_density'] == 0.15
        assert 'viewport' not in shown
        assert not any(k in blocked for k in ssapi.SsApi.vitals_fields)

    def test_get_data_writes_string_sites_manifest_and_ad_columns(
            self, tmp_path, monkeypatch):
        """Every capture lands as a string-named row with its ad
        columns, and the run's manifest carries the row, the shot and
        its slots under the caller's prefix."""
        api = self._api(tmp_path, monkeypatch, prefix='screenshots/plans/7')
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper',
                            _FakeSweepBrowser)
        _FakeSweepBrowser.instances = []
        df = api.get_data(None, None, None)
        assert df['site'].map(type).eq(str).all()
        assert df['ad_count'].tolist() == [1, 1]
        assert set(df['ad_domains']) == {'landing.example.com'}
        run = 'screenshots/plans/7/260101_08/'
        for name in ('example_Desktop.png', 'example_Desktop_ad0.png',
                     'example_Mobile.png', 'manifest.json'):
            assert run + name in api.s3.uploads
        rows = json.loads(api.s3.uploads[run + 'manifest.json'])
        row = rows[0]
        assert (row['partner'], row['run'], row['device']) == (
            'Example News', '260101_08', 'Desktop')
        assert row['captured_at'] == '2026-01-01T08:00:00'
        assert row['url'] == 'https://www.example.org'
        assert row['img_url'].endswith(run + 'example_Desktop.png')
        assert row['shot_key'] == run + 'example_Desktop.png'
        assert row['ads'][0]['shot_url'].endswith('example_Desktop_ad0.png')
        assert row['ads'][0]['evidence'] == ['marker google_ads_iframe']
        assert 'shot_path' not in row['ads'][0]
        assert os.path.isfile(tmp_path / 'screenshots' / 'plans' / '7'
                              / '260101_08' / 'manifest.json')
        shots = [s for b in _FakeSweepBrowser.instances for s in b.shots]
        assert len(shots) == 2 and all(s[2] for s in shots)
        assert [b.mobile for b in _FakeSweepBrowser.instances] == [
            False, True]
        assert (row['capture_status'], row['capture_detail']) == ('ok', '')
        assert 'capture_status' not in df.columns

    def test_article_row_copies_home_row_and_names_file(
            self, tmp_path, monkeypatch):
        """An article row keeps the home row's own columns and site
        name, takes the article url, and is shot to a file named after
        the home shot with the article ordinal before the device."""
        api = self._api(tmp_path, monkeypatch)
        api.config[0]['ad_count'] = 3
        story = 'https://www.example.org/articles/big-story'
        row, site = api.article_row(0, story, 1)
        assert (row['page_kind'], row['parent_url']) == (
            'article', 'https://www.example.org')
        assert (row['url'], row['site'], row['partner'], row['device']) == (
            story, 'example.org', 'Example News', 'Desktop')
        assert row['pages_walked'] == 0 and 'ad_count' not in row
        assert site.url == story
        assert site.file_name == os.path.join(
            'screenshots', '260101_08', 'example_a1_Desktop.png')
        _, mobile = api.article_row(1, story, 2)
        assert mobile.file_name.endswith('example_a2_Mobile.png')

    def test_write_config_to_df_drops_walk_columns(self, tmp_path,
                                                   monkeypatch):
        """The walk fields ride the manifest only; the sheet and the db
        frame keep their columns, section included."""
        monkeypatch.chdir(tmp_path)
        api = ssapi.SsApi(sites=[{'url': 'example.org',
                                  'partner': 'Example News',
                                  'section': 'gaming'}],
                          s3=_FakeS3(), ss_file_path_date='260101_08')
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper',
                            _FakeSweepBrowser)
        _FakeSweepBrowser.instances = []
        df = api.get_data(None, None, None)
        assert not set(ssapi.SsApi.walk_fields) & set(df.columns)
        assert 'partner' in df.columns
        rows = api.manifest_rows()
        assert all(r['section'] == 'gaming' for r in rows)
        assert all(r['page_kind'] == 'home' for r in rows)

    def test_get_data_walks_one_article_per_shown_home_page(
            self, tmp_path, monkeypatch):
        """Each shown home page gets one article row beside it and a
        full-page jpeg in the bucket; a page not shown is not walked,
        and rows handed in by a caller never walk unless asked."""
        assert self._api(tmp_path, monkeypatch).articles_per_site == 0
        api = self._api(tmp_path, monkeypatch, articles_per_site=1)
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper',
                            _FakeWalkingBrowser)
        _FakeSweepBrowser.instances = []
        _FakeSweepBrowser.verdicts = [
            ('bot_check', 'performing security verification'), ('ok', ''),
            ('ok', '')]
        df = api.get_data(None, None, None)
        run = 'screenshots/260101_08/'
        rows = json.loads(api.s3.uploads[run + 'manifest.json'])
        assert [(r['page_kind'], r['device'], r['pages_walked'])
                for r in rows] == [('home', 'Desktop', 0),
                                   ('home', 'Mobile', 1),
                                   ('article', 'Mobile', 0)]
        article = rows[2]
        assert article['url'] == 'https://www.example.org/articles/story-1'
        assert article['parent_url'] == 'https://www.example.org'
        assert (article['site'], article['partner']) == (
            'example.org', 'Example News')
        assert article['shot_key'] == run + 'example_a1_Mobile.png'
        assert article['full_shot_key'] == run + 'example_a1_Mobile_full.jpg'
        assert article['page_height'] == 5000
        assert rows[1]['full_shot_key'] == run + 'example_Mobile_full.jpg'
        assert rows[1]['full_shot_url'].endswith('example_Mobile_full.jpg')
        assert (rows[0]['full_shot_key'], rows[0]['page_height']) == ('', '')
        assert run + 'example_a1_Mobile_full.jpg' in api.s3.uploads
        assert len(df) == 3 and df['site'].eq('example.org').all()
        assert 'page_kind' not in df.columns

    def test_get_data_writes_capture_status_to_manifest_not_csv(
            self, tmp_path, monkeypatch):
        """A page not shown keeps its verdict on the manifest and gets
        no ad scan; the sheet and db frame never carry the verdict."""
        api = self._api(tmp_path, monkeypatch)
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper',
                            _FakeSweepBrowser)
        _FakeSweepBrowser.instances = []
        _FakeSweepBrowser.verdicts = [
            ('bot_check', 'performing security verification'), ('ok', '')]
        df = api.get_data(None, None, None)
        rows = json.loads(api.s3.uploads['screenshots/260101_08/'
                                         'manifest.json'])
        assert [(r['capture_status'], r['capture_detail']) for r in rows
                ] == [('bot_check', 'performing security verification'),
                      ('ok', '')]
        assert [r['ad_count'] for r in rows] == [0, 1]
        assert not {'capture_status', 'capture_detail'} & set(df.columns)
        sheet = pd.read_csv(tmp_path / 'sites.csv')
        assert 'capture_status' not in sheet.columns

    def test_blocked_reddit_falls_back_to_old_reddit(
            self, tmp_path, monkeypatch):
        """A blocked reddit page is re-shot on old.reddit.com under the
        configured url, and the row says so."""
        monkeypatch.chdir(tmp_path)
        api = ssapi.SsApi(sites=[{'url': 'https://www.reddit.com/r/gaming',
                                  'partner': 'Reddit'}],
                          s3=_FakeS3(), ss_file_path_date='260101_08')
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper',
                            _FakeSweepBrowser)
        _FakeSweepBrowser.instances = []
        _FakeSweepBrowser.verdicts = [
            ('blocked', 'blocked by network security'), ('ok', ''),
            ('ok', '')]
        api.get_data(None, None, None)
        shots = [s for b in _FakeSweepBrowser.instances for s in b.shots]
        assert [s[0] for s in shots] == [
            'https://www.reddit.com/r/gaming',
            'https://old.reddit.com/r/gaming',
            'https://www.reddit.com/r/gaming']
        assert len({s[1] for s in shots[:2]}) == 1
        rows = json.loads(api.s3.uploads['screenshots/260101_08/'
                                         'manifest.json'])
        assert rows[0]['url'] == 'https://www.reddit.com/r/gaming'
        assert (rows[0]['capture_status'], rows[0]['capture_detail']) == (
            'ok', 'via old.reddit.com')
        assert rows[1]['capture_status'] == 'ok'
        assert ssapi.SsApi.fallback_url('https://example.org/news') == ''

    @staticmethod
    def _sweep(api, monkeypatch, browser, verdicts):
        """The frame and bucket manifest of ``api`` run on ``browser``
        fakes answering ``verdicts`` in turn."""
        monkeypatch.setattr(ssapi.utl, 'SeleniumWrapper', browser)
        _FakeSweepBrowser.instances = []
        _FakeSweepBrowser.verdicts = list(verdicts)
        df = api.get_data(None, None, None)
        key = f'{api.ss_file_path}/{api.run_id}/manifest.json'
        return df, json.loads(api.s3.uploads[key])

    @staticmethod
    def _csv_api(tmp_path, text, **kwargs):
        """An SsApi reading ``text`` as its site config csv."""
        (tmp_path / 'config').mkdir(exist_ok=True)
        (tmp_path / 'config' / 'site_config.csv').write_text(text)
        return ssapi.SsApi(ss_file_path_date='260101_09', **kwargs)

    def test_driver_failure_names_the_exception(self, tmp_path,
                                                monkeypatch):
        api = self._api(tmp_path, monkeypatch)
        browser = _FailingBrowser()
        assert ssapi.SsApi.screenshot_site(browser, api.get_site(0)) == (
            [], ('error_page', 'browser stopped answering: ValueError'))
        assert browser.restarts == 1

    def test_second_pass_rescues_and_tags(self, tmp_path, monkeypatch):
        """A home page that shows on the second pass replaces its row,
        tagged, retried on fresh browsers desktop first."""
        monkeypatch.chdir(tmp_path)
        api = ssapi.SsApi(
            sites=[{'url': 'example.org'}, {'url': 'games.example.net'}],
            s3=_FakeS3(), ss_file_path_date='260101_08', second_pass=True)
        first = [('bot_check', 'just a moment'), ('ok', ''),
                 ('ok', ''), ('blank', 'single-colour page')]
        retry = [('ok', ''), ('consent_wall', 'we use cookies')]
        _, rows = self._sweep(api, monkeypatch, _FakeRetryBrowser,
                              first + retry)
        assert [(r['capture_status'], r['capture_detail'])
                for r in rows] == [
            ('ok', 'second pass'), ('ok', ''), ('ok', ''),
            ('consent_wall', 'we use cookies second pass')]
        assert [r['ad_count'] for r in rows] == [1, 1, 1, 1]
        assert [b.mobile for b in _FakeSweepBrowser.instances] == [
            False, True, False, True]
        shot = tmp_path / 'screenshots' / '260101_08' / 'example_Desktop.png'
        assert shot.read_bytes() == b'ok '

    def test_second_pass_keeps_first_verdict(self, tmp_path, monkeypatch):
        """A retry that is no better leaves the first verdict and shot,
        or no shot where the first had none."""
        api = self._api(tmp_path, monkeypatch, second_pass=True)
        first = [('bot_check', 'just a moment'),
                 ('error_page', 'page unreachable: TimeoutException')]
        retry = [('blocked', 'access denied'), ('login_wall', 'log in')]
        _, rows = self._sweep(api, monkeypatch, _FakeRetryBrowser,
                              first + retry)
        assert [(r['capture_status'], r['capture_detail'])
                for r in rows] == first
        folder = tmp_path / 'screenshots' / '260101_08'
        assert (folder / 'example_Desktop.png').read_bytes() == (
            b'bot_check just a moment')
        assert not (folder / 'example_Mobile.png').exists()
        api = ssapi.SsApi(sites=[{'url': 'example.org'}], s3=_FakeS3(),
                          ss_file_path_date='260101_10', second_pass=True)
        api.retry_budget_s = -1
        self._sweep(api, monkeypatch, _FakeRetryBrowser,
                    [('bot_check', 'just a moment'), ('ok', ''), ('ok', '')])
        assert _FakeSweepBrowser.verdicts == [('ok', '')]

    def test_second_pass_off_for_handed_rows(self, tmp_path, monkeypatch):
        """Handed-in rows are shot once; the csv sweep retries only the
        home rows worth it."""
        api = self._api(tmp_path, monkeypatch)
        assert api.second_pass is False
        self._sweep(api, monkeypatch, _FakeRetryBrowser,
                    [('bot_check', 'just a moment'), ('ok', ''), ('ok', '')])
        assert len(_FakeSweepBrowser.instances) == 2
        swept = self._csv_api(tmp_path, 'url\ngames.example.net\n')
        assert swept.second_pass is True
        swept.config[0].update(capture_status='blocked')
        swept.config[1].update(capture_status='blank')
        swept.config[2] = dict(swept.config[0], page_kind='article',
                               capture_status='error_page')
        assert swept.retry_indices() == [1]
        swept.config[0].update(capture_status='error_page',
                               device='Mobile')
        swept.config[1].update(device='Desktop')
        assert swept.retry_indices() == [1, 0]

    def test_ssl_error_toggles_www(self, tmp_path, monkeypatch):
        """A certificate error is retried on the host's www twin either
        way round, and the row names the host shot."""
        twin = ssapi.SsApi.fallback_url
        assert twin('https://www.example.net/news?p=1', 'ssl_error') == (
            'https://example.net/news?p=1')
        assert twin('https://example.net/news', 'ssl_error') == (
            'https://www.example.net/news')
        assert twin('https://www.example.net/news', 'blocked') == ''
        assert twin('https://www.reddit.com/r/gaming', 'blocked') == (
            'https://old.reddit.com/r/gaming')
        assert twin('not a url', 'ssl_error') == ''
        monkeypatch.chdir(tmp_path)
        api = ssapi.SsApi(sites=[{'url': 'https://www.example.net'}],
                          s3=_FakeS3(), ss_file_path_date='260101_08')
        _, rows = self._sweep(
            api, monkeypatch, _FakeSweepBrowser,
            [('ssl_error', 'your connection is not private'), ('ok', ''),
             ('blank', 'single-colour page')])
        shots = [s for b in _FakeSweepBrowser.instances for s in b.shots]
        assert [s[0] for s in shots[:2]] == [
            'https://www.example.net', 'https://example.net']
        assert shots[0][1] == shots[1][1]
        assert [r['capture_detail'] for r in rows] == [
            'via example.net', 'single-colour page']

    def test_existing_manifest_skips_the_run(self, tmp_path, monkeypatch):
        """A second run in the same hour shoots nothing and leaves the
        first run's manifest as it was."""
        self._sweep(self._api(tmp_path, monkeypatch), monkeypatch,
                    _FakeSweepBrowser, [])
        manifest = tmp_path / 'screenshots' / '260101_08' / 'manifest.json'
        written = manifest.read_bytes()
        again = self._api(tmp_path, monkeypatch)
        _FakeSweepBrowser.instances = []
        assert again.get_data(None, None, None).empty
        assert _FakeSweepBrowser.instances == [] and not again.s3.uploads
        assert manifest.read_bytes() == written

    def test_device_capture_keeps_a_profile_per_device(
            self, tmp_path, monkeypatch):
        """The csv sweep's profile folder splits by device, and rows
        handed in by a caller use none."""
        options = ssapi.utl.CaptureOptions(name='trial', paint_wait=6,
                                           profile_dir='profiles')
        api = self._api(tmp_path, monkeypatch, capture=options)
        assert api.device_capture(False) == ssapi.utl.CaptureOptions(
            name='trial', paint_wait=6)
        self._sweep(api, monkeypatch, _FakeSweepBrowser, [])
        assert [b.capture.paint_wait
                for b in _FakeSweepBrowser.instances] == [6, 6]
        swept = self._csv_api(tmp_path, 'url\ngames.example.net\n',
                              capture=options)
        assert [swept.device_capture(m).profile_dir
                for m in (False, True)] == [
            os.path.join('profiles', 'desktop'),
            os.path.join('profiles', 'mobile')]

    def test_second_pass_waits_longer(self, tmp_path, monkeypatch):
        """The second pass lengthens each wait the run has on and turns
        on none it left off."""
        api = self._api(tmp_path, monkeypatch, second_pass=True)
        retry = api.retry_capture()
        for wait, seconds in ssapi.SsApi.retry_waits.items():
            assert getattr(retry, wait) == seconds > getattr(api.capture,
                                                             wait)
        self._sweep(api, monkeypatch, _FakeRetryBrowser,
                    [('bot_check', 'just a moment'), ('ok', ''),
                     ('ok', '')])
        assert _FakeSweepBrowser.instances[-1].capture == retry
        quiet = ssapi.utl.CaptureOptions(page_load_timeout=60)
        assert self._api(tmp_path, monkeypatch,
                         capture=quiet).retry_capture() == quiet

    def test_editorial_fields_ride_the_manifest_only(
            self, tmp_path, monkeypatch):
        """Headlines, feeds and article meta land on the manifest, never
        the frame or sheet; the article walked is the first headline."""
        monkeypatch.chdir(tmp_path)
        api = ssapi.SsApi(
            sites=[{'url': 'example.org', 'partner': 'Example News',
                    'feed': 'https://www.example.org/rss'}],
            s3=_FakeS3(), ss_file_path_date='260101_08',
            articles_per_site=1, headlines_per_site=2)
        assert api.config[0]['headlines'] is not api.config[1]['headlines']
        df, rows = self._sweep(
            api, monkeypatch, _FakeEditorialBrowser,
            [('bot_check', 'just a moment'), ('ok', ''), ('ok', '')])
        hidden = set(ssapi.SsApi.editorial_fields) | {'feed'}
        assert not hidden & set(df.columns)
        assert not hidden & set(pd.read_csv(tmp_path / 'sites.csv').columns)
        blocked, home, article = rows
        assert (blocked['headlines'], blocked['article'],
                blocked['feeds']) == ([], {}, [])
        assert [x['href'] for x in home['headlines']] == [
            'https://www.example.org/articles/lead-1',
            'https://www.example.org/articles/lead-2']
        assert home['feeds'] == ['https://www.example.org/feed.xml']
        assert home['article'] == {}
        assert article['url'] == 'https://www.example.org/articles/lead-1'
        assert article['article'] == {
            'title': 'Structured headline', 'author': 'Ada Writer',
            'sources': {'title': 'ld', 'author': 'ld'}}
        assert (article['headlines'], article['feeds']) == ([], [])
        assert all(r['feed'] == 'https://www.example.org/rss' for r in rows)

    def test_headlines_kept_without_an_article_walk(self, tmp_path,
                                                    monkeypatch):
        """Headlines are read whether or not an article is walked;
        handed-in rows keep none unless asked, the csv sweep 40."""
        api = self._api(tmp_path, monkeypatch, headlines_per_site=3)
        _, rows = self._sweep(api, monkeypatch, _FakeEditorialBrowser, [])
        assert [len(r['headlines']) for r in rows] == [3, 3]
        assert [r['page_kind'] for r in rows] == ['home', 'home']
        quiet = ssapi.SsApi(sites=[{'url': 'example.org'}], s3=_FakeS3(),
                            ss_file_path_date='260101_10')
        _, rows = self._sweep(quiet, monkeypatch, _FakeEditorialBrowser,
                              [])
        assert [r['headlines'] for r in rows] == [[], []]
        assert rows[0]['feeds'] == ['https://www.example.org/feed.xml']
        swept = self._csv_api(tmp_path, 'url,feed\ngames.example.net,\n')
        assert swept.headlines_per_site == 40
        assert swept.config[0]['feed'] == ''

    def test_walk_falls_back_to_the_page_when_no_headline_was_kept(
            self, tmp_path, monkeypatch):
        api = self._api(tmp_path, monkeypatch, articles_per_site=1)
        _, rows = self._sweep(api, monkeypatch, _FakeEditorialBrowser, [])
        assert [r['url'] for r in rows if r['page_kind'] == 'article'] == [
            'https://www.example.org/articles/story-1'] * 2


class TestPmApi:
    def test_pick_brand_takes_only_the_named_brand(self):
        """A fuzzy neighbour never wins; a brand beats an advertiser of
        the same name, then the bigger spender."""
        brands = [
            {'id': 1, 'name': 'Harbor City Raiders', 'type': 'brand',
             'spend': {'value': 900}},
            {'id': 2, 'name': 'Game A Raiders', 'type': 'advertiser',
             'spend': {'value': 500}},
            {'id': 3, 'name': 'Game a  Raiders!', 'type': 'brand',
             'spend': {'value': 10}},
            {'id': 4, 'name': 'GAME A RAIDERS', 'type': 'brand',
             'spend': {'value': 50}}]
        api = pmapi.PmApi()
        assert api.pick_brand('Game A Raiders', brands)['id'] == 4
        assert api.pick_brand('Game A Raiders', brands[:3])['id'] == 3
        assert api.pick_brand('Game A Raiders', brands[:2])['id'] == 2
        assert api.pick_brand('Game B', brands) is None
        assert api.pick_brand('', brands) is None

    def test_series_rows_are_dollars_per_channel_day(self):
        answer = {'currency': {'unit': 'cent'}, 'channels': [
            {'channel': 'youtube', 'timeseries': [
                {'date': '2026-09-01T00:00:00Z', 'spend': 12345,
                 'impressions': 7},
                {'date': '2026-09-02T00:00:00Z', 'spend': 0,
                 'impressions': 0}]},
            {'channel': 'new_channel', 'timeseries': [
                {'date': '2026-09-01', 'spend': 100, 'impressions': 1}]}]}
        assert pmapi.PmApi.series_rows(answer) == [
            {'Date': '2026-09-01', 'Environment-variable': 'YouTube ($)',
             'Environment-value': 123.45, 'Impressions': 7},
            {'Date': '2026-09-01',
             'Environment-variable': 'new_channel ($)',
             'Environment-value': 1.0, 'Impressions': 1}]
        with pytest.raises(ValueError):
            pmapi.PmApi.series_rows({'currency': {'unit': 'yen'}})

    def test_get_file_as_df_appends_creatives(self):
        """Spend and creatives share one frame, a creative dated at the
        window's end; nothing pulled is an empty frame with the
        columns."""
        api = pmapi.PmApi()
        ed = dt.datetime(2026, 9, 2)
        assert list(api.get_file_as_df(ed=ed).columns) == pmapi.FRAME_COLS
        api.frame = pd.DataFrame(pmapi.PmApi.series_rows(
            {'currency': {'unit': 'dollar'}, 'channels': [
                {'channel': 'ott', 'timeseries': [
                    {'date': '2026-09-01', 'spend': 5}]}]}))
        creatives = pd.DataFrame({'Creative': ['https://a.com/ad.png']})
        df = api.get_file_as_df(api.temp_path, creatives, ed)
        assert list(df[pmapi.DATE_COL]) == ['2026-09-01', ed]
        assert list(df[pmapi.SPEND_COL].fillna(0)) == [5, 0]


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

    def test_awapi_loads_a_config_without_its_section(self, tmp_path_factory):
        """A card config that has lost its `adwords:` section still
        runs, reading the file as its own section. The next write of
        the file nests it again.
        """
        config = {'client_customer_id': '123', 'client_id': 'abc',
                  'campaign_filter': 'sem'}
        file_name = '{}/awconfig.yaml'.format(
            tmp_path_factory.mktemp('config'))
        with open(file_name, 'w') as f:
            yaml.dump(config, f)
        api = awapi.AwApi()
        api.configfile = file_name
        api.load_config()
        assert api.client_customer_id == '123'
        assert api.campaign_filter == 'sem'

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


class TestAccountListing:
    """Dict loaders and the account listings the app's pickers call."""

    @staticmethod
    def response(payload, status_code=200):
        return types.SimpleNamespace(status_code=status_code,
                                     json=lambda: payload)

    def test_load_config_dict_skips_the_file(self):
        cases = [
            (fbapi.FbApi(), 'access_token',
             {'app_id': 'a', 'app_secret': 's', 'access_token': 't'}),
            (awapi.AwApi(), 'developer_token',
             {'client_id': 'c', 'client_secret': 's', 'developer_token': 'd',
              'refresh_token': 'r', 'login_customer_id': '1'}),
            (dcapi.DcApi(), 'refresh_url',
             {'client_id': 'c', 'client_secret': 's', 'refresh_token': 'r',
              'refresh_url': 'u'}),
            (amzapi.AmzApi(), 'refresh_token',
             {'client_id': 'c', 'client_secret': 's', 'refresh_token': 'r'}),
            (yvapi.YvApi(), 'client_secret',
             {'client_id': 'c', 'client_secret': 's', 'advertiser': '7'}),
        ]
        for api, key, config in cases:
            api.load_config_dict(config)
            assert getattr(api, key) == config[key]
            assert api.config is config
        assert cases[1][0].login_customer_id == '1'
        assert cases[1][0].client_customer_id == ''
        assert cases[4][0].advertiser == 7

    def test_fb_ad_accounts_export_rows(self, monkeypatch):
        row = types.SimpleNamespace(
            export_all_data=lambda: {'account_id': '1', 'name': 'One'})
        monkeypatch.setattr(fbapi, 'User', lambda fbid: types.SimpleNamespace(
            get_ad_accounts=lambda fields=None: [row]))
        assert fbapi.FbApi.get_ad_accounts() == [
            {'account_id': '1', 'name': 'One'}]

    def test_aw_accessible_customers_strip_prefix(self, monkeypatch):
        api = awapi.AwApi()
        monkeypatch.setattr(api, 'get_client', lambda: {})
        api.client = types.SimpleNamespace(
            get=lambda url, headers=None: self.response(
                {'resourceNames': ['customers/1', 'customers/2']}))
        assert api.get_accessible_customers() == ['1', '2']
        api.client = types.SimpleNamespace(
            get=lambda url, headers=None: self.response({'error': 'x'}))
        assert api.get_accessible_customers() == []

    def test_aw_customer_clients_parse_and_skip_failures(self, monkeypatch):
        api = awapi.AwApi()
        page = [{'results': [{'customerClient': {
            'id': '10', 'descriptiveName': 'Client', 'manager': False}}]}]
        monkeypatch.setattr(api, 'request_report',
                            lambda report: self.response(page))
        assert api.get_customer_clients('9') == [
            {'id': '10', 'name': 'Client', 'manager': False}]
        assert (api.login_customer_id, api.client_customer_id) == ('9', '9')
        monkeypatch.setattr(api, 'request_report',
                            lambda report: self.response([{'error': {}}], 403))
        assert api.get_customer_clients('9') == []
        monkeypatch.setattr(api, 'request_report', lambda report: None)
        assert api.get_customer_clients('9') == []

    def test_dc_user_profiles_share_the_url(self, monkeypatch):
        api = dcapi.DcApi()
        api.usr_id = '55'
        assert api.create_user_url() == (
            'https://www.googleapis.com/dfareporting/v5/userprofiles/55/')
        monkeypatch.setattr(api, 'get_client', lambda: None)
        api.client = types.SimpleNamespace(
            get=lambda url: self.response({'items': [{'profileId': 1}]}))
        assert api.get_user_profiles() == [{'profileId': 1}]
        api.client = types.SimpleNamespace(
            get=lambda url: self.response({'error': {}}, 401))
        assert api.get_user_profiles() == []

    def test_amz_builds_without_a_config_dir(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        assert amzapi.AmzApi().report_cache == {}

    def test_amz_request_profiles_waits_for_a_list(self, monkeypatch):
        api = amzapi.AmzApi()
        monkeypatch.setattr(amzapi.time, 'sleep', lambda s: None)
        answers = [{'code': 'PENDING'}, [{'profileId': 1}]]
        monkeypatch.setattr(
            api, 'make_request',
            lambda url, method, headers=None: self.response(answers.pop(0)))
        assert api.request_profiles(api.eu_url) == [{'profileId': 1}]
        monkeypatch.setattr(
            api, 'make_request',
            lambda url, method, headers=None: self.response({'code': 'NO'}))
        assert api.request_profiles(api.eu_url) == []

    def test_red_request_ad_accounts_feeds_the_username_match(
            self, monkeypatch):
        api = redapi.RedApi()
        api.username = 'Liquid'
        rows = [{'id': 't2_other', 'name': 'other',
                 'time_zone_id': 'America/New_York'},
                {'id': 't2_a', 'name': 'liquid', 'time_zone_id': 'UTC'}]
        monkeypatch.setattr(redapi.requests, 'get',
                            lambda url, headers=None: self.response(
                                {'data': rows}))
        assert api.request_ad_accounts('b1') == rows
        assert api.get_ad_accounts_by_business(['b1']) == 't2_a'
        assert api.time_zone_id == 'UTC'
        api.username = 'T2_A'
        assert api.get_ad_accounts_by_business(['b1']) == 't2_a'
        api.username = 'nobody'
        assert api.get_ad_accounts_by_business(['b1']) == ''
        assert api.time_zone_id == 'UTC'


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

    def test_v5_endpoint_gone_falls_back_to_v4(self, monkeypatch):
        """A missing v5 report endpoint falls back to v4, then pins it."""
        api = self.make_api()
        posts = _FakeRequests([
            _FakeResponse(404, 'gone'),
            _FakeResponse(200, json_data={'report_id': 'abc'})])
        monkeypatch.setattr(simapi.requests, 'post', posts)
        monkeypatch.setattr(simapi.time, 'sleep', _no_sleep)
        sd = ed = dt.datetime.today()
        assert api.make_request(sd, ed) == 'abc'
        assert api.make_request(sd, ed) == 'abc'
        assert posts.calls == [
            'https://api.similarweb.com/batch/v5/request-report',
            'https://api.similarweb.com/batch/v4/request-report',
            'https://api.similarweb.com/batch/v4/request-report']

    def test_every_version_gone_returns_none(self, monkeypatch):
        """When no batch version answers, nothing is pinned or charged."""
        api = self.make_api()
        posts = _FakeRequests([_FakeResponse(404, 'gone')])
        monkeypatch.setattr(simapi.requests, 'post', posts)
        sd = ed = dt.datetime.today()
        assert api.make_request(sd, ed) is None
        assert api.batch_url is None
        assert len(posts.calls) == 2

    def test_status_validate_and_retry_paths(self, monkeypatch):
        """Status is unversioned; validate and retry stay on v3/batch."""
        api = self.make_api()
        gets = _FakeRequests([_FakeResponse(200, json_data={
            'status': 'completed', 'download_url': 'http://d'})])
        posts = _FakeRequests([_FakeResponse(200, json_data={
            'is_valid': True, 'estimated_credits': 3})])
        monkeypatch.setattr(simapi.requests, 'get', gets)
        monkeypatch.setattr(simapi.requests, 'post', posts)
        monkeypatch.setattr(api, 'download_report', _download_stub)
        api.check_report_status('rid')
        api.request_report_retry('rid')
        assert api.make_validate_request()['estimated_credits'] == 3
        assert gets.calls == [
            'https://api.similarweb.com/batch/request-status/rid']
        assert posts.calls == [
            'https://api.similarweb.com/v3/batch/retry/rid',
            'https://api.similarweb.com/v3/batch/request-validate']

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
        table = payload['report_query']['tables'][0]
        assert table['metrics'] == ['all_traffic_visits', 'desktop_visits']
        assert table['vtable'] == 'traffic_and_engagement'

    def test_config_data_version_passthrough(self):
        """A data_version in config rides along."""
        api = self.make_api()
        today = dt.datetime.today()
        api.config = {'data_version': 'VERSION_5.0'}
        table = api.construct_payload(today, today)['report_query'][
            'tables'][0]
        assert table['data_version'] == 'VERSION_5.0'


class _FakeGaClient(object):
    """Stand in for the GA session, breaking n posts before it answers."""

    def __init__(self, breaks, response=None):
        self.breaks = breaks
        self.response = response
        self.calls = 0

    def post(self, url, json=None):
        self.calls += 1
        if self.calls <= self.breaks:
            raise gaapi.requests.exceptions.ChunkedEncodingError(
                'Connection broken: IncompleteRead(1239 bytes read, '
                '9001 more expected)')
        return self.response


class TestGaApi:
    """A GA response body that stops mid stream retries, never raises."""

    @staticmethod
    def make_api(monkeypatch, breaks, rows=True):
        response = _FakeResponse(200, json_data={
            'dimensionHeaders': [{'name': 'date'}],
            'metricHeaders': [{'name': 'sessions'}],
            'rows': [{'dimensionValues': [{'value': '20260826'}],
                      'metricValues': [{'value': '5'}]}]}) if rows else None
        api = gaapi.GaApi()
        api.ga_id = '1234'
        api.max_attempts = 3
        api.client = _FakeGaClient(breaks, response)
        monkeypatch.setattr(api, 'get_client', _no_sleep)
        monkeypatch.setattr(gaapi.time, 'sleep', _no_sleep)
        return api

    def test_broken_read_retries_then_recovers(self, monkeypatch):
        """The df still comes back once a later attempt reads cleanly."""
        api = self.make_api(monkeypatch, breaks=2)
        df = api.get_data()
        assert api.client.calls == 3
        assert list(df['sessions']) == ['5']

    def test_broken_read_exhausts_retries_to_empty_df(self, monkeypatch):
        """Retries spent, the vendor yields an empty df instead of killing
        the run - importhandler leaves the last good raw file alone."""
        api = self.make_api(monkeypatch, breaks=99)
        df = api.get_data()
        assert api.client.calls == api.max_attempts
        assert df.empty
        assert api.r is None


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

    def test_string_replace_all_transform(self):
        df = pd.DataFrame({
            'Campaign Name': ['camp a|camp b|camp c', 'camp 1|camp 2|camp 3'],
            'Adset Name': ['adset a|adset b', 'adset 1'],
            'Adgroup Name': ['adgroup a', 'adgroup 1|adgroup 2|adgroup 3'],
            'Partner Name': ['Partner|1', 'Partner|2'],
        })
        transform = 'StringReplaceAll::|::_'
        delim_cols = ['Campaign Name', 'Adset Name', 'Adgroup Name']
        result = vm.df_transform(df, transform, [], delim_cols)
        expected = ['camp a_camp b_camp c', 'camp 1_camp 2_camp 3']
        assert result['Campaign Name'].tolist() == expected
        assert result['Adset Name'].tolist() == ['adset a_adset b', 'adset 1']
        expected = ['adgroup a', 'adgroup 1_adgroup 2_adgroup 3']
        assert result['Adgroup Name'].tolist() == expected
        assert result['Partner Name'].tolist() == ['Partner|1', 'Partner|2']

    @requires_base_config
    def test_vm_load(self):
        matrix = vm.VendorMatrix()
        assert matrix.vm
        bar_col = vmc.barsplitcol[0]
        plan_val = matrix.vm[bar_col][vm.plan_key]
        assert isinstance(plan_val, list)

    @pytest.mark.parametrize('contents', [
        '', 'FILENAME,Placement Name\n', 'not a csv\n"unclosed\n'])
    def test_vm_parse_survives_an_unreadable_file(self, tmp_path, monkeypatch,
                                                  contents):
        """An empty or unparsable Vendormatrix.csv read back as None or
        as a frame without a Vendor Key column, and everything indexes
        the matrix by that column, so opening a processor whose file had
        been truncated raised a KeyError instead of a warning."""
        os.makedirs(os.path.join(tmp_path, utl.config_path))
        with open(os.path.join(tmp_path, vm.csv_full_file), 'w') as f:
            f.write(contents)
        monkeypatch.chdir(tmp_path)
        matrix = vm.VendorMatrix()
        assert vmc.vendorkey in matrix.vm_df.columns
        assert not matrix.vm_df.columns.duplicated().any()
        assert matrix.vm_df.empty
        assert matrix.vl == [vm.plan_key]
        assert vm.ImportConfig(matrix=matrix).get_current_imports() == []

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

    def test_get_config_file_value_without_the_section(self):
        """An Adwords config that has lost its `adwords:` section reads
        from the root instead of raising, so the card keeps reporting
        its account id. Reading one used to KeyError, which took down
        every Set Imports save on the processor.
        """
        get_value = vm.ImportConfig.get_config_file_value
        nested = {'adwords': {'client_customer_id': '123'}}
        assert get_value(nested, 'client_customer_id', 'adwords') == '123'
        flat = {'client_customer_id': '456'}
        assert get_value(flat, 'client_customer_id', 'adwords') == '456'
        assert get_value(flat, 'campaign_filter', 'adwords') == ''

    def test_set_config_file_value_creates_the_section(self):
        """Writing a nested param into a flattened config nests it,
        leaving no second answer at the root."""
        config = vm.ImportConfig.set_config_file_value(
            {'client_customer_id': '456'}, 'client_customer_id', '123',
            'adwords')
        assert config == {'adwords': {'client_customer_id': '123'}}

    def test_set_config_file_value_skips_an_unnamed_param(self):
        """A Key with no import_config row falls back to the raw file
        params, whose account id column is empty; that must not stamp a
        nan key into the config file it copied."""
        config = vm.ImportConfig.set_config_file_value(
            {'campaign_id': ''}, np.nan, '123')
        assert config == {'campaign_id': ''}


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

    def test_metric_cap_keeps_pre_cap_cost(self, tmp_path):
        cap_file = tmp_path / 'cap.csv'
        pd.DataFrame({'Package Description': ['pkd'],
                      'Net Cost (Capped)': [500.0]}).to_csv(
            cap_file, index=False)
        config = tmp_path / 'cap_config.csv'
        pd.DataFrame({'file_name': [str(cap_file)],
                      'file_dim': ['Package Description'],
                      'file_metric': ['Net Cost (Capped)'],
                      'processor_dim': [dctc.PKD],
                      'processor_metric': [dctc.PNC]}).to_csv(
            config, index=False)
        df = pd.DataFrame({vmc.date: ['1/1/23', '1/2/23'],
                           dctc.FPN: ['fpn', 'fpn'],
                           dctc.PKD: ['pkd', 'pkd'],
                           vmc.cost: [300.0, 400.0]})
        df = cal.MetricCap(config_file=str(config)).apply_all_caps(df)
        assert df[cal.NC_PRE_CAP].tolist() == [300.0, 400.0]
        assert df[vmc.cost].tolist() == [300.0, 200.0]

    def test_net_plan_comp_string_uncapped(self):
        for flags in (['True', ''], [True, False]):
            df = pd.DataFrame({dctc.PFPN: ['a', 'b'],
                               dctc.PNC: [100.0, 100.0],
                               vmc.cost: [50.0, 50.0],
                               dctc.UNC: flags})
            df = cal.net_plan_comp(df)
            assert df[cal.DIF_PNC].isna().tolist() == [True, False]

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
        pn = '28091057_Ven1_US_All_0_0_0_Flat_0_44768_Click Tracker_0.013_0_'
        pn += 'CPE_Brand Page_Brand_0.1_0_V_Cross Device_1080x1080_Video '
        pn += 'SK_IG In-Feed_Social Post_Social_All'
        df = pd.DataFrame({
            vmc.clicks: [1],
            vmc.date: [44755],
            vmc.cost: [0],
            vmc.vendorkey: ['API_DCM_GameA2022BrandCampaign'],
            dctc.PN: [pn],
            dctc.BM: ['Flat'],
            dctc.BR: [0],
            dctc.CAM: ['Brand'],
            dctc.COU: ['US'],
            dctc.PKD: ['Social Post'],
            dctc.PD: [44767],
            dctc.VEN: ['Ven1'],
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
            dctc.VEN: ['Ven1'],
            dctc.COU: ['US'],
            dctc.PN: [
                '28091057_Ven1_US_All_0_0_0_Flat_0_44768_Click '
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

    @requires_base_config
    def test_max_date_repeat_split(self):
        """APIs with short windows split on every run.  Each new vendor key
        must carry only its own start date, otherwise the stamps stack and
        the name grows past the file name length limit."""
        base_vk = 'API_Amazon_Repeat'
        start_date = (dt.datetime.today() - dt.timedelta(
            days=120)).strftime('%Y-%m-%d')
        end_date = dt.datetime.today().strftime('%Y-%m-%d')
        vm_dict = pd.DataFrame({vmc.vendorkey: [base_vk],
                                vmc.startdate: [start_date],
                                vmc.enddate: [end_date],
                                vmc.filename: ['raw_data/amazon_repeat.csv'],
                                vmc.date: [vmc.date]})
        matrix = vm.VendorMatrix()
        matrix.vm_parse(vm_dict)
        adl = az.CheckApiDateLength(az.Analyze(matrix=matrix))
        vk = base_vk
        for _ in range(3):
            aly_dict = pd.DataFrame({vmc.vendorkey: [vk],
                                     adl.highest_date: [None],
                                     adl.name: [31]})
            f_df = adl.fix_analysis(aly_dict=aly_dict, write=False)
            vk = [x for x in f_df[vmc.vendorkey] if 'API_' in x][0]
            assert vk.count('_20') == 1
            assert utl.remove_date_suffix(vk) == base_vk
        new_fn = f_df[f_df[vmc.vendorkey] == vk][vmc.filename].iloc[0]
        assert new_fn == '{}.csv'.format(vk.replace('API_', '').lower())

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

    def test_package_cap_status_floats(self):
        cpc = az.CheckPackageCapping(az.Analyze())
        df = pd.DataFrame({
            dctc.VEN: [np.nan, 'Facebook', 'Adwords'],
            dctc.PKD: ['Pkg', 'Pkg', 'Pkg'],
            cpc.plan_net_temp: [100.0, np.nan, np.nan],
            vmc.cost: [np.nan, 60.0, 40.0],
            cal.NC_PRE_CAP: [np.nan, 80.0, 70.0]})
        df = cpc.check_package_cap(df, dctc.PKD)
        assert df['Cap'][0] == '150.00%'
        status = [x for x in cpc.aly.analysis_dict
                  if x[az.Analyze.analysis_dict_param_col] ==
                  az.Analyze.package_cap_status][0]
        status = pd.DataFrame(status[az.Analyze.analysis_dict_data_col])
        assert status['Cap'][0] == 100.0
        assert status['Delivered'][0] == 150.0
        assert status['Capped'][0] == 100.0
        assert status['Vendors'][0] == 'Adwords, Facebook'
        assert status['Percent'][0] == 1.5

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


class FakeConn:
    """A raw_connection() double: it remembers being closed."""

    def __init__(self):
        self.closed = False

    def cursor(self):
        return object()

    def close(self):
        self.closed = True


class FakeEngine:
    """An engine double that counts checkouts and disposals, and can
    refuse the next ``fail_times`` connects the way psycopg2 does when
    the host is out of ephemeral ports."""

    def __init__(self):
        self.conns = []
        self.checkouts = 0
        self.disposals = 0
        self.fail_times = 0

    def raw_connection(self):
        self.checkouts += 1
        if self.fail_times:
            self.fail_times -= 1
            raise psycopg2.OperationalError(
                'connection to server at "127.0.0.1", port 5434 failed: '
                'Address already in use')
        self.conns.append(FakeConn())
        return self.conns[-1]

    def dispose(self):
        self.disposals += 1


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

    @staticmethod
    def make_db(monkeypatch, engines):
        """An exp.DB whose engine is a recording double."""
        db = exp.DB()
        db.host, db.conn_string = 'h', 'postgresql://u:p@h:1/d'
        monkeypatch.setattr(
            exp.sqa, 'create_engine',
            lambda *a, **kw: engines.append(FakeEngine()) or engines[-1])
        return db

    def test_close_is_safe_with_nothing_open(self):
        exp.DB().close()

    def test_connect_reuses_one_engine(self, monkeypatch):
        """An upload connects several times per table across ~25
        tables. Building an engine per statement opened a socket every
        time and closed none of them, and a host that runs out of
        ephemeral ports fails the export mid-write ("Address already
        in use"). One engine, and the last checkout handed back before
        the next."""
        engines = []
        db = self.make_db(monkeypatch, engines)
        for _ in range(5):
            db.connect()
        assert len(engines) == 1, 'an engine per connect leaks sockets'
        assert engines[0].checkouts == 5
        assert sum(c.closed for c in engines[0].conns) == 4
        db.close()
        assert all(c.closed for c in engines[0].conns)
        assert db.connection is None

    def test_connect_retries_a_refused_connection(self, monkeypatch):
        """Port pressure and a restarting database both clear on
        their own, so a refused connect backs off and tries again
        rather than ending the run. psycopg2 raises its own
        OperationalError through raw_connection() -- the SQLAlchemy
        wrapper this used to catch never sees it."""
        engines = []
        db = self.make_db(monkeypatch, engines)
        monkeypatch.setattr(exp.time, 'sleep', lambda s: None)
        db.connect()
        engines[0].fail_times = 2
        db.connect()
        assert engines[0].checkouts == 4, 'two failures, then a hand-back'
        assert engines[0].disposals == 2
        engines[0].fail_times = exp.CONNECT_ATTEMPTS
        with pytest.raises(psycopg2.OperationalError):
            db.connect()


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

    @requires_base_config
    def test_import_plan_data_missing_col(self):
        df = pd.DataFrame({
            vmc.vendorkey: ['API_Test1', 'API_Test2'],
            dctc.CAM: ['Camp1', 'Camp2'],
            dctc.VEN: ['Ven1', 'Ven2'],
            vmc.date: pd.to_datetime(['2025-01-01', '2025-01-02']),
        })
        cur_path = os.getcwd()
        key = vm.plan_key
        kwargs = {
            vmc.fullplacename: [dctc.CAM, 'mpPackacge Description',
                                dctc.VEN],
            vmc.vendorkey: [key],
            vmc.filenamedict: os.path.join(cur_path, utl.dict_path,
                                           dctc.PFN),
            vmc.filenameerror: os.path.join(
                cur_path, utl.error_path, 'PLANNET_ERROR_REPORT.csv')
        }
        result = vm.import_plan_data(key, df, [], **kwargs)
        assert isinstance(result, pd.DataFrame)
        assert 'mpPackacge Description' not in result.columns
        assert dctc.CAM in result.columns

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
            'appid': 400030, 'app_detail_name': 'Game C',
            'publishers': ['Example Studio'],
            'developers': ['Example Studio'],
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
        game = gdb.upsert_game(s, 'Game C',
                               **gamesw.game_fields(row))
        assert game.gameid and game.steam_appid == 400030
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
        again = gdb.upsert_game(s, 'Game C',
                                **gamesw.game_fields(row))
        assert again.gameid == game.gameid
        assert gdb.upsert_fact(s, gmdl.GameEvent, key,
                               gamesw.event_fields(row)) == 0
        s.commit()
        assert s.query(gmdl.Game).count() == 1
        assert s.query(gmdl.GameEvent).count() == 1

    def test_upsert_game_fills_without_clobbering(self):
        s = self._session()
        seeded = gdb.upsert_game(s, 'Game A Prime',
                                 registry_slug='game-a-prime',
                                 publisher='Example Publisher')
        merged = gdb.upsert_game(s, 'Game A Prime',
                                 registry_slug='game-a-prime',
                                 opencritic_id=42, developer='Example Dev')
        assert merged.gameid == seeded.gameid
        assert merged.publisher == 'Example Publisher'  # filled value kept
        assert merged.opencritic_id == 42 and merged.developer == 'Example Dev'
        assert s.query(gmdl.Game).count() == 1

    def test_writer_field_helpers(self):
        row = self._wide_row()
        fields = gamesw.game_fields(row)
        assert fields['steam_appid'] == 400030
        assert fields['publisher'] == 'Example Studio'
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
        reg = gdb.upsert_game(s, 'Game A Prime',
                              registry_slug='game-a-prime')
        steam = gdb.upsert_game(s, 'Game A Prime', match_name=True,
                                steam_appid=400010)
        assert steam.gameid == reg.gameid
        assert steam.registry_slug == 'game-a-prime'
        assert steam.steam_appid == 400010
        # The reverse order knits too (case-insensitive).
        first = gdb.upsert_game(s, 'GAME B', match_name=True,
                                steam_appid=400020)
        second = gdb.upsert_game(s, 'Game B', match_name=True,
                                 registry_slug='game-b')
        assert second.gameid == first.gameid
        assert s.query(gmdl.Game).count() == 2
        # A name collision carrying a conflicting identity is a
        # different game (remake/re-release): new row, no clobber.
        clash = gdb.upsert_game(s, 'Game B', match_name=True,
                                steam_appid=999)
        assert clash.gameid != first.gameid
        assert first.steam_appid == 400020
        # Without match_name, a bare name never matches (old behavior).
        other = gdb.upsert_game(s, 'Game A Prime', opencritic_id=7)
        assert other.gameid != reg.gameid

    def test_title_score_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        day = dt.date(2026, 7, 17)
        key = {'score_date': day, 'title': 'Game A Prime'}
        fields = {'gameid': game.gameid, 'influence': 1.2,
                  'engagement': -0.4, 'momentum': 0.3, 'composite': 1.1,
                  'headline_metric': 'Player Share', 'current': 0.5,
                  'prior': 0.4, 'share': 0.62, 'share_delta': 0.02,
                  'movement': 'Rising', 'primary_period': '2026-07',
                  'comparison_period': '2026-06', 'genre': 'Shooter',
                  'set_size': 40, 'signals': 7}
        assert gdb.upsert_fact(s, gmdl.TitleScore, key, fields) == 1
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
            title='Game A Prime').one()
        assert float(row.share) == 0.64
        assert row.gameid == game.gameid
        assert row.movement == 'Rising'
        assert row.genre == 'Shooter'
        assert row.set_size == 40 and row.signals == 7

    def test_new_games_facts_use_full_natural_keys(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        spend = {'spend_date': dt.date(2025, 9, 10),
                 'brand': 'Game E', 'channel': 'ALL',
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

    def test_attention_facts_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        week = dt.date(2026, 8, 10)
        si_key = {'title': 'Game A Prime', 'week_start': week,
                  'geo': 'GLOBAL'}
        assert gdb.upsert_fact(
            s, gmdl.SearchInterest, si_key,
            {'gameid': game.gameid, 'interest': 40, 'raw_interest': 20,
             'anchor': 'Game F', 'anchor_value': 50}) == 1
        share_key = {'week_start': week, 'title': 'Game A Prime'}
        assert gdb.upsert_fact(
            s, gmdl.AttentionShare, share_key,
            {'gameid': game.gameid, 'attention_share': 0.4,
             'twitch_viewers': 1200, 'signals_present': 3,
             'set_size': 2}) == 1
        # Unmatched titles land too, with a NULL gameid.
        assert gdb.upsert_fact(
            s, gmdl.AttentionShare,
            {'week_start': week, 'title': 'Mystery Title'},
            {'gameid': None, 'attention_share': 0.6}) == 1
        s.commit()
        # Reruns update in place — the Trends rolling window and the
        # weekly derive both overwrite by design.
        assert gdb.upsert_fact(s, gmdl.SearchInterest, si_key,
                               {'interest': 44}) == 0
        assert gdb.upsert_fact(s, gmdl.AttentionShare, share_key,
                               {'attention_share': 0.38}) == 0
        s.commit()
        assert s.query(gmdl.SearchInterest).count() == 1
        assert s.query(gmdl.AttentionShare).count() == 2
        row = s.query(gmdl.SearchInterest).one()
        assert float(row.interest) == 44
        assert row.gameid == game.gameid
        share = s.query(gmdl.AttentionShare).filter_by(
            title='Game A Prime').one()
        assert float(share.attention_share) == 0.38

    def test_youtube_video_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        video = gmdl.YoutubeVideo(
            video_id='abc123', gameid=game.gameid, kind='official',
            source='igdb', label='Launch Trailer')
        s.add(video)
        s.flush()
        for day, views in ((dt.date(2026, 8, 20), 1000),
                           (dt.date(2026, 8, 21), 1500)):
            assert gdb.upsert_fact(
                s, gmdl.YoutubeVideoStat,
                {'youtubevideoid': video.youtubevideoid,
                 'stat_date': day}, {'views': views}) == 1
        s.commit()
        # A rerun on the same day updates in place.
        assert gdb.upsert_fact(
            s, gmdl.YoutubeVideoStat,
            {'youtubevideoid': video.youtubevideoid,
             'stat_date': dt.date(2026, 8, 21)}, {'views': 1600}) == 0
        s.commit()
        assert s.query(gmdl.YoutubeVideoStat).count() == 2
        latest = s.query(gmdl.YoutubeVideoStat).filter_by(
            stat_date=dt.date(2026, 8, 21)).one()
        assert float(latest.views) == 1600
        assert latest.youtubevideoid == video.youtubevideoid

    def test_descriptor_neighbour_alias_upserts_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        rival = gdb.upsert_game(s, 'Game D', registry_slug='game-d')
        desc_key = {'gameid': game.gameid}
        assert gdb.upsert_fact(
            s, gmdl.GameDescriptor, desc_key,
            {'igdb_id': 1, 'genres': 'Shooter', 'themes': 'Sci-fi',
             'similar_igdb_ids': '2, 3',
             'first_release_date': dt.date(2021, 12, 8)}) == 1
        assert gdb.upsert_fact(
            s, gmdl.GameAlias, {'alias_key': 'game a'},
            {'gameid': game.gameid, 'alias': 'Game A',
             'source': 'test'}) == 1
        s.add(gmdl.GameNeighbour(
            gameid=game.gameid, neighbour_gameid=rival.gameid, rank=1,
            score=0.7, evidence_weight=0.5,
            components={'genre': {'s': 1, 'w': 0.16,
                                  'shared': ['Shooter'],
                                  'label': 'Shooter'}},
            computed_at=dt.datetime(2026, 9, 9, 11)))
        s.commit()
        assert gdb.upsert_fact(s, gmdl.GameDescriptor, desc_key,
                               {'themes': 'Sci-fi, War'}) == 0
        assert gdb.upsert_fact(s, gmdl.GameAlias, {'alias_key': 'game a'},
                               {'alias': 'GAME A'}) == 0
        s.commit()
        assert s.query(gmdl.GameDescriptor).one().themes == 'Sci-fi, War'
        assert s.query(gmdl.GameAlias).one().alias == 'GAME A'
        edge = s.query(gmdl.GameNeighbour).one()
        assert edge.components['genre']['shared'] == ['Shooter']
        s.add(gmdl.GameNeighbour(
            gameid=game.gameid, neighbour_gameid=rival.gameid, rank=2,
            score=0.1, computed_at=dt.datetime(2026, 9, 9, 11)))
        import sqlalchemy.exc as sa_exc
        with pytest.raises(sa_exc.IntegrityError):
            s.commit()

    def test_pulse_and_price_upserts_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        video = gmdl.YoutubeVideo(
            video_id='abc123', gameid=game.gameid, kind='official',
            source='igdb', label='Launch Trailer')
        s.add(video)
        s.flush()
        slot = dt.datetime(2026, 8, 31, 8)
        pulse_key = {'gameid': game.gameid, 'sampled_at': slot}
        video_key = {'youtubevideoid': video.youtubevideoid,
                     'sampled_at': slot}
        assert gdb.upsert_fact(
            s, gmdl.CommunityPulse, pulse_key,
            {'twitch_viewers': 42000, 'twitch_channels': 310,
             'sponsored_streams': 2}) == 1
        assert gdb.upsert_fact(
            s, gmdl.StreamFlag,
            {'gameid': game.gameid, 'sampled_at': slot,
             'channel': 'streamer_one'},
            {'title': 'Game A w/ sponsor #ad', 'token': '#ad',
             'viewer_count': 1200}) == 1
        assert gdb.upsert_fact(s, gmdl.YoutubeVideoPulse, video_key,
                               {'views': 1000, 'likes': 10,
                                'comments': 1}) == 1
        s.commit()
        assert gdb.upsert_fact(
            s, gmdl.CommunityPulse, pulse_key,
            {'twitch_viewers': 43000}) == 0
        assert gdb.upsert_fact(
            s, gmdl.StreamFlag,
            {'gameid': game.gameid, 'sampled_at': slot,
             'channel': 'streamer_one'}, {'viewer_count': 1300}) == 0
        assert gdb.upsert_fact(s, gmdl.YoutubeVideoPulse, video_key,
                               {'views': 1200}) == 0
        s.commit()
        assert s.query(gmdl.CommunityPulse).count() == 1
        assert s.query(gmdl.StreamFlag).count() == 1
        pulse = s.query(gmdl.CommunityPulse).one()
        assert float(pulse.twitch_viewers) == 43000
        assert float(pulse.sponsored_streams) == 2
        sample = s.query(gmdl.YoutubeVideoPulse).one()
        assert float(sample.views) == 1200
        assert float(sample.likes) == 10
        assert sample.sampled_at == slot
        price_key = {'gameid': game.gameid,
                     'price_date': dt.date(2026, 8, 31)}
        assert gdb.upsert_fact(
            s, gmdl.PriceSnapshot, price_key,
            {'currency': 'USD', 'base_price': 59.99,
             'final_price': 29.99, 'discount_pct': 50}) == 1
        assert gdb.upsert_fact(
            s, gmdl.PriceSnapshot, price_key,
            {'final_price': 59.99, 'discount_pct': 0}) == 0
        s.commit()
        snap = s.query(gmdl.PriceSnapshot).one()
        assert float(snap.discount_pct) == 0
        assert float(snap.base_price) == 59.99

    def test_store_asset_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        key = {'gameid': game.gameid,
               'checked_at': dt.date(2026, 9, 1),
               'asset_kind': 'screenshots'}
        assert gdb.upsert_fact(
            s, gmdl.StoreAsset, key,
            {'digest': 'abc', 'item_count': 6,
             'sample': 'https://cdn/ss_1.jpg', 'changed': False}) == 1
        s.commit()
        assert gdb.upsert_fact(
            s, gmdl.StoreAsset, key, {'digest': 'def',
                                      'changed': True}) == 0
        s.commit()
        row = s.query(gmdl.StoreAsset).one()
        assert row.digest == 'def' and row.changed is True
        assert row.item_count == 6

    def test_critic_review_upsert_idempotent(self):
        """A re-fetch restamps the row in place, and an unscored
        review is legal."""
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        key = {'review_id': '9001'}
        stamp = dt.datetime(2026, 9, 4, 8, 0)
        assert gdb.upsert_fact(
            s, gmdl.CriticReview, key,
            {'gameid': game.gameid, 'opencritic_id': 42,
             'outlet': 'Example Outlet', 'score': 90,
             'published_date': dt.date(2026, 9, 1),
             'fetched_at': stamp}) == 1
        s.commit()
        later = stamp + dt.timedelta(days=1)
        assert gdb.upsert_fact(
            s, gmdl.CriticReview, key,
            {'score': None, 'fetched_at': later}) == 0
        s.commit()
        row = s.query(gmdl.CriticReview).one()
        assert row.score is None and row.fetched_at == later
        assert row.outlet == 'Example Outlet' and row.gameid == game.gameid

    def test_critic_review_id_unique_constraint(self):
        """The natural key is OpenCritic's review id, so a second row
        carrying it is a duplicate whatever game it claims."""
        s = self._session()
        game = gdb.upsert_game(s, 'Game A', opencritic_id=42)
        s.commit()
        fields = dict(gameid=game.gameid, opencritic_id=42,
                      review_id='9001',
                      fetched_at=dt.datetime(2026, 9, 4))
        s.add(gmdl.CriticReview(**fields))
        s.commit()
        s.add(gmdl.CriticReview(**fields))
        assert gdb.safe_commit(s, 'test') is False

    def test_store_asset_fields_shapes(self):
        assert gamesw.store_asset_fields(None) == []
        assert gamesw.store_asset_fields({}) == []
        data = {
            'header_image': 'https://cdn/header.jpg?t=1700000000',
            'screenshots': [
                {'id': 0, 'path_full': 'https://cdn/ss_a.jpg?t=1'},
                {'id': 1, 'path_full': 'https://cdn/ss_b.jpg?t=2'}],
            'movies': [{'id': 256, 'name': 'Launch Trailer',
                        'webm': {'max': 'https://cdn/m.webm?t=3'}}],
            'short_description': '  Fight   the\ninvaders.  '}
        rows = dict(gamesw.store_asset_fields(data))
        assert list(rows) == list(gamesw.ASSET_KINDS)
        restamped = dict(gamesw.store_asset_fields({
            **data, 'header_image': 'https://cdn/header.jpg?t=9',
            'screenshots': [
                {'id': 0, 'path_full': 'https://cdn/ss_a.jpg?t=7'},
                {'id': 1, 'path_full': 'https://cdn/ss_b.jpg?t=8'}]}))
        for kind in gamesw.ASSET_KINDS:
            assert rows[kind]['digest'] == restamped[kind]['digest']
        assert rows['header']['sample'] == 'https://cdn/header.jpg'
        assert rows['screenshots']['item_count'] == 2
        assert rows['movies']['sample'] == 'Launch Trailer'
        assert rows['description']['sample'] == 'Fight the invaders.'
        swapped = dict(gamesw.store_asset_fields({
            **data, 'header_image': 'https://cdn/header_v2.jpg'}))
        assert swapped['header']['digest'] != rows['header']['digest']
        partial = dict(gamesw.store_asset_fields(
            {'short_description': 'Only words.'}))
        assert list(partial) == ['description']

    def test_review_text_and_theme_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime',
                               registry_slug='game-a-prime')
        text_key = {'recommendationid': 900001}
        assert gdb.upsert_fact(
            s, gmdl.ReviewText, text_key,
            {'gameid': game.gameid, 'language': 'english',
             'review_text': 'Great campaign, rough netcode.',
             'voted_up': 1, 'votes_up': 12,
             'created_at': dt.datetime(2026, 8, 1, 5)}) == 1
        assert gdb.upsert_fact(
            s, gmdl.ReviewText, {'recommendationid': 900002},
            {'gameid': game.gameid, 'voted_up': 0,
             'created_at': dt.datetime(2026, 8, 2, 5)}) == 1
        s.commit()
        # A refetch updates in place; fields it does not send (the
        # classification columns) keep their values.
        assert gdb.upsert_fact(s, gmdl.ReviewText, text_key,
                               {'votes_up': 15}) == 0
        s.commit()
        assert s.query(gmdl.ReviewText).count() == 2
        row = s.query(gmdl.ReviewText).filter_by(
            recommendationid=900001).one()
        assert float(row.votes_up) == 15
        assert row.review_text == 'Great campaign, rough netcode.'
        theme_key = {'gameid': game.gameid, 'theme': 'performance_bugs'}
        assert gdb.upsert_fact(
            s, gmdl.ReviewTheme, theme_key,
            {'mentions': 5, 'share': 0.25, 'sample_size': 20,
             'taxonomy_version': 1}) == 1
        assert gdb.upsert_fact(
            s, gmdl.ReviewTheme, theme_key,
            {'mentions': 6, 'taxonomy_version': 2}) == 0
        s.commit()
        assert s.query(gmdl.ReviewTheme).count() == 1
        theme = s.query(gmdl.ReviewTheme).one()
        assert float(theme.mentions) == 6
        assert theme.taxonomy_version == 2

    def test_game_release_upsert_idempotent(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game A Prime', igdb_id=1105,
                               registry_slug='game-a-prime')
        assert game.igdb_id == 1105
        key = {'igdb_id': 1105}
        fields = {'gameid': game.gameid, 'title': 'Game A Prime',
                  'slug': 'game-a-prime',
                  'release_date': dt.date(2026, 11, 15), 'hypes': 320,
                  'genres': 'Shooter', 'platforms': 'PC, Console'}
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
        reg = gdb.upsert_game(s, 'Game A Prime',
                              registry_slug='game-a-prime')
        knit = gdb.upsert_game(s, 'Game A Prime', match_name=True,
                               igdb_id=1105)
        assert knit.gameid == reg.gameid and knit.igdb_id == 1105
        assert gdb.find_game(s, igdb_id=1105).gameid == reg.gameid
        # A name collision carrying a different igdb_id is a new row.
        clash = gdb.upsert_game(s, 'Game A Prime', match_name=True,
                                igdb_id=9999)
        assert clash.gameid != reg.gameid
        assert reg.igdb_id == 1105
        assert s.query(gmdl.Game).count() == 2

    def test_upsert_game_stamps_provenance(self):
        s = self._session()
        game = gdb.upsert_game(s, 'Game C', steam_appid=400030)
        assert game.first_seen_at is not None
        first_seen = game.first_seen_at
        again = gdb.upsert_game(s, 'Game C', steam_appid=400030)
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
    def _api(titles='Game A', country_filter=None):
        api = nzapi.NzApi()
        api.game_title = titles
        api.api_key = 'k'
        api.country_filter = country_filter
        return api

    def test_month_span_and_slugify(self):
        months = nzapi.NzApi.month_span(dt.datetime(2026, 5, 15),
                                        dt.datetime(2026, 7, 2))
        assert months == {'2026-05', '2026-06', '2026-07'}
        assert nzapi.NzApi.slugify("The Captain's Game 2") == \
            'the-captain-s-game-2'

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
        api = self._api(titles='Game A,Game C')
        df = pd.DataFrame({
            'title': ['Game A', 'Game B', 'Game C'],
            'date': [dt.date(2026, 6, 1)] * 3,
            'country_code': ['ZZ', 'ZZ', 'US'],
            'mau': [1, 2, 3],
            'source': [['b', 'a'], None, ['c']]})
        out = api.shape_df(df)
        assert list(out['title']) == ['Game A', 'Game C']
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


class TestTikApiAdIds:
    """A TikTok report only carries ad ids, so the ad and campaign names
    are pulled separately and joined on, and that pull can come back
    empty for an advertiser the token cannot fully see."""

    @staticmethod
    def make_api(campaign_id=None):
        api = tikapi.TikApi()
        api.advertiser_id = '123'
        api.campaign_id = campaign_id
        api.set_headers()
        return api

    @staticmethod
    def report_response(ad_id='1'):
        return _FakeResponse(200, json_data={'data': {
            'list': [{'dimensions': {'ad_id': ad_id,
                                     'stat_time_day': '2026-09-10'},
                      'metrics': {'spend': '1.5', 'impressions': '10'}}],
            'page_info': {'total_page': 1}}})

    @staticmethod
    def campaigns():
        return [{'campaign_id': '32357452', 'campaign_name': 'GameA Launch',
                 'campaign_automation_type': 'SMART_PLUS'},
                {'campaign_id': '99999999', 'campaign_name': 'GameB Teaser',
                 'campaign_automation_type': ''}]

    def test_report_survives_an_empty_ad_id_pull(self, monkeypatch):
        """An empty id list has no ad_id column to merge the report on,
        which raised a KeyError that ended the entire run."""
        api = self.make_api()
        monkeypatch.setattr(api, 'make_request',
                            _FakeRequests([self.report_response()]))
        df = api.request_and_get_data('2026-09-10', '2026-09-11')
        assert len(df) == 1
        assert df[tikapi.TikApi.old_date].tolist() == ['2026-09-10']
        assert 'stat_cost' in df.columns
        assert 'ad_name' not in df.columns

    def test_get_data_survives_an_empty_ad_id_pull(self, monkeypatch):
        """One api raising takes every other vendor's data down with it,
        so no campaigns to pull ids for has to stay a warning."""
        api = self.make_api('GameA')
        monkeypatch.setattr(api, 'check_url', lambda: [])
        monkeypatch.setattr(api, 'make_request',
                            _FakeRequests([self.report_response()]))
        assert len(api.get_data()) == 1

    def test_ad_ids_merge_on_when_they_pulled(self):
        """A duplicated id must not fan the report's row out either."""
        api = self.make_api()
        api.ad_id_list = [{'ad_id': '1', 'ad_name': 'Video4.mp4_Real Name'},
                          {'ad_id': '1', 'ad_name': 'Video4.mp4_Real Name'}]
        df = api.merge_ad_ids(pd.DataFrame({'ad_id': ['1', '2'],
                                            'spend': [1, 2]}))
        assert len(df) == 2
        assert df['ad_name'][0] == 'Video4.mp4_Real Name'
        assert df['ad_name'].isna().tolist() == [False, True]

    def test_campaign_filter_matches_an_id(self, monkeypatch):
        """A filter is as often a campaign id as a campaign name, and an
        id matched against the name alone found no campaigns at all."""
        api = self.make_api('32357452')
        monkeypatch.setattr(api, 'get_campaign_list', self.campaigns)
        assert api.check_url() == [{'ad_url': tikapi.TikApi.smart_url,
                                    'campaign_id': '32357452'}]

    def test_campaign_filter_matches_a_name(self, monkeypatch):
        api = self.make_api('GameB')
        monkeypatch.setattr(api, 'get_campaign_list', self.campaigns)
        assert api.check_url() == [{'ad_url': tikapi.TikApi.ad_url,
                                    'campaign_id': '99999999'}]

    def test_campaign_filter_no_match_keeps_every_campaign(self, monkeypatch):
        """A stale filter costs the report its filter, not its ad names."""
        api = self.make_api('NoSuchCampaign')
        monkeypatch.setattr(api, 'get_campaign_list', self.campaigns)
        assert len(api.check_url()) == 2

    def test_no_campaign_list_pulls_ad_ids_blind(self, monkeypatch):
        """A campaign list the token cannot see must not mean no ad ids."""
        api = self.make_api()
        monkeypatch.setattr(api, 'get_campaign_list', lambda: [])
        assert api.check_url() == [{'ad_url': tikapi.TikApi.ad_url,
                                    'campaign_id': None}]

    def test_report_filter_matches_an_id(self):
        """The report filter has to agree with the campaign list filter,
        or the right ads pull and then every row of them is dropped."""
        api = self.make_api('32357452')
        df = pd.DataFrame({'campaign_id': ['32357452', '99999999'],
                           'campaign_name': ['GameA Launch', 'GameB Teaser'],
                           'stat_cost': [1, 2]})
        assert api.filter_df_on_campaign(df)['stat_cost'].tolist() == [1]

    def test_request_id_error_response_is_not_fatal(self, monkeypatch):
        """An errored id request carries a message and no data key."""
        api = self.make_api()
        monkeypatch.setattr(api, 'make_request', _FakeRequests(
            [_FakeResponse(200, json_data={'code': 40001, 'message': 'no'})]))
        ids, r = api.request_id('http://u', {}, [])
        assert ids == []
        assert api.ad_id_list == []


def test_slides_sparse_table_height_and_caption(monkeypatch):
    """Sparse tables stay compact; dense tables continue onto more
    slides before reaching the footer, each inside the page."""
    api = gsapi.GsApi.__new__(gsapi.GsApi)
    captured = []
    monkeypatch.setattr(api, 'slides_batch_update',
                        lambda pid, reqs: captured.extend(reqs))
    api.add_table_slide('p', 'compact', 'Summary', ['Metric', 'Value'],
                        [['Cost', '$10']], caption='Evidence')
    table = next(req['createTable'] for req in captured
                 if 'createTable' in req)
    height = table['elementProperties']['size']['height']['magnitude']
    assert height == 720000
    caption = next(req['createShape'] for req in captured
                   if req.get('createShape', {}).get('objectId') == 'compactc')
    assert caption['elementProperties']['transform']['translateY'] == (
        api.CONTENT_TOP_EMU + height + 80000)
    batches = []
    monkeypatch.setattr(api, 'slides_batch_update',
                        lambda pid, reqs: batches.append(reqs))
    api.add_table_slide('p', 'dense', 'Summary', ['Metric', 'Value'],
                        [['Cost', '$10']] * 30, caption='Evidence')
    tables = [req['createTable'] for batch in batches for req in batch
              if 'createTable' in req]
    assert len(tables) == len(batches) > 1
    assert sum(table['rows'] - 1 for table in tables) == 30
    assert all(table['elementProperties']['size']['height']['magnitude']
               <= api.PAGE_H_EMU - api.CONTENT_TOP_EMU - 800000
               for table in tables)
    texts = [req['insertText']['text'] for batch in batches for req in batch
             if 'insertText' in req]
    assert texts.count('Summary (continued)') == len(batches) - 1
    assert texts.count('Evidence') == len(batches)
