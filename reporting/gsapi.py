import os
import re
import sys
import json
import logging
import time
import collections
import textwrap
import pandas as pd
import reporting.awss3 as awss3
import reporting.utils as utl
import reporting.vmcolumns as vmc
from requests_oauthlib import OAuth2Session
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

config_path = utl.config_path

QUOTA_RETRIES = 6
QUOTA_BACKOFF_S = 2


def quota_retry_adapter(retries=QUOTA_RETRIES, backoff=QUOTA_BACKOFF_S):
    """An adapter that waits out Google's per-minute request quota.

    Every Slides write is its own batchUpdate, so a long deck runs past
    the per-user writes-per-minute limit and each call after that comes
    back 429. A 429 means Google applied nothing, so even a POST is safe
    to send again; nothing else is retried, because a write that failed
    any other way may have landed. The resends wait 0, 4, 8, 16, 32 and
    64 seconds (or Google's ``Retry-After``), past a minute by the
    fifth, so the quota window turns over before the call gives up.
    The last 429 is returned rather than raised, for
    :meth:`GsApi.request_applied` to log as a rejection.

    :param retries: resends after the first 429.
    :param backoff: urllib3's ``backoff_factor``.
    :returns: an ``HTTPAdapter`` to mount on the authed session.
    """
    retry = Retry(total=retries, connect=0, read=0, status_forcelist=(429,),
                  allowed_methods=None, backoff_factor=backoff,
                  raise_on_status=False)
    return HTTPAdapter(max_retries=retry)


EMU_PER_PT = 12700
TEXT_INSET_EMU = 100000
LINE_LEADING_PT = 3
GLYPH_WIDTH_RATIO = .6
PROSE_GLYPH_RATIO = .52
TILE_PAD_EMU = 100000
TILE_HERO_PT = 60
TILE_HERO_VALUE_H_EMU = 914400
TILE_MIN_H_EMU = 1370000
TILE_TEXT_PT = (12, 10, 9)
TILE_VALUE_BIG_PT = 40


def _chars_per_line(width, font_pt, glyph=GLYPH_WIDTH_RATIO):
    """How many glyphs of ``font_pt`` fit one line of a ``width`` box, by
    average advance width, since Slides reports no metrics; mixed-case
    prose measures at :data:`PROSE_GLYPH_RATIO`."""
    return max(1, int((width - TEXT_INSET_EMU)
                      / (font_pt * EMU_PER_PT * glyph)))


def _floored_shares(shares, mins):
    """``shares`` rescaled to sum to one, each at least its ``mins``
    entry, or the floors alone rescaled when they overflow."""
    total_min = sum(mins)
    if total_min >= 1:
        return [floor / total_min for floor in mins]
    fixed = set()
    while True:
        free = [i for i in range(len(shares)) if i not in fixed]
        left = 1 - sum(mins[i] for i in fixed)
        weight = sum(shares[i] for i in free)
        out = list(mins)
        for i in free:
            out[i] = shares[i] / weight * left if weight else left / len(free)
        under = {i for i in free if out[i] < mins[i]}
        if not under:
            return out
        fixed |= under


def _wrapped_lines(text, width, font_pt, glyph=GLYPH_WIDTH_RATIO):
    """The lines ``text`` wraps to in a ``width`` box at ``font_pt``."""
    chars = _chars_per_line(width, font_pt, glyph)
    return sum(max(1, len(textwrap.wrap(line, chars)))
               for line in str(text).split('\n'))


def _clip_lines(text, width, font_pt, max_lines, glyph=GLYPH_WIDTH_RATIO):
    """``text`` cut on a word to ``max_lines`` lines of the box, with an
    ellipsis."""
    text = ' '.join(str(text or '').split())
    chars = _chars_per_line(width, font_pt, glyph)
    if _wrapped_lines(text, width, font_pt, glyph) <= max_lines:
        return text
    budget = chars * max_lines - 1
    cut = text[:budget]
    space = cut.rfind(' ')
    if space > budget // 2:
        cut = cut[:space]
    return f'{cut.rstrip()}…'


def _clip_block(text, width, font_pt, max_lines, glyph=GLYPH_WIDTH_RATIO):
    """``text`` cut to the first ``max_lines`` wrapped lines of a
    ``width`` box, paragraph breaks kept and an ellipsis where it was
    cut; the cut line backs off a word at a time until the wrap fits."""
    if max_lines < 1:
        return ''
    kept, used = [], 0
    for paragraph in str(text or '').split('\n'):
        lines = _wrapped_lines(paragraph, width, font_pt, glyph)
        if used + lines <= max_lines:
            kept.append(paragraph)
            used += lines
            continue
        room = max_lines - used
        if room > 0:
            piece = _clip_lines(paragraph, width, font_pt, room, glyph)
            while (_wrapped_lines(piece, width, font_pt, glyph) > room
                   and ' ' in piece):
                piece = f"{piece[:piece.rstrip('…').rfind(' ')].rstrip()}…"
            kept.append(piece)
        elif kept:
            kept[-1] = f'{kept[-1].rstrip(".…")}…'
        break
    return '\n'.join(kept)


def _clamped_ranges(ranges, length):
    """``ranges`` cut to a text of UTF-16 ``length``, empties dropped."""
    out = []
    for start, end in ranges or []:
        end = min(end, length)
        if end > start:
            out.append((start, end))
    return out


FitResult = collections.namedtuple('FitResult', 'pt lines height fits')


def _line_pitch(font_pt, spacing=100):
    """EMU one wrapped line takes at a ``lineSpacing`` percentage."""
    return (font_pt + LINE_LEADING_PT) * spacing / 100 * EMU_PER_PT


def _block_height(text, width, font_pt, spacing=100, space_below_pt=0,
                  glyph=GLYPH_WIDTH_RATIO):
    """EMU a text block needs, with each paragraph's ``spaceBelow`` and
    Slides' insets; it errs tall, since an overlap reads worse than
    slack."""
    if not text:
        return 0
    lines = _wrapped_lines(text, width, font_pt, glyph)
    breaks = str(text).count('\n')
    return int(lines * _line_pitch(font_pt, spacing)
               + breaks * space_below_pt * EMU_PER_PT + TEXT_INSET_EMU)


def _fit_text(text, width, height, sizes, max_lines=None, spacing=100,
              space_below_pt=0, glyph=GLYPH_WIDTH_RATIO):
    """The largest step of ``sizes`` whose wrap fits ``height`` and
    ``max_lines``, else the floor with ``fits`` False."""
    for font_pt in sizes:
        lines = _wrapped_lines(text, width, font_pt, glyph)
        needed = _block_height(text, width, font_pt, spacing,
                               space_below_pt, glyph)
        fits = needed <= height and (max_lines is None or lines <= max_lines)
        if fits:
            break
    return FitResult(font_pt, lines, needed, fits)


def _ladder_pt(text, width, sizes, max_lines, height=None):
    """The largest step of ``sizes`` keeping ``text`` to ``max_lines``
    lines of bare line pitch standing in ``height``, else the floor."""
    for font_pt in sizes:
        lines = _wrapped_lines(text, width, font_pt)
        if lines > max_lines:
            continue
        if height is None or lines * _line_pitch(font_pt) <= height:
            return font_pt
    return sizes[-1]


def _values_fit(tiles, width, font_pt):
    """Whether every tile value sets on one line at ``font_pt``."""
    chars = _chars_per_line(width, font_pt)
    return all(len(str(tile.get('value') or '')) <= chars for tile in tiles)


def _value_box_h(font_pt):
    """EMU a one-line tile value takes; display sizes sit tighter."""
    factor = 1.2 if font_pt > TILE_VALUE_BIG_PT else 1.36
    return int(font_pt * factor * EMU_PER_PT)


def _lines_fit(height, font_pt, spacing=100):
    """How many lines at ``font_pt`` stand in a ``height`` box after its
    insets."""
    return int((height - TEXT_INSET_EMU) // _line_pitch(font_pt, spacing))


def _clip_box(text, width, height, font_pt, spacing=100):
    """``text`` cut to the prose lines a ``width`` by ``height`` box holds."""
    return _clip_block(text, width, font_pt,
                       _lines_fit(height, font_pt, spacing), PROSE_GLYPH_RATIO)


def _leading_fit(items, sizes, budget):
    """``(kept, used)``: the leading ``items`` whose ``sizes`` sum within
    ``budget``, always at least one, and what they take of it."""
    kept, used = [], 0
    for item, size in zip(items, sizes):
        if kept and used + size > budget:
            break
        kept.append(item)
        used += size
    return kept, used


def _fact_pairs(facts):
    """``[(label, [line, ...])]`` from a parameters slide's ``facts``,
    without blank labels and lines."""
    pairs = [(str(fact.get('label') or '').strip(),
              [str(line) for line in fact.get('lines') or []
               if str(line or '').strip()]) for fact in facts or []]
    return [pair for pair in pairs if any(pair)]


def _clip_pairs(pairs, width, font_pt, max_lines):
    """The leading facts of ``pairs``, lines cut, within ``max_lines`` of
    a ``width`` box, counting each label and the gap before it a line."""
    kept, used = [], 0
    for label, lines in pairs:
        need = (1 if kept else 0) + (1 if label else 0)
        if kept and used + need >= max_lines:
            break
        used += need
        fit_lines, taken = _leading_fit(
            lines, [_wrapped_lines(line, width, font_pt) for line in lines],
            max_lines - used)
        used += taken
        kept.append((label, fit_lines))
        if used >= max_lines:
            break
    return kept


def _luminance(rgb):
    """WCAG relative luminance of a 0-1 ``{'red', 'green', 'blue'}``."""
    red, green, blue = (v / 12.92 if v <= 0.03928
                        else ((v + 0.055) / 1.055) ** 2.4
                        for v in (rgb['red'], rgb['green'], rgb['blue']))
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue


def _contrast(fore, back):
    """WCAG contrast ratio between two 0-1 colours."""
    high, low = sorted((_luminance(fore), _luminance(back)), reverse=True)
    return (high + 0.05) / (low + 0.05)


def _composite(fill, alpha, under):
    """``fill`` at ``alpha`` laid over ``under``, the colour a reader
    sees."""
    if alpha is None:
        return dict(fill)
    return {key: alpha * fill[key] + (1 - alpha) * under[key]
            for key in ('red', 'green', 'blue')}


def _solid_fill(color, alpha, field):
    """``(solidFill, fields)`` filling ``field`` with ``color``; alpha is
    only sent when set, since the API rejects it on an opaque fill."""
    if alpha is None:
        return {'color': {'rgbColor': color}}, f'{field}.solidFill.color'
    return ({'color': {'rgbColor': color}, 'alpha': alpha},
            f'{field}.solidFill.color,{field}.solidFill.alpha')


TABLE_CELL_PAD_EMU = 91440
TABLE_LINE_RATIO = 1.25
TABLE_ROW_MIN_EMU = 340000


def _table_row_height(row, widths, font_pt, row_min=TABLE_ROW_MIN_EMU):
    """Reserve native cell wrapping plus Slides' fixed 0.1in cell insets
    for a row whose cells sit in columns of ``widths``."""
    lines = max((_wrapped_lines(value, width, font_pt)
                 for value, width in zip(row, widths) if value), default=1)
    return max(row_min,
               int(lines * font_pt * TABLE_LINE_RATIO * EMU_PER_PT
                   + 2 * TABLE_CELL_PAD_EMU))


def _table_heights(header, rows, widths, head_pt, body_pt,
                   row_min=TABLE_ROW_MIN_EMU):
    """``(header height, [row heights])`` of a table in columns of
    ``widths``."""
    return (_table_row_height(header, widths, head_pt, row_min),
            [_table_row_height(row, widths, body_pt, row_min)
             for row in rows])


def _clip_rows(rows, widths, font_pt, row_h):
    """``rows`` with every cell cut on a word to the lines a ``row_h``
    table row holds."""
    lines = max(1, int((row_h - 2 * TABLE_CELL_PAD_EMU)
                       // (font_pt * TABLE_LINE_RATIO * EMU_PER_PT)))
    return [[_clip_lines(cell, width, font_pt, lines)
             for cell, width in zip(row, widths)] for row in rows]


TABLE_WIDTH_CAP_CHARS = 40
TABLE_COL_MIN_EMU = 406400


def _longest_word_emu(cells, font_pt):
    """EMU the longest word in any of ``cells`` needs at ``font_pt``,
    insets included, so a column that wide never breaks it mid-word."""
    longest = max((len(word) for cell in cells
                   for word in str(cell or '').split()), default=0)
    return (longest * font_pt * EMU_PER_PT * GLYPH_WIDTH_RATIO
            + TEXT_INSET_EMU)


def _weighted_widths(width, weights, mins):
    """``width`` split by ``weights``, each part at least its ``mins``
    share, the rounding left on the last so the parts sum exactly."""
    total = sum(weights)
    shares = [weight / total if total else 1 / len(weights)
              for weight in weights]
    widths = [int(width * share) for share in _floored_shares(shares, mins)]
    widths[-1] += width - sum(widths)
    return widths


def _table_widths(header, rows, width, head_pt, body_pt):
    """EMU per column: each at least its longest word and the API's 32pt
    floor, the rest by longest cell; equal when the floors overflow."""
    if not header:
        return []
    columns = [[head] + [row[c] for row in rows if c < len(row)]
               for c, head in enumerate(header)]
    mins = [max(TABLE_COL_MIN_EMU, _longest_word_emu([head], head_pt),
                _longest_word_emu(cells, body_pt)) / width
            for head, *cells in columns]
    if sum(mins) >= 1:
        return [width / len(header)] * len(header)
    weights = [min(max(len(str(cell or '')) for cell in column),
                   TABLE_WIDTH_CAP_CHARS) for column in columns]
    return _weighted_widths(width, weights, mins)


def _tile_block_heights(tile, width, sizes=TILE_TEXT_PT):
    """``(label, caption, note)`` heights for one stat tile's text — read
    once to size the grid and again to place each block, so the two
    cannot drift apart and overlap."""
    return tuple(_block_height(tile.get(key), width, font_pt)
                 for key, font_pt in zip(('label', 'caption', 'note'),
                                         sizes))


def _fit_rows(rows, heights, budget):
    """``rows`` split into slide-sized groups, in order. A row taller
    than ``budget`` gets a group of its own rather than no group: it
    still has to print somewhere."""
    groups, group, used = [], [], 0
    for row, height in zip(rows, heights):
        if group and used + height > budget:
            groups.append(group)
            group, used = [], 0
        group.append(row)
        used += height
    if group:
        groups.append(group)
    return groups


TableCell = collections.namedtuple(
    'TableCell', 'start end para_ranges paragraphs')


class GsApi(object):
    api_field_options = (
        ('Doc', 'Read a Google Doc rather than a sheet'),)
    sheets_url = 'https://sheets.googleapis.com/v4/spreadsheets'
    slides_url = 'https://slides.googleapis.com/v1/presentations'
    files_url = 'https://www.googleapis.com/drive/v3/files'
    drive_url = 'https://www.googleapis.com/drive/v3/drives'
    docs_url = 'https://docs.googleapis.com/v1/documents'
    body_str = 'body'
    cont_str = 'content'
    para_str = 'paragraph'
    head_str = 'header'
    img_str = 'image'
    doc_str = 'Doc'
    text_format = 'NORMAL_TEXT'
    screenshot_dir = os.path.join('screenshots', 'charts/')
    default_config_file_name = 'gsapi.json'
    default_config = 'gsapi_screenshots.json'
    required_scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/presentations',
        'https://www.googleapis.com/auth/documents',
    ]

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.sheet_id = None
        self.config_list = None
        self.warn_config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None
        self.google_doc = False
        self.on_token_refresh = None
        self.deck_layouts = {}
        self.deck_fonts = {}
        self.parse_response = self.parse_sheets_response

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading GS config file:{}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        return self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.load_config_dict(self.config)

    def load_config_dict(self, config):
        """Populate credentials from an in-memory dict, bypassing the
        CWD-relative config file load (used by the app-layer vault).

        ``sheet_id`` may be absent or blank: a pooled credential has
        none, and most callers set the sheet after loading or create
        their own, so only a card that reads its own sheet needs it.

        :param config: dict in gsapi.json shape
        """
        self.config = config
        self.client_id = config.get('client_id', '')
        self.client_secret = config.get('client_secret', '')
        self.access_token = config.get('access_token', '')
        self.refresh_token = config.get('refresh_token', '')
        self.refresh_url = config.get('refresh_url', '')
        self.sheet_id = config.get('sheet_id', '')
        self.config_list = [('client_id', self.client_id),
                            ('client_secret', self.client_secret),
                            ('refresh_token', self.refresh_token),
                            ('refresh_url', self.refresh_url)]
        self.warn_config_list = [('sheet_id', self.sheet_id)]

    def check_config(self):
        utl.check_blank_config(
            self.warn_config_list, self.config_file, 'Google Sheets',
            'The card reads no sheet or doc until it is filled in.')
        return utl.check_blank_config(
            self.config_list, self.config_file, 'Google Sheets')

    def parse_fields(self, fields):
        if fields:
            for field in fields:
                if field == self.doc_str:
                    self.google_doc = True
                    self.parse_response = self.parse_google_doc

    def get_client(self):
        """Refresh the access token and keep an authed session on
        ``self.client`` that resends quota-limited calls
        (:func:`quota_retry_adapter`)."""
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.client.refresh_token(self.refresh_url, **extra)
        self.client = OAuth2Session(self.client_id, token=token)
        self.client.mount('https://', quota_retry_adapter())
        if self.on_token_refresh:
            self.on_token_refresh(token)

    def create_url(self):
        if self.google_doc:
            url = '{}/{}'.format(self.docs_url, self.sheet_id)
        else:
            url = '{}/{}/values/A:ZZ'.format(self.sheets_url, self.sheet_id)
        return url

    def parse_sheets_response(self, response):
        if 'values' in response:
            self.df = pd.DataFrame(response['values'])
            logging.info('Data received, returning dataframe.')
        else:
            logging.warning('Values not in response: {}'.format(response))
            self.df = pd.DataFrame()
        return self.df

    def get_data(self, sd=None, ed=None, fields=None):
        logging.info('Getting df from sheet: {}'.format(self.sheet_id))
        self.parse_fields(fields)
        self.get_client()
        url = self.create_url()
        r = self.client.get(url)
        try:
            response = r.json()
        except json.JSONDecodeError as e:
            logging.warning('Error parsing JSON: {}'.format(e))
            response = ''
        if response:
            self.df = self.parse_response(response)
        return self.df

    def get_simple_df(self, sheet_id=''):
        self.input_config('gsapi.json')
        self.sheet_id = sheet_id
        df = self.get_data()
        return df

    def create_spreadsheet(self, title=None):
        """Create a new Google Spreadsheet via the Sheets API.
        Returns the new spreadsheet id; caller is responsible for
        sharing via `add_permissions`."""
        logging.info('Creating GSheet: {}'.format(title))
        body = {'properties': {'title': title or 'Untitled spreadsheet'}}
        response = self.client.post(url=self.sheets_url, json=body)
        response = response.json()
        spreadsheet_id = response.get('spreadsheetId')
        if spreadsheet_id:
            self.sheet_id = spreadsheet_id
        return spreadsheet_id

    blank_write_msg = ('Google rejected the {} contents, so the sheet would '
                       'have been empty. Nothing was exported.')

    @staticmethod
    def request_applied(response, context=''):
        """Whether a Google write actually applied, logging any rejection.

        Google applies a batch atomically, so one bad request can leave
        the artifact completely empty while the call still returns a
        usable file id. **Never treat a returned response as success** —
        that is how a blank Doc shipped with a working URL.

        :param response: the requests response; ``None`` counts as a
            no-op success so empty batches and test doubles stay valid.
        :param context: file id / range, for the log line.
        :returns: whether the write applied.
        """
        if response is None:
            return True
        status = getattr(response, 'status_code', 200)
        if status in (200, 204):
            return True
        logging.error('Google write rejected ({}) for {}: {}'.format(
            status, context, str(getattr(response, 'text', ''))[:500]))
        return False

    def write_values(self, spreadsheet_id, range_, values):
        """Write a 2D list of values into the given A1 range. Uses
        USER_ENTERED so formula-like cells render naturally. Returns the
        response; a rejection is logged here."""
        url = '{}/{}/values/{}'.format(
            self.sheets_url, spreadsheet_id, range_)
        params = {'valueInputOption': 'USER_ENTERED'}
        body = {'values': values}
        response = self.client.put(url=url, params=params, json=body)
        self.request_applied(response,
                             '{}!{}'.format(spreadsheet_id, range_))
        return response

    def write_values_applied(self, spreadsheet_id, range_, values):
        """:meth:`write_values`, reporting whether Google applied it.
        Callers that would otherwise hand back a URL to an empty sheet
        pair this with :attr:`blank_write_msg`."""
        return self.request_applied(
            self.write_values(spreadsheet_id, range_, values),
            spreadsheet_id)

    def batch_update(self, spreadsheet_id, requests):
        """POST a list of Sheets requests (repeatCell, mergeCells,
        updateBorders, ...) to spreadsheets.batchUpdate."""
        if not requests:
            return None
        url = f'{self.sheets_url}/{spreadsheet_id}:batchUpdate'
        return self.client.post(url=url, json={'requests': requests})

    def clear_values(self, spreadsheet_id, range_='A:ZZ'):
        """Clear an A1 range's values (formatting stays). Returns the
        response; a rejection is logged via `request_applied`."""
        url = f'{self.sheets_url}/{spreadsheet_id}/values/{range_}:clear'
        response = self.client.post(url=url, json={})
        self.request_applied(response, f'{spreadsheet_id} clear {range_}')
        return response

    @staticmethod
    def spreadsheet_url(spreadsheet_id):
        return 'https://docs.google.com/spreadsheets/d/{}/edit'.format(
            spreadsheet_id)

    def create_presentation(self, presentation_name=None, template_id=None,
                            fonts=None):
        """Create a deck from a template, falling back to blank if copying
        or layout detection fails; ``fonts`` overrides the kit's faces."""
        logging.info('Creating GSlides Presentation: {}'.format(
            presentation_name))
        self.deck_layouts = {}
        self.deck_fonts = dict(fonts or {})
        if template_id:
            presentation_id = self._copy_template(template_id,
                                                  presentation_name)
            if presentation_id:
                self.add_permissions(presentation_id)
                return presentation_id
        body = {
            "title": presentation_name,
        }
        response = self.client.post(url=self.slides_url, json=body)
        response = response.json()
        presentation_id = response["presentationId"]
        self.add_permissions(presentation_id)
        return presentation_id

    def _copy_template(self, template_id, name):
        """Copy the template into the caller's own Drive and load
        recognized layouts; return None if unusable. Without a parent
        the copy stays beside the template, and a template kept on a
        shared drive would hand every deck to that drive's members."""
        response = self.client.post(
            '{}/{}/copy'.format(self.files_url, template_id),
            params={'supportsAllDrives': 'true'},
            json={'name': name, 'parents': ['root']})
        if getattr(response, 'status_code', 0) != 200:
            logging.warning('Deck template %s could not be copied: %s',
                            template_id,
                            str(getattr(response, 'text', ''))[:300])
            return None
        presentation_id = response.json().get('id')
        if not presentation_id:
            return None
        layouts = self._read_layouts(self.get_presentation(presentation_id))
        if 'content' not in layouts:
            logging.warning('Deck template %s has no content layout; '
                            'the deck starts blank', template_id)
            self.client.delete('{}/{}'.format(self.files_url,
                                              presentation_id))
            return None
        self.deck_layouts = layouts
        return presentation_id

    @classmethod
    def _read_layouts(cls, presentation):
        """Find TITLE and CUSTOM_14 layouts; the longer custom name denotes
        content."""
        out, customs = {}, []
        for layout in presentation.get('layouts') or []:
            props = layout.get('layoutProperties') or {}
            name = str(props.get('name') or '')
            if name == 'TITLE' and 'title' not in out:
                out['title'] = layout['objectId']
            elif name.startswith(cls.TEMPLATE_LAYOUT_STEM):
                customs.append((len(name), layout['objectId']))
        customs.sort()
        if customs:
            out['section'] = customs[0][1]
            out['content'] = customs[-1][1]
        return out

    @property
    def is_dark(self):
        """Whether the deck draws on the template's dark layouts."""
        return bool(getattr(self, 'deck_layouts', None))

    @property
    def deck_font(self):
        """The body face: this deck's configured one, else the kit's."""
        return ((getattr(self, 'deck_fonts', None) or {}).get('body')
                or self.DECK_FONT)

    @property
    def eyebrow_font(self):
        """The eyebrow face: this deck's configured one, else the kit's."""
        return ((getattr(self, 'deck_fonts', None) or {}).get('eyebrow')
                or self.DECK_EYEBROW_FONT)

    def _create_permission(self, file_id, body, params=None):
        """POST a single Drive permission and return the response.

        Shared by the domain-wide and per-user share helpers so the
        request + warning-log path lives in one place. Drive answers
        404 for a file on a shared drive unless the call says it
        supports them."""
        url = '{}/{}/permissions'.format(self.files_url, file_id)
        response = self.client.post(
            url=url, params=dict(params or {}, supportsAllDrives='true'),
            json=body)
        if response.status_code not in (200, 204):
            logging.warning(
                'Failed to set permission on {}: {} (Status {})'.format(
                    file_id, response.text, response.status_code))
        return response

    def add_permissions(self, presentation_id,
                        domain="liquidadvertising.com", role="writer"):
        """Share a Drive file with an entire Workspace domain."""
        return self._create_permission(presentation_id, {
            "role": role,
            "type": "domain",
            "domain": domain,
            "allowFileDiscovery": True,
        })

    def add_user_permission(self, file_id, email, role="writer",
                            notify=False):
        """Share a Drive file with a single Google account.

        More reliable than domain sharing: a ``type:"user"`` grant to
        a Workspace member does not depend on the admin's domain-wide
        sharing policy, so the recipient keeps access even when
        whole-domain sharing is refused. ``notify=False`` skips the
        "shared with you" email since the caller usually opens the
        file itself."""
        return self._create_permission(
            file_id,
            {"role": role, "type": "user", "emailAddress": email},
            {"sendNotificationEmail": "true" if notify else "false"})

    def add_image_slide(self, presentation_id=None, ad_id=None,
                        image_url=None):
        logging.info('Creating slide and adding image: {}, {}'.format(
            ad_id, image_url))
        url = self.slides_url + "/" + presentation_id + ":batchUpdate"
        headers = {"Content-Type": "application/json"}
        body = {
            "requests": [
                {
                    "createSlide": {
                        "objectId": ad_id
                    }
                },
                {
                    "createImage": {
                        "elementProperties": {
                            "pageObjectId": ad_id
                        },
                        "url": image_url
                    }
                }
            ]
        }
        response = self.client.post(url=url, json=body, headers=headers)
        return response

    def add_speaker_notes(self, presentation_id=None, page_id=None, text=''):
        logging.info('Adding speaker note: {}'.format(text))
        url = self.slides_url + "/" + presentation_id + "/pages/" + page_id
        notes_id = None
        for x in range(1, 11):
            response = self.client.get(url)
            response = response.json()
            if "slideProperties" in response:
                notes_id = response["slideProperties"][
                    "notesPage"]["notesProperties"]["speakerNotesObjectId"]
                break
            else:
                logging.warning('Slide not created yet. Attempt: {}'.format(x))
                time.sleep(3)
        if not notes_id:
            return {'error': 'Unable to add speaker notes to slide '
                             '{}.'.format(page_id)}
        url = self.slides_url + "/" + presentation_id + ":batchUpdate"
        headers = {"Content-Type": "application/json"}
        body = {
            "requests": [
                {
                    "insertText": {
                        "objectId": notes_id,
                        "insertionIndex": 0,
                        "text": text
                    }
                }
            ]
        }
        response = self.client.post(url=url, json=body, headers=headers)
        return response

    PAGE_W_EMU = 9144000
    PAGE_H_EMU = 5143500
    MARGIN_EMU = 457200
    DECK_FONT = 'Proxima Nova'
    DECK_EYEBROW_FONT = 'Work Sans'
    DECK_INK = {'red': 0.173, 'green': 0.243, 'blue': 0.314}
    DECK_MUTED = {'red': 0.533, 'green': 0.533, 'blue': 0.533}
    DECK_ACCENT = {'red': 0.231, 'green': 0.510, 'blue': 0.965}
    DECK_CARD = {'red': 0.957, 'green': 0.969, 'blue': 0.980}
    DECK_WHITE = {'red': 1.0, 'green': 1.0, 'blue': 1.0}
    DECK_RULE = {'red': 0.878, 'green': 0.906, 'blue': 0.933}
    DECK_MUTED_LIGHT = {'red': 0.722, 'green': 0.780, 'blue': 0.839}
    DECK_GOOD = {'red': 0.082, 'green': 0.502, 'blue': 0.239}
    DECK_BAD = {'red': 0.863, 'green': 0.149, 'blue': 0.149}
    DECK_ACCENT_DARK = {'red': 0.376, 'green': 0.647, 'blue': 0.980}
    DECK_GOOD_DARK = {'red': 0.290, 'green': 0.871, 'blue': 0.502}
    DECK_BAD_DARK = {'red': 0.973, 'green': 0.443, 'blue': 0.443}
    DECK_CARD_ALPHA_DARK = 0.10
    DECK_RULE_DARK = {'red': 0.290, 'green': 0.361, 'blue': 0.510}
    CONTENT_TOP_EMU = 1000000
    CONTENT_TOP_DARK_EMU = 1180000
    EYEBROW_Y_EMU = 100000
    EYEBROW_H_EMU = 200000
    HEADING_DARK_EMU = (300000, 560000, 820000, 320000)
    HEADING_LIGHT_EMU = (160000, 460000, 840000, 380000)
    TEMPLATE_LAYOUT_STEM = 'CUSTOM_14'
    TITLE_BLOCK_Y_EMU = 2260000
    TITLE_BLOCK_W_EMU = 5900000
    TITLE_BLOCK_H_EMU = 900000
    COVER_TITLE_PT = (36, 32, 28)
    TABLE_FILL_SHARE = 0.92
    SECTION_X_EMU = 1600200
    CAPTION_H_EMU = 340000
    CAPTION_PT = 12
    CAPTION_GAP_EMU = 80000
    CAPTION_FOOT_EMU = 380000
    BODY_FOOT_EMU = 420000
    SOURCE_PT = 7.5
    SOURCE_H_EMU = 230000
    HEADLINE_LINES = 2
    HEADLINE_PT_DARK = (36, 32, 28, 24, 22)
    HEADLINE_PT_LIGHT = (32, 28, 24, 22, 20)
    STATEMENT_PT = (24, 22, 20, 18)
    SUBHEAD_PT_DARK = (14, 13, 12)
    SUBHEAD_PT_LIGHT = (13, 12, 11)
    NARRATIVE_PT = (16, 15, 14)
    NARRATIVE_SPACING = 115
    NARRATIVE_SPACE_BELOW_PT = 4
    RAIL_PT = (12, 11, 10)
    STAT_PT = (14, 12, 11)
    CARD_HEAD_PT = (13, 12, 11)
    CARD_BODY_PT = (12, 11, 10)
    CARD_STAT_PT = (24, 20, 18)
    TILE_VALUE_PT_LADDER = (56, 48, 40, 32, 26, 22, 18)
    TOC_PT = (14, 12)
    TABLE_HEAD_PT = 12
    TABLE_BODY_PT = 11
    TABLE_SHORT_ROWS = 6
    TABLE_SHORT_SHARE = 0.55
    TABLE_SHORT_HEAD_PT = 14
    TABLE_SHORT_BODY_PT = 13
    TABLE_SHORT_ROW_MIN_EMU = 457200

    def _note_fit(self, slide_id, slot, fit):
        """Record a block that does not fit at the ladder's floor on
        ``fit_warnings``, so the export can log the slide."""
        if fit.fits:
            return
        self.fit_warnings = getattr(self, 'fit_warnings', [])
        self.fit_warnings.append((slide_id, slot, fit.lines, fit.pt))

    @classmethod
    def body_box(cls, dark, stats=False):
        """``(x, y, w, h)`` EMU of a content slide's body, above the stat
        bar's band when ``stats``."""
        top = cls._content_top(dark)
        band = cls.STORY_STAT_H_EMU + cls.STORY_GAP_EMU if stats else 0
        return (cls.MARGIN_EMU, top, cls.PAGE_W_EMU - 2 * cls.MARGIN_EMU,
                cls.PAGE_H_EMU - top - cls.BODY_FOOT_EMU - band)

    @classmethod
    def fit_body(cls, text, dark, stats=False):
        """How a narrative sets in the body box, before the deck exists."""
        _, _, width, height = cls.body_box(dark, stats)
        return cls._fit_prose(text, width, height, cls.NARRATIVE_PT)

    @classmethod
    def fit_rail(cls, text, dark, stats=False):
        """How a narrative sets in the rail beside a story chart."""
        _, _, chart_w, _ = cls.story_chart_box(dark, True, stats)
        _, _, width, height = cls.body_box(dark)
        return cls._fit_prose(text, width - chart_w - cls.STORY_GAP_EMU,
                              height, cls.RAIL_PT)

    @classmethod
    def _fit_prose(cls, text, width, height, sizes):
        """How body copy sets on the ``sizes`` ladder at the leading
        :meth:`_prose_spacing_req` draws."""
        return _fit_text(text, width, height, sizes,
                         spacing=cls.NARRATIVE_SPACING,
                         space_below_pt=cls.NARRATIVE_SPACE_BELOW_PT,
                         glyph=PROSE_GLYPH_RATIO)

    @classmethod
    def fit_caption(cls, text, caption_h=None):
        """How a caption sets under a chart image."""
        return _fit_text(text, cls.PAGE_W_EMU - 2 * cls.MARGIN_EMU,
                         caption_h or cls.CAPTION_H_EMU, (cls.CAPTION_PT,),
                         glyph=PROSE_GLYPH_RATIO)

    @classmethod
    def table_row_groups(cls, header, rows, dark, caption=None,
                         narrative=False, stats=False, widths=None):
        """``rows`` split into the groups :meth:`add_table_slide` draws a
        slide each, laid out on ``widths`` when a continuation keeps them;
        rows take :data:`TABLE_FILL_SHARE` of the budget, since Slides
        grows a wrapped cell past its height."""
        _, _, width, body_h = cls.story_chart_box(dark, bool(narrative),
                                                  bool(stats))
        widths = widths or _table_widths(header, rows, width,
                                         cls.TABLE_HEAD_PT, cls.TABLE_BODY_PT)
        head_h, heights = _table_heights(header, rows, widths,
                                         cls.TABLE_HEAD_PT, cls.TABLE_BODY_PT)
        budget = (body_h - cls._caption_reserve(caption, width, body_h)
                  - head_h)
        return _fit_rows(rows, heights, int(budget * cls.TABLE_FILL_SHARE))

    @classmethod
    def _caption_reserve(cls, caption, width, body_h):
        """The height a table caption and its gap take from the rows: its
        wrapped height, at least the kit's box, at most half the body."""
        if not caption:
            return 0
        return cls.CAPTION_GAP_EMU + min(
            max(cls.CAPTION_H_EMU, _block_height(
                caption, width, cls.CAPTION_PT, glyph=PROSE_GLYPH_RATIO)),
            body_h // 2)

    def _brand_colors(self, brand):
        """Use the client palette on light slides and light ink with its
        accent on dark slides."""
        brand = brand or {}
        if self.is_dark:
            return self._art_colors(brand)
        return {'accent': brand.get('accent') or self.DECK_ACCENT,
                'ink': brand.get('ink') or self.DECK_INK,
                'muted': brand.get('muted') or self.DECK_MUTED,
                'card': brand.get('card') or self.DECK_CARD}

    def _art_colors(self, brand):
        """The palette for anything drawn over the ink field, whatever
        the deck's theme."""
        brand = brand or {}
        return {'accent': brand.get('accent') or self.DECK_ACCENT_DARK,
                'ink': self.DECK_WHITE,
                'muted': self.DECK_MUTED_LIGHT,
                'card': self.DECK_WHITE,
                'card_alpha': self.DECK_CARD_ALPHA_DARK}

    CONTINUED_SUFFIX = ' (continued)'

    def _continued(self, title):
        """``title`` marked as a continuation once, never twice."""
        title = str(title or '')
        return title if title.endswith(self.CONTINUED_SUFFIX) \
            else f'{title}{self.CONTINUED_SUFFIX}'

    def _part(self, slide_id, title, index):
        """``(slide id, title)`` of part ``index`` of a continued slide."""
        if not index:
            return slide_id, title
        return f'{slide_id}part{index}', self._continued(title)

    @classmethod
    def _content_top(cls, dark):
        """Where a content slide's body starts, below the chrome."""
        return cls.CONTENT_TOP_DARK_EMU if dark else cls.CONTENT_TOP_EMU

    def content_top(self):
        """Where this deck's content slides start, below the chrome."""
        return self._content_top(self.is_dark)

    @classmethod
    def story_chart_box(cls, dark, narrative=True, stats=False,
                        split=False):
        """``(x, y, w, h)`` EMU of a chart-story slide's chart, for sizing
        a capture early; ``split`` centres it between two rails."""
        cx, top, cw, body_h = cls.body_box(dark, stats)
        if not narrative:
            return cx, top, cw, body_h
        share = cls.STORY_SPLIT_SHARE if split else cls.STORY_CHART_SHARE
        chart_w = int(cw * share)
        if split:
            cx += (cw - chart_w) // 2
        return cx, top, chart_w, body_h

    @classmethod
    def chart_image_box(cls, dark, title=True, caption=True,
                        caption_h=None):
        """``(x, y, w, h)`` EMU of the image in a captioned chart slide,
        the box :meth:`add_chart_slide` fits the picture into."""
        cx, cw = cls.MARGIN_EMU, cls.PAGE_W_EMU - 2 * cls.MARGIN_EMU
        top = cls._content_top(dark) if title else 300000
        if not caption:
            return cx, top, cw, cls.PAGE_H_EMU - top - cls.BODY_FOOT_EMU
        bottom = (cls.PAGE_H_EMU - cls.CAPTION_FOOT_EMU - cls.CAPTION_GAP_EMU
                  - (caption_h or cls.CAPTION_H_EMU))
        return cx, top, cw, bottom - top

    def headline_pt(self, title, width=None, lines=HEADLINE_LINES,
                    height=None):
        """The largest step of the headline ladder that keeps ``title``
        to ``lines`` lines standing in ``height``, else the floor."""
        if width is None:
            width = self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        sizes = (self.HEADLINE_PT_DARK if self.is_dark
                 else self.HEADLINE_PT_LIGHT)
        return _ladder_pt(str(title or ''), width, sizes, lines, height)

    def _title_box(self, title, width, lines, one_h, two_h):
        """``(pt, h)`` of a content title: the one-line box when it fits
        one line or sits over a subhead, else the two-line box."""
        one_pt = self.headline_pt(title, width, 1, one_h)
        if lines <= 1 or _wrapped_lines(title, width, one_pt) <= 1:
            return one_pt, one_h
        return self.headline_pt(title, width, lines, two_h), two_h

    def slides_batch_update(self, presentation_id, requests):
        """POST a Slides batchUpdate and record whether it applied; every
        ``createSlide`` id lands on ``drawn_slides``, the count a deck
        budget reads."""
        if not requests:
            return None
        url = '{}/{}:batchUpdate'.format(self.slides_url, presentation_id)
        self.drawn_slides = getattr(self, 'drawn_slides', [])
        self.drawn_slides.extend(
            r['createSlide'].get('objectId') for r in requests
            if isinstance(r, dict) and 'createSlide' in r
            and r['createSlide'].get('objectId'))
        response = self.client.post(url=url, json={'requests': requests})
        if not self.request_applied(response, presentation_id):
            slide_id = next(
                (r['createSlide'].get('objectId') for r in requests
                 if isinstance(r, dict) and 'createSlide' in r), None)
            reason = str(getattr(response, 'text', '') or '')[:300]
            self.slides_rejections = getattr(self, 'slides_rejections', [])
            self.slides_rejections.append((slide_id, reason))
        return response

    def get_presentation(self, presentation_id):
        url = '{}/{}'.format(self.slides_url, presentation_id)
        return self.client.get(url).json()

    @staticmethod
    def presentation_url(presentation_id):
        return 'https://docs.google.com/presentation/d/{}/edit'.format(
            presentation_id)

    @staticmethod
    def _emu(magnitude):
        return {'magnitude': int(magnitude), 'unit': 'EMU'}

    def _elem_props(self, page_id, w, h, x, y):
        return {
            'pageObjectId': page_id,
            'size': {'width': self._emu(w), 'height': self._emu(h)},
            'transform': {'scaleX': 1, 'scaleY': 1, 'translateX': int(x),
                          'translateY': int(y), 'unit': 'EMU'},
        }

    @staticmethod
    def _fit_box(img_w, img_h, box_x, box_y, box_w, box_h):
        """Aspect-preserving centered fit of an image into a content box."""
        if not img_w or not img_h:
            return int(box_x), int(box_y), int(box_w), int(box_h)
        scale = min(box_w / float(img_w), box_h / float(img_h))
        draw_w, draw_h = img_w * scale, img_h * scale
        return (int(box_x + (box_w - draw_w) / 2),
                int(box_y + (box_h - draw_h) / 2),
                int(draw_w), int(draw_h))

    def _blank_slide_req(self, slide_id, kind='content'):
        """Use the requested template layout when available, otherwise a
        blank slide."""
        layout_id = getattr(self, 'deck_layouts', {}).get(kind)
        if layout_id:
            return {'createSlide': {
                'objectId': slide_id,
                'slideLayoutReference': {'layoutId': layout_id}}}
        return {'createSlide': {
            'objectId': slide_id,
            'slideLayoutReference': {'predefinedLayout': 'BLANK'}}}

    def _text_box_reqs(self, slide_id, shape_id, text, x, y, w, h,
                       font_pt=18, bold=False, align='START', color=None,
                       font=None, italic=False):
        """A text box with one style over all its text; an empty box gets
        none, since Slides rejects a style on an empty range."""
        ink = self.DECK_WHITE if self.is_dark else self.DECK_INK
        style = {'fontSize': {'magnitude': font_pt, 'unit': 'PT'},
                 'bold': bold, 'italic': italic,
                 'fontFamily': font or self.deck_font,
                 'foregroundColor': {'opaqueColor': {
                     'rgbColor': color or ink}}}
        reqs = [{'createShape': {
            'objectId': shape_id, 'shapeType': 'TEXT_BOX',
            'elementProperties': self._elem_props(slide_id, w, h, x, y)}}]
        if not text:
            return reqs
        return reqs + [
            {'insertText': {
                'objectId': shape_id, 'insertionIndex': 0,
                'text': str(text)}},
            {'updateTextStyle': {
                'objectId': shape_id, 'style': style,
                'textRange': {'type': 'ALL'},
                'fields': 'fontSize,bold,italic,fontFamily,foregroundColor'}},
            {'updateParagraphStyle': {
                'objectId': shape_id, 'style': {'alignment': align},
                'textRange': {'type': 'ALL'}, 'fields': 'alignment'}},
        ]

    def create_title_slide(self, presentation_id, meta, slide_id='rbtitle'):
        """The dark hero cover (gold-deck convention): full-bleed ink field,
        white title, one date line, brand-accent foot band, and the brand
        mark on a white plate so any logo colorway reads on the dark field."""
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        colors = self._brand_colors(meta.get('deck_brand'))
        if self.is_dark:
            return self._template_title_slide(presentation_id, meta,
                                              slide_id, colors)
        reqs = [self._blank_slide_req(slide_id, 'title')]
        reqs += self._art_field_reqs(slide_id, colors, meta.get('image_url'),
                                     meta.get('img_w'), meta.get('img_h'))
        band_h = 137160
        reqs += self._rect_reqs(slide_id, f'{slide_id}band', 0,
                                self.PAGE_H_EMU - band_h, self.PAGE_W_EMU,
                                band_h, colors['accent'])
        logo_url = meta.get('logo_url')
        if logo_url:
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}logo', logo_url,
                (self.PAGE_W_EMU - self.LOGO_W_EMU) // 2, 520000)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', meta.get('title', ''),
            cx, 1600200, cw, 900000, font_pt=30, bold=True, align='CENTER',
            color=self.DECK_WHITE)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}s', meta.get('subtitle', ''),
            cx, 2600000, cw, 600000, font_pt=16, align='CENTER',
            color=self.DECK_MUTED_LIGHT)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}b', meta.get('brand', ''),
            cx, self.PAGE_H_EMU - 700000, cw, 260000, font_pt=12,
            align='CENTER', color=self.DECK_MUTED_LIGHT)
        if meta.get('legal'):
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}legal', meta['legal'],
                cx, self.PAGE_H_EMU - 420000, cw, 260000, font_pt=9,
                align='CENTER', color=self.DECK_MUTED_LIGHT)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def _template_title_slide(self, presentation_id, meta, slide_id,
                              colors):
        """The template cover: title (or ``game_logo_url``) on a plate, the
        publisher mark bottom-left and, over art, the house mark right."""
        reqs = [self._blank_slide_req(slide_id, 'title')]
        art = meta.get('image_url')
        reqs += self._art_field_reqs(slide_id, colors, art,
                                     meta.get('img_w'), meta.get('img_h'),
                                     meta.get('focus'))
        block_w = self.TITLE_BLOCK_W_EMU
        block_x = self.PAGE_W_EMU - self.MARGIN_EMU - block_w
        y = self.TITLE_BLOCK_Y_EMU
        sub_y = y + self.TITLE_BLOCK_H_EMU
        sub_h, foot_h = 300000, 260000
        foot = ' · '.join(s for s in (meta.get('brand', ''),
                                      meta.get('legal', '')) if s)
        if art:
            reqs += self._text_plate_reqs(
                slide_id, y,
                self.TITLE_BLOCK_H_EMU + sub_h + (foot_h if foot else 0))
        if meta.get('game_logo_url'):
            _, gy, gw, gh = self._fit_box(
                meta.get('game_logo_w'), meta.get('game_logo_h'),
                block_x, y, block_w, self.TITLE_BLOCK_H_EMU)
            reqs += self._image_reqs(slide_id, f'{slide_id}gl',
                                     meta['game_logo_url'],
                                     block_x + block_w - gw, gy, gw, gh)
        else:
            title = str(meta.get('title', '') or '')
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}t', title, block_x, y, block_w,
                self.TITLE_BLOCK_H_EMU,
                font_pt=_ladder_pt(title, block_w, self.COVER_TITLE_PT, 2,
                                   self.TITLE_BLOCK_H_EMU),
                bold=True, align='END', color=self.DECK_WHITE)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}s', meta.get('subtitle', ''),
            block_x, sub_y, block_w, sub_h, font_pt=14,
            align='END', color=self.DECK_MUTED_LIGHT)
        if foot:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}legal', foot, block_x, sub_y + sub_h,
                block_w, foot_h, font_pt=8, align='END',
                color=self.DECK_MUTED_LIGHT)
        logo_y = self.PAGE_H_EMU - self.LOGO_H_EMU - self.LOGO_CLEAR_EMU
        if meta.get('logo_url'):
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}logo', meta['logo_url'],
                self.MARGIN_EMU, logo_y, img_w=meta.get('logo_w'),
                img_h=meta.get('logo_h'))
        if art and meta.get('house_logo_url'):
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}house', meta['house_logo_url'],
                self.PAGE_W_EMU - self.MARGIN_EMU - self.LOGO_W_EMU, logo_y,
                img_w=meta.get('house_logo_w'),
                img_h=meta.get('house_logo_h'))
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_section_slide(self, presentation_id, slide_id, heading,
                          brand=None, eyebrow=None, note=None,
                          image_url=None, img_w=None, img_h=None,
                          focus=None):
        """Build a divider with optional eyebrow, note and background art,
        cropped to ``focus``, the heading block on a plate over art."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id, 'section')]
        reqs += self._art_field_reqs(slide_id, colors, image_url,
                                     img_w, img_h, focus)
        align = 'START' if self.is_dark else 'CENTER'
        if self.is_dark:
            cx = self.SECTION_X_EMU
            cw = self.PAGE_W_EMU - 2 * self.SECTION_X_EMU
        head_h = 1100000 if eyebrow else 700000
        head_y = (self.PAGE_H_EMU - head_h) // 2 - 137160
        eyebrow_y = head_y - 400000
        rule_y = head_y + head_h + 91440
        note_y, note_h = rule_y + 150000, 400000
        if image_url:
            block_top = eyebrow_y if eyebrow else head_y
            block_end = note_y + note_h if note else head_y + head_h
            reqs += self._text_plate_reqs(slide_id, block_top,
                                          block_end - block_top, 91440)
        if eyebrow:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}e', eyebrow, cx, eyebrow_y,
                cw, 340000, font_pt=12, bold=True, align=align,
                color=colors['accent'])
        head_pt = (_ladder_pt(heading or '', cw, self.STATEMENT_PT, 3, head_h)
                   if eyebrow else 28)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', heading or '', cx, head_y, cw, head_h,
            font_pt=head_pt, bold=True, align=align, color=self.DECK_WHITE)
        rule_w = 2057400
        if not self.is_dark:
            reqs += self._rect_reqs(
                slide_id, f'{slide_id}rule', (self.PAGE_W_EMU - rule_w) // 2,
                rule_y, rule_w, 45720, colors['accent'])
        if note:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}n', note, cx, note_y,
                cw, note_h, font_pt=12, align=align,
                color=self.DECK_MUTED_LIGHT)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_closing_slide(self, presentation_id, slide_id, heading,
                          logos=None, legal=None, brand=None,
                          image_url=None, img_w=None, img_h=None,
                          focus=None):
        """The deck's last slide: a thank-you on the art field over
        ``logos`` (``[publisher_url, liquid_url]``, either None) and the
        legal line."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id, 'section')]
        reqs += self._art_field_reqs(slide_id, colors, image_url,
                                     img_w, img_h, focus)
        head_y, head_h = 1700000, 900000
        if image_url:
            reqs += self._text_plate_reqs(slide_id, head_y, head_h)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', heading or 'Thank you',
            cx, head_y, cw, head_h, font_pt=36, bold=True,
            align='CENTER', color=self.DECK_WHITE)
        logo_y = self.PAGE_H_EMU - self.LOGO_H_EMU - 640000
        left, right = (list(logos or []) + [None, None])[:2]
        if left:
            reqs += self._logo_plate_reqs(slide_id, f'{slide_id}l', left,
                                          cx, logo_y)
        if right:
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}r', right,
                self.PAGE_W_EMU - self.MARGIN_EMU - self.LOGO_W_EMU, logo_y)
        if legal:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}legal', legal, cx,
                self.PAGE_H_EMU - 380000, cw, 300000, font_pt=9,
                align='CENTER', color=self.DECK_MUTED_LIGHT)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_narrative_slide(self, presentation_id, slide_id, title, text,
                            brand=None, footer=None, page=None, rich=None,
                            eyebrow=None, subhead=None, source=None,
                            stats=None):
        """A native text slide with optional bold and bullet ranges and a
        ``stats`` bar (``[(value, label), ...]``) under the text."""
        colors = self._brand_colors(brand)
        body_id = f'{slide_id}b'
        body_text = (rich or {}).get('text') or text or ''
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title or '', colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        cx, top, cw, text_h = self.body_box(self.is_dark, bool(stats))
        fit = self.fit_body(body_text, self.is_dark, bool(stats))
        self._note_fit(slide_id, 'body', fit)
        if not fit.fits:
            body_text = _clip_box(body_text, cw, text_h, fit.pt,
                                  self.NARRATIVE_SPACING)
        length = self.utf16_len(body_text)
        bold_ranges = _clamped_ranges((rich or {}).get('bold_ranges'), length)
        bullet_ranges = _clamped_ranges((rich or {}).get('bullet_ranges'),
                                        length)
        reqs += self._text_box_reqs(
            slide_id, body_id, body_text, cx, top, cw, text_h,
            font_pt=fit.pt, align='START', color=colors['ink'])
        if body_text:
            reqs.append(self._prose_spacing_req(body_id))
        if stats:
            reqs += self._stat_bar_reqs(slide_id, stats, cx, cw, colors)
        for start, end in bold_ranges:
            reqs.append({'updateTextStyle': {
                'objectId': body_id, 'style': {'bold': True},
                'textRange': {'type': 'FIXED_RANGE',
                              'startIndex': start, 'endIndex': end},
                'fields': 'bold'}})
        for start, end in bullet_ranges:
            reqs.append(self._bullets_req(body_id, start, end))
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_chart_slide(self, presentation_id, slide_id, title=None,
                        image_url=None, caption=None, notes=None,
                        img_w=None, img_h=None, brand=None, footer=None,
                        page=None, caption_h=CAPTION_H_EMU, eyebrow=None,
                        subhead=None, source=None):
        """A captioned picture fitted into :meth:`chart_image_box`."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        caption_y = self.PAGE_H_EMU - self.CAPTION_FOOT_EMU - caption_h
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        if image_url:
            box = self.chart_image_box(self.is_dark, bool(title),
                                       bool(caption), caption_h)
            x, y, w, h = self._fit_box(img_w, img_h, *box)
            reqs += self._image_reqs(slide_id, f'{slide_id}i', image_url,
                                     x, y, w, h, outline=True)
        if caption:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}c',
                _clip_box(caption, cw, caption_h, self.CAPTION_PT),
                cx, caption_y, cw, caption_h, font_pt=self.CAPTION_PT,
                align='START', color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        if notes:
            self.add_speaker_notes(presentation_id, slide_id, notes)
        return slide_id

    def add_sheets_chart_slide(self, presentation_id, slide_id, title=None,
                               spreadsheet_id=None, chart_id=None,
                               caption=None, notes=None, brand=None,
                               footer=None, page=None,
                               linking_mode='NOT_LINKED_IMAGE',
                               eyebrow=None, subhead=None, source=None):
        """Embed a native Sheets chart with the deck's content styling and
        optional caption."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        _, box_y, _, box_h = self.chart_image_box(self.is_dark, bool(title),
                                                  bool(caption))
        reqs.append({'createSheetsChart': {
            'objectId': f'{slide_id}i',
            'spreadsheetId': spreadsheet_id,
            'chartId': chart_id,
            'linkingMode': linking_mode,
            'elementProperties': self._elem_props(
                slide_id, cw, box_h, cx, box_y)}})
        if caption:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}c', caption, cx,
                self.PAGE_H_EMU - 720000, cw, self.CAPTION_H_EMU,
                font_pt=self.CAPTION_PT, align='START',
                color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        if notes:
            self.add_speaker_notes(presentation_id, slide_id, notes)
        return slide_id

    def _rect_reqs(self, slide_id, shape_id, x, y, w, h, fill_color,
                   shape_type='RECTANGLE', alpha=None):
        """A filled shape (no outline) — the card behind a stat tile, the
        band behind a section heading, the ink field behind a cover;
        ``alpha`` (0-1) makes it translucent."""
        fill, fields = _solid_fill(fill_color, alpha, 'shapeBackgroundFill')
        return [
            {'createShape': {
                'objectId': shape_id, 'shapeType': shape_type,
                'elementProperties': self._elem_props(slide_id, w, h, x, y)}},
            {'updateShapeProperties': {
                'objectId': shape_id,
                'shapeProperties': {
                    'shapeBackgroundFill': {'solidFill': fill},
                    'outline': {'propertyState': 'NOT_RENDERED'}},
                'fields': fields + ',outline.propertyState'}},
        ]

    def _image_reqs(self, slide_id, shape_id, url, x, y, w, h,
                    outline=False):
        """A picture drawn at an exact box, ruled on a light deck when
        ``outline``."""
        reqs = [{'createImage': {
            'objectId': shape_id, 'url': url,
            'elementProperties': self._elem_props(slide_id, w, h, x, y)}}]
        if outline and not self.is_dark:
            reqs.append({'updateImageProperties': {
                'objectId': shape_id,
                'imageProperties': {'outline': {
                    'outlineFill': {'solidFill': {
                        'color': {'rgbColor': self.DECK_RULE}}},
                    'weight': {'magnitude': 1, 'unit': 'PT'}}},
                'fields': 'outline'}})
        return reqs

    def _cover_fit(self, img_w, img_h, focus=None):
        """``(x, y, w, h)`` covering the page undistorted, cropped at
        ``focus`` (0-1 ``(fx, fy)``, centred by default); unknown is 16:9."""
        img_w, img_h = img_w or 16, img_h or 9
        fx, fy = focus or (0.5, 0.5)
        scale = max(self.PAGE_W_EMU / float(img_w),
                    self.PAGE_H_EMU / float(img_h))
        draw_w, draw_h = img_w * scale, img_h * scale
        return (int((self.PAGE_W_EMU - draw_w) * fx),
                int((self.PAGE_H_EMU - draw_h) * fy),
                int(draw_w), int(draw_h))

    ART_VEIL_ALPHA = 0.55
    ART_PLATE_ALPHA = 0.6
    ART_PLATE_PAD_EMU = 137160
    LOGO_W_EMU, LOGO_H_EMU = 2057400, 685800
    LOGO_CLEAR_EMU = 228600
    LOGO_PAD_EMU = 91440

    def _art_field_reqs(self, slide_id, colors, image_url=None,
                        img_w=None, img_h=None, focus=None):
        """The field behind a dark slide: full-bleed art under a
        translucent ink veil, else plain ink — or nothing at all on a
        template layout, whose own art is the field."""
        reqs = []
        if not image_url and self.is_dark:
            return reqs
        if image_url:
            x, y, w, h = self._cover_fit(img_w, img_h, focus)
            reqs += self._image_reqs(slide_id, f'{slide_id}art', image_url,
                                     x, y, w, h)
        reqs += self._rect_reqs(
            slide_id, f'{slide_id}bg', 0, 0, self.PAGE_W_EMU,
            self.PAGE_H_EMU, self.DECK_INK if self.is_dark else colors['ink'],
            alpha=self.ART_VEIL_ALPHA if image_url else None)
        return reqs

    def _text_plate_reqs(self, slide_id, y, h, pad=ART_PLATE_PAD_EMU):
        """A translucent ink plate across the page behind the heading
        block at ``y``, ``h`` tall, so the words read over any art."""
        return self._rect_reqs(slide_id, f'{slide_id}plate', 0, y - pad,
                               self.PAGE_W_EMU, h + 2 * pad, self.DECK_INK,
                               alpha=self.ART_PLATE_ALPHA)

    def _logo_plate_reqs(self, slide_id, shape_id, logo_url, x, y, w=None,
                         h=None, img_w=None, img_h=None):
        """A brand mark on a white plate at ``(x, y)``, ``w`` by ``h`` (the
        kit's logo box by default), kept to its aspect when that is known."""
        lw, lh = w or self.LOGO_W_EMU, h or self.LOGO_H_EMU
        pad = self.LOGO_PAD_EMU
        reqs = self._rect_reqs(
            slide_id, f'{shape_id}plate', x - pad, y - pad,
            lw + 2 * pad, lh + 2 * pad, self.DECK_WHITE,
            shape_type='ROUND_RECTANGLE')
        return reqs + self._image_reqs(slide_id, shape_id, logo_url,
                                       *self._fit_box(img_w, img_h, x, y, lw,
                                                      lh))

    PPTX_MIME = ('application/vnd.openxmlformats-officedocument'
                 '.presentationml.presentation')
    PDF_MIME = 'application/pdf'

    def export_file(self, file_id, mime_type=PDF_MIME):
        """A Drive file's bytes converted to ``mime_type``; ``None`` when
        Drive refuses (the credential needs the drive scope)."""
        response = self.client.get(f'{self.files_url}/{file_id}/export',
                                   params={'mimeType': mime_type})
        if getattr(response, 'status_code', 0) != 200:
            logging.warning('Drive export of %s as %s refused: %s',
                            file_id, mime_type,
                            str(getattr(response, 'text', ''))[:300])
            return None
        return response.content

    def _content_chrome_reqs(self, slide_id, title, colors, footer=None,
                             page=None, eyebrow=None, subhead=None,
                             source=None):
        """Build the heading and footer for light or dark content layouts;
        a ``subhead`` sits under a one-line title. A ``source`` runs
        small and italic on the foot line, or leads the footer's one
        line when both are given, so neither sits on the body."""
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = []
        if title:
            reqs += self._heading_reqs(slide_id, title, colors, eyebrow,
                                       subhead)
        foot_y, foot_w = self.PAGE_H_EMU - 320000, cw - 700000
        if footer and source:
            footer = _clip_lines(f'{source} · {footer}', foot_w, 9, 1)
            source = None
        if footer:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}f', footer, cx, foot_y, foot_w,
                260000, font_pt=9, align='START', color=colors['muted'])
        if source:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}src',
                _clip_lines(source, foot_w, self.SOURCE_PT, 1), cx, foot_y,
                foot_w, self.SOURCE_H_EMU, font_pt=self.SOURCE_PT,
                italic=True, align='START', color=colors['muted'])
        if page:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}pg', str(page),
                self.PAGE_W_EMU - self.MARGIN_EMU - 600000,
                foot_y, 600000, 260000, font_pt=9,
                align='END', color=colors['muted'])
        return reqs

    def _heading_reqs(self, slide_id, title, colors, eyebrow, subhead):
        """The eyebrow (dark chrome only), title and subhead, stacked
        down to :meth:`content_top`; a title past its lines at the floor
        is cut, since the box under it cannot move."""
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        title_y, one_h, two_h, subhead_h = (
            self.HEADING_DARK_EMU if self.is_dark else self.HEADING_LIGHT_EMU)
        reqs = []
        if eyebrow and self.is_dark:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}e', str(eyebrow).upper(), cx,
                self.EYEBROW_Y_EMU, cw, self.EYEBROW_H_EMU, font_pt=8,
                bold=True, align='START', color=colors['muted'],
                font=self.eyebrow_font)
        lines = 1 if subhead else self.HEADLINE_LINES
        title_pt, title_h = self._title_box(title, cw, lines, one_h, two_h)
        title = _clip_lines(title, cw, title_pt, lines)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', title, cx, title_y, cw, title_h,
            font_pt=title_pt, bold=True, align='START', color=colors['ink'])
        subhead, subhead_pt = self._subhead_line(subhead, cw)
        if subhead:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}sh', subhead, cx, title_y + title_h,
                cw, subhead_h, font_pt=subhead_pt, align='START',
                color=colors['ink'])
        return reqs

    def _subhead_line(self, subhead, width):
        """``(text, pt)`` for a subhead on one line, stepped down and then
        cut on a word, since the title block cannot grow."""
        if not subhead:
            return '', None
        sizes = self.SUBHEAD_PT_DARK if self.is_dark else self.SUBHEAD_PT_LIGHT
        fit = _fit_text(subhead, width, self.PAGE_H_EMU, sizes, max_lines=1,
                        glyph=PROSE_GLYPH_RATIO)
        return (_clip_lines(subhead, width, fit.pt, 1, PROSE_GLYPH_RATIO),
                fit.pt)

    def _tone_color(self, tone, default):
        """The deck colour a good/bad tone earns, else ``default``."""
        if self.is_dark:
            return {'good': self.DECK_GOOD_DARK,
                    'bad': self.DECK_BAD_DARK}.get(tone, default)
        return {'good': self.DECK_GOOD,
                'bad': self.DECK_BAD}.get(tone, default)

    def _card_reqs(self, slide_id, shape_id, x, y, w, h, colors):
        """Draw the kit's card fill, translucent on dark slides."""
        return self._rect_reqs(slide_id, shape_id, x, y, w, h,
                               colors['card'], alpha=colors.get('card_alpha'))

    def _tile_reqs(self, slide_id, base, tile, x, y, w, h, colors, pad,
                   sizes, heights, bold_label=False):
        """Share tile content across grids while preserving their geometry."""
        reqs = self._card_reqs(slide_id, f'{base}r', x, y, w, h, colors)
        fields = (
            ('value', 'v', colors['accent'], True),
            ('label', 'l', colors['ink'], bold_label),
            ('caption', 'p', self._tone_color(
                tile.get('tone'), colors['muted']), False),
            ('note', 'n', colors['muted'], False))
        text_y = y + pad
        for (key, suffix, color, bold), size, height in zip(
                fields, sizes, heights):
            if key in ('value', 'label') or tile.get(key):
                text = str(tile.get(key, ''))
                if key in ('caption', 'note'):
                    text = _clip_box(text, w - 2 * pad, height, size)
                reqs += self._text_box_reqs(
                    slide_id, f'{base}{suffix}', text, x + pad, text_y,
                    w - 2 * pad, height, font_pt=size, bold=bold,
                    color=color)
            text_y += height
        return reqs

    def add_stat_tile_slide(self, presentation_id, slide_id, title, tiles,
                            brand=None, footer=None, page=None,
                            eyebrow=None, subhead=None, source=None,
                            image_url=None, img_w=None, img_h=None,
                            focus=None):
        """Draw up to nine native KPI cards, four across for four, else
        three with a short last row centred, over ``image_url`` in the ink
        palette; a dense grid steps its values down before it continues."""
        colors = (self._art_colors(brand) if image_url
                  else self._brand_colors(brand))
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        if image_url:
            reqs += self._art_field_reqs(slide_id, colors, image_url,
                                         img_w, img_h, focus)
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        tiles = list(tiles or [])[:9]
        if tiles:
            cols = 4 if len(tiles) == 4 else min(len(tiles), 3)
            n_rows = (len(tiles) + cols - 1) // cols
            gap = 137160
            grid_y = self.content_top()
            grid_h = self.PAGE_H_EMU - grid_y - self.BODY_FOOT_EMU
            span = max(cols, 3)
            cell_w = (cw - gap * (span - 1)) // span
            cell_h = (grid_h - gap * (n_rows - 1)) // n_rows
            pad, text_w = TILE_PAD_EMU, cell_w - 2 * TILE_PAD_EMU
            blocks = [_tile_block_heights(tile, text_w) for tile in tiles]
            spare = cell_h - 2 * pad - max(sum(block) for block in blocks)
            if (n_rows == 1 and _values_fit(tiles, text_w, TILE_HERO_PT)
                    and TILE_HERO_VALUE_H_EMU <= spare):
                value_pt, value_h = TILE_HERO_PT, TILE_HERO_VALUE_H_EMU
            else:
                value_pt, value_h = self._value_size(
                    tiles, text_w, self.TILE_VALUE_PT_LADDER, spare)
            required = max(2 * pad + value_h + sum(block)
                           for block in blocks)
            if required > cell_h and n_rows > 1:
                for start in range(0, len(tiles), cols):
                    self.add_stat_tile_slide(
                        presentation_id, *self._part(slide_id, title, start),
                        tiles[start:start + cols], brand, footer, page,
                        eyebrow, subhead=subhead, source=source,
                        image_url=image_url, img_w=img_w, img_h=img_h,
                        focus=focus)
                return slide_id
            cell_h = min(cell_h, max(TILE_MIN_H_EMU, required))
            if n_rows == 1:
                grid_y += (grid_h - cell_h) // 2
            for i, tile in enumerate(tiles):
                r, c = divmod(i, cols)
                in_row = min(cols, len(tiles) - r * cols)
                row_x = cx + (cw - in_row * cell_w - gap * (in_row - 1)) // 2
                reqs += self._tile_reqs(
                    slide_id, f'{slide_id}c{i}', tile,
                    row_x + c * (cell_w + gap), grid_y + r * (cell_h + gap),
                    cell_w, cell_h, colors, pad, (value_pt, *TILE_TEXT_PT),
                    (value_h, *blocks[i]))
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    TILE_ROW_MAX = 3
    TILE_ROW_TILES_MAX = 6
    TILE_ROW_TEXT_PT = (9, 8, 7.5)
    TILE_ROW_VALUE_PT = (28, 24, 20, 18, 16)
    TILE_ROW_LABEL_H_EMU = 180000
    TILE_ROW_LABEL_TOP_EMU = TILE_ROW_LABEL_H_EMU + 20000
    TILE_ROW_GAP_EMU = 91440
    TILE_ROW_PAD_EMU = 80000

    @staticmethod
    def _value_size(tiles, width, sizes, height=None):
        """``(pt, box height)`` of the largest step setting every value on
        one line in a box that stands in ``height``, else the floor."""
        for font_pt in sizes:
            box_h = _value_box_h(font_pt)
            if ((height is None or box_h <= height)
                    and _values_fit(tiles, width, font_pt)):
                return font_pt, box_h
        return sizes[-1], _value_box_h(sizes[-1])

    def add_tile_rows_slide(self, presentation_id, slide_id, title, rows,
                            brand=None, footer=None, page=None,
                            eyebrow=None, subhead=None, source=None):
        """Up to three labelled bands of six tiles on one column grid,
        stepping values down and dropping notes before they continue."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        rows = [row for row in list(rows or [])[:self.TILE_ROW_MAX]
                if row.get('tiles')]
        if not rows:
            self.slides_batch_update(presentation_id, reqs)
            return slide_id
        gap, pad = self.TILE_ROW_GAP_EMU, self.TILE_ROW_PAD_EMU
        grid_y = self.content_top()
        grid_h = self.PAGE_H_EMU - grid_y - self.BODY_FOOT_EMU
        label_top = self.TILE_ROW_LABEL_TOP_EMU
        cols = max(3, *(min(len(row['tiles']), self.TILE_ROW_TILES_MAX)
                        for row in rows))
        groups = self._band_groups(rows, cols)
        if len(groups) > 1:
            for index, group in enumerate(groups):
                self.add_tile_rows_slide(
                    presentation_id, *self._part(slide_id, title, index),
                    group, brand, footer, page, eyebrow, subhead=subhead,
                    source=source)
            return slide_id
        tile_hs = (self._band_heights(rows, cols)
                   or [grid_h - label_top] * len(rows))
        y = grid_y
        for r, (row, tile_h) in enumerate(zip(rows, tile_hs)):
            base = f'{slide_id}b{r}'
            reqs += self._text_box_reqs(
                slide_id, f'{base}l', str(row.get('label', '')).upper(),
                cx, y, cw, self.TILE_ROW_LABEL_H_EMU, font_pt=8,
                bold=True, align='START', color=colors['muted'],
                font=self.eyebrow_font)
            tiles, cell_w, value_pt, heights, with_notes = (
                self._tile_band(row, cw, tile_h, cols)
                or self._tile_band(row, cw, tile_h, cols, force=True))
            for i, tile in enumerate(tiles):
                note = tile.get('note') if with_notes else None
                reqs += self._tile_reqs(
                    slide_id, f'{base}t{i}', dict(tile, note=note),
                    cx + i * (cell_w + gap), y + label_top, cell_w, tile_h,
                    colors, pad, (value_pt, *self.TILE_ROW_TEXT_PT),
                    heights[i],
                    bold_label=True)
            y += label_top + tile_h + gap
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def _band_heights(self, rows, cols):
        """Tile heights for ``rows`` on one slide: an even share if every
        band stands in it, else each band's floor (notes dropped) plus an
        even share of the spare; None when the floors overflow."""
        _, _, width, grid_h = self.body_box(self.is_dark)
        n = len(rows)
        room = (grid_h - self.TILE_ROW_GAP_EMU * (n - 1)
                - n * self.TILE_ROW_LABEL_TOP_EMU)
        if all(self._tile_band(row, width, room // n, cols) for row in rows):
            return [room // n] * n
        floors = [max(2 * self.TILE_ROW_PAD_EMU + sum(block)
                      for block in self._tile_band(row, width, 0, cols,
                                                   force=True)[3])
                  for row in rows]
        spare = room - sum(floors)
        if spare < 0:
            return None
        return [floor + spare // n for floor in floors]

    def _band_groups(self, rows, cols):
        """``rows`` in slide-sized runs, each slide taking as many leading
        bands as :meth:`_band_heights` can stand."""
        groups, rest = [], list(rows)
        while rest:
            take = next((k for k in range(len(rest), 1, -1)
                         if self._band_heights(rest[:k], cols)), 1)
            groups.append(rest[:take])
            rest = rest[take:]
        return groups

    def _tile_band(self, row, width, tile_h, cols, force=False):
        """``(tiles, cell_w, value_pt, heights, with_notes)`` for a band
        that fits ``tile_h``, dropping notes first, else None; ``force``
        answers without notes for a band with a slide to itself."""
        tiles = list(row['tiles'])[:self.TILE_ROW_TILES_MAX]
        pad = self.TILE_ROW_PAD_EMU
        cell_w = (width - self.TILE_ROW_GAP_EMU * (cols - 1)) // cols
        text_w = cell_w - 2 * pad
        label_pt, caption_pt, note_pt = self.TILE_ROW_TEXT_PT
        label_h = int(label_pt * 1.6 * EMU_PER_PT)
        captions = [_block_height(tile.get('caption'), text_w, caption_pt)
                    for tile in tiles]
        notes = [_block_height(tile.get('note'), text_w, note_pt)
                 for tile in tiles]
        for with_notes in (True, False):
            tails = [caption + (note if with_notes else 0)
                     for caption, note in zip(captions, notes)]
            spare = tile_h - 2 * pad - label_h - max(tails, default=0)
            value_pt, value_h = self._value_size(
                tiles, text_w, self.TILE_ROW_VALUE_PT, spare)
            heights = [(value_h, label_h, caption, note if with_notes else 0)
                       for caption, note in zip(captions, notes)]
            if (max(2 * pad + sum(block) for block in heights) <= tile_h
                    or (force and not with_notes)):
                return tiles, cell_w, value_pt, heights, with_notes
        return None

    STORY_CHART_SHARE = 0.6
    STORY_SPLIT_SHARE = 0.5
    STORY_STAT_H_EMU = 440000
    STORY_GAP_EMU = 182880

    def _stat_bar_reqs(self, slide_id, stats, x, w, colors):
        """One card on the body's foot carrying ``stats``
        (``[(value, label), ...]``) on one line."""
        stat_h, pad = self.STORY_STAT_H_EMU, 60000
        y = self.PAGE_H_EMU - self.BODY_FOOT_EMU - stat_h
        reqs = self._card_reqs(slide_id, f'{slide_id}sr', x, y, w, stat_h,
                               colors)
        text, stat_pt = self._stat_line(stats, w - 2 * pad)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}s', text, x + pad, y + pad, w - 2 * pad,
            stat_h - 2 * pad, font_pt=stat_pt, bold=True, align='CENTER',
            color=colors['ink'])
        return reqs

    def add_chart_story_slide(self, presentation_id, slide_id, title,
                              image_url=None, img_w=None, img_h=None,
                              narrative=None, stats=None, brand=None,
                              footer=None, page=None, eyebrow=None,
                              subhead=None, source=None, left=None):
        """Place a chart beside its narrative and above a stat bar, full
        width without narrative and between two rails with ``left``."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        gap = self.STORY_GAP_EMU
        split = bool(narrative and left)
        chart_x, top, chart_w, chart_h = self.story_chart_box(
            self.is_dark, bool(narrative), bool(stats), split)
        body_h = self.PAGE_H_EMU - top - self.BODY_FOOT_EMU
        if split:
            reqs += self._prose_reqs(
                slide_id, f'{slide_id}ln', left,
                (cx, top, chart_x - gap - cx, body_h),
                colors, self.RAIL_PT, 'rail')
        if image_url:
            x, y, w, h = self._fit_box(img_w, img_h, chart_x, top, chart_w,
                                       chart_h)
            reqs += self._image_reqs(slide_id, f'{slide_id}i', image_url,
                                     x, y, w, h)
        if stats:
            reqs += self._stat_bar_reqs(slide_id, stats, chart_x, chart_w,
                                        colors)
        if narrative:
            rail_x = chart_x + chart_w + gap
            reqs += self._prose_reqs(
                slide_id, f'{slide_id}n', narrative,
                (rail_x, top, cx + cw - rail_x, body_h),
                colors, self.RAIL_PT, 'rail')
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    COLUMN_ART_H_EMU = 1100000
    COLUMN_HEAD_H_EMU = 400000
    COLUMN_HEAD_LINES = 2

    def _stat_line(self, stats, width):
        """``(text, pt)`` for the stat bar on one line, stepped down and
        then dropping trailing stats, since a wrapped bar overlaps the
        chart."""
        for count in range(len(stats), 0, -1):
            text = '   |   '.join(f'{value} {label}'
                                  for value, label in stats[:count])
            fit = _fit_text(text, width, self.PAGE_H_EMU, self.STAT_PT,
                            max_lines=1)
            if fit.fits or count == 1:
                return text, fit.pt

    def _card_head_reqs(self, slide_id, shape_id, head, x, y, w, colors,
                        tone=None):
        """``(reqs, height)`` of a card's toned heading, fitted to
        :data:`COLUMN_HEAD_LINES` lines and cut on a word past them."""
        head = str(head or '')
        head_fit = _fit_text(head, w, self.PAGE_H_EMU, self.CARD_HEAD_PT,
                             max_lines=self.COLUMN_HEAD_LINES)
        if not head_fit.fits:
            head = _clip_lines(head, w, head_fit.pt, self.COLUMN_HEAD_LINES)
        head_h = max(self.COLUMN_HEAD_H_EMU,
                     _block_height(head, w, head_fit.pt))
        reqs = self._text_box_reqs(
            slide_id, shape_id, head, x, y, w, head_h, font_pt=head_fit.pt,
            bold=True, align='START',
            color=self._tone_color(tone, colors['accent']))
        return reqs, head_h

    def _bullet_prose_reqs(self, slide_id, shape_id, text, bullets, box,
                           colors, sizes, slot):
        """:meth:`_prose_reqs` with ``bullets`` joined under ``text`` as
        one bulleted range, dropping trailing bullets that overflow."""
        bullets = [str(b) for b in bullets or [] if str(b or '').strip()]
        if not bullets:
            return self._prose_reqs(slide_id, shape_id, text, box, colors,
                                    sizes, slot)
        x, y, w, h = box
        lead = f'{text}\n' if text else ''
        for count in range(len(bullets), 0, -1):
            joined = ''.join((lead, '\n'.join(bullets[:count])))
            fit = self._fit_prose(joined, w, h, sizes)
            if fit.fits:
                break
        self._note_fit(slide_id, slot, fit)
        if not fit.fits:
            joined = _clip_box(joined, w, h, fit.pt, self.NARRATIVE_SPACING)
        reqs = self._text_box_reqs(slide_id, shape_id, joined, x, y, w, h,
                                   font_pt=fit.pt, align='START',
                                   color=colors['ink'])
        reqs.append(self._prose_spacing_req(shape_id))
        end = self.utf16_len(joined)
        start = min(self.utf16_len(lead), end)
        if end > start:
            reqs.append(self._bullets_req(shape_id, start, end))
        return reqs

    @staticmethod
    def _bullets_req(shape_id, start, end):
        """Disc bullets on the paragraphs in ``[start, end)``, UTF-16
        indices as the API counts them."""
        return {'createParagraphBullets': {
            'objectId': shape_id,
            'textRange': {'type': 'FIXED_RANGE',
                          'startIndex': start, 'endIndex': end},
            'bulletPreset': 'BULLET_DISC_CIRCLE_SQUARE'}}

    def _column_reqs(self, slide_id, base, column, x, y, w, h, colors):
        """One card: an optional picture, the toned head, an optional
        ``stat`` figure and the body with its ``bullets``, each fitted."""
        pad = 100000
        reqs = self._card_reqs(slide_id, f'{base}r', x, y, w, h, colors)
        text_y, text_w = y + pad, w - 2 * pad
        if column.get('image_url'):
            ix, iy, iw, ih = self._fit_box(
                column.get('img_w'), column.get('img_h'), x + pad, text_y,
                text_w, self.COLUMN_ART_H_EMU)
            reqs += self._image_reqs(slide_id, f'{base}i',
                                     column['image_url'], ix, iy, iw, ih)
            text_y += self.COLUMN_ART_H_EMU + pad
        head_reqs, head_h = self._card_head_reqs(
            slide_id, f'{base}h', column.get('head', ''), x + pad, text_y,
            text_w, colors, column.get('tone'))
        reqs += head_reqs
        body_y = text_y + head_h + 20000
        stat = str(column.get('stat') or '')
        if stat:
            stat_pt = _ladder_pt(stat, text_w, self.CARD_STAT_PT, 1)
            stat_h = _block_height(stat, text_w, stat_pt)
            reqs += self._text_box_reqs(
                slide_id, f'{base}s', stat, x + pad, body_y, text_w, stat_h,
                font_pt=stat_pt, bold=True, align='START',
                color=colors['accent'])
            body_y += stat_h
        return reqs + self._bullet_prose_reqs(
            slide_id, f'{base}b', str(column.get('body', '')),
            column.get('bullets'),
            (x + pad, body_y, text_w, max(y + h - pad - body_y, pad)),
            colors, self.CARD_BODY_PT, 'card')

    def add_columns_slide(self, presentation_id, slide_id, title, columns,
                          brand=None, footer=None, page=None, eyebrow=None,
                          subhead=None, source=None):
        """Draw up to four narrative cards with toned headings; a column
        carrying ``image_url`` opens on the picture."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        columns = list(columns or [])[:4]
        if columns:
            gap = 137160
            y = self.content_top()
            h = self.PAGE_H_EMU - y - self.BODY_FOOT_EMU
            w = (cw - gap * (len(columns) - 1)) // len(columns)
            for i, column in enumerate(columns):
                reqs += self._column_reqs(slide_id, f'{slide_id}k{i}',
                                          column, cx + i * (w + gap), y,
                                          w, h, colors)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    # A display value that reads as a number/rate — right-aligned in native
    # tables ("$1,234.50", "1.2B", "0.15%", "3.4x", "120% of plan").
    _NUMERIC_CELL_RE = re.compile(
        r'^[-+(]?[$€£]?\s?\d[\d,.]*\s*(?:[KMB%]|x)?\)?'
        r'(?:\s+of plan)?$', re.IGNORECASE)

    def _text_on(self, fill):
        """White or ink, whichever contrasts more with ``fill``
        (``{'color', 'alpha'?}``) laid over the page."""
        under = self.DECK_INK if self.is_dark else self.DECK_WHITE
        seen = _composite(fill['color'], fill.get('alpha'), under)
        return max((self.DECK_WHITE, self.DECK_INK),
                   key=lambda ink: _contrast(ink, seen))

    @staticmethod
    def _cell_fill_req(table_id, row, col, rows, cols, fill):
        """One ``updateTableCellProperties`` filling a block of cells
        with ``fill`` (``{'color', 'alpha'?}``)."""
        solid, fields = _solid_fill(fill['color'], fill.get('alpha'),
                                    'tableCellBackgroundFill')
        return {'updateTableCellProperties': {
            'objectId': table_id,
            'tableRange': {'location': {'rowIndex': row, 'columnIndex': col},
                           'rowSpan': rows, 'columnSpan': cols},
            'tableCellProperties': {'tableCellBackgroundFill': {
                'solidFill': solid}},
            'fields': fields}}

    def _table_reqs(self, slide_id, table_id, x, y, w, h, header, body_rows,
                    colors, col_widths=None, header_fills=None,
                    label_col=False, head_pt=TABLE_HEAD_PT,
                    body_pt=TABLE_BODY_PT, row_heights=()):
        """A native table with ``header_fills`` (None takes the card), zebra
        rows, aligned numbers and a bold ``label_col`` rail; Slides rejects
        empty-cell styles and columns under 32pt, so neither is sent."""
        all_rows = [list(header)] + [list(r) for r in body_rows]
        n_cols = len(header)
        card = {'color': colors['card'], 'alpha': colors.get('card_alpha')}
        fills = [fill or card for fill in
                 header_fills or [{'color': colors['accent']}] * n_cols]
        reqs = [{'createTable': {
            'objectId': table_id,
            'elementProperties': self._elem_props(slide_id, w, h, x, y),
            'rows': len(all_rows), 'columns': n_cols}}]
        for r, row in enumerate(all_rows):
            is_header = (r == 0)
            for c in range(n_cols):
                val = '' if c >= len(row) or row[c] is None else str(row[c])
                if not val:
                    continue
                is_label = label_col and c == 0 and not is_header
                loc = {'rowIndex': r, 'columnIndex': c}
                reqs.append({'insertText': {
                    'objectId': table_id, 'cellLocation': loc,
                    'insertionIndex': 0, 'text': val}})
                reqs.append({'updateTextStyle': {
                    'objectId': table_id, 'cellLocation': loc,
                    'style': {
                        'fontSize': {'magnitude': (head_pt if is_header
                                                   else body_pt),
                                     'unit': 'PT'},
                        'bold': is_header or is_label,
                        'fontFamily': self.deck_font,
                        'foregroundColor': {'opaqueColor': {'rgbColor': (
                            self._text_on(fills[c]) if is_header
                            else colors['ink'])}}},
                    'textRange': {'type': 'ALL'},
                    'fields': 'fontSize,bold,fontFamily,foregroundColor'}})
                if (not is_header and not is_label
                        and self._NUMERIC_CELL_RE.match(val)):
                    reqs.append({'updateParagraphStyle': {
                        'objectId': table_id, 'cellLocation': loc,
                        'style': {'alignment': 'END'},
                        'textRange': {'type': 'ALL'},
                        'fields': 'alignment'}})
        if header_fills:
            reqs += [self._cell_fill_req(table_id, 0, c, 1, 1, fill)
                     for c, fill in enumerate(fills)]
        else:
            reqs.append(self._cell_fill_req(table_id, 0, 0, 1, n_cols,
                                            fills[0]))
        for r in range(2, len(all_rows), 2):
            reqs.append(self._cell_fill_req(table_id, r, 0, 1, n_cols, card))
        if label_col and len(all_rows) > 1:
            reqs.append(self._cell_fill_req(table_id, 1, 0,
                                            len(all_rows) - 1, 1, card))
        if col_widths and min(col_widths) >= TABLE_COL_MIN_EMU:
            for c, width in enumerate(col_widths):
                reqs.append({'updateTableColumnProperties': {
                    'objectId': table_id, 'columnIndices': [c],
                    'tableColumnProperties': {
                        'columnWidth': self._emu(width)},
                    'fields': 'columnWidth'}})
        if self.is_dark:
            reqs.append({'updateTableBorderProperties': {
                'objectId': table_id, 'borderPosition': 'ALL',
                'tableBorderProperties': {
                    'tableBorderFill': {'solidFill': {
                        'color': {'rgbColor': self.DECK_RULE_DARK}}},
                    'weight': {'magnitude': 0.5, 'unit': 'PT'}},
                'fields': 'tableBorderFill,weight'}})
        for index, height in enumerate(row_heights):
            reqs.append({'updateTableRowProperties': {
                'objectId': table_id, 'rowIndices': [index],
                'tableRowProperties': {'minRowHeight': self._emu(height)},
                'fields': 'minRowHeight'}})
        return reqs

    def add_table_slide(self, presentation_id, slide_id, title, header,
                        body_rows, brand=None, caption=None, footer=None,
                        page=None, eyebrow=None, subhead=None,
                        source=None, narrative=None, stats=None,
                        col_widths=None):
        """Build a native table, continuing rows on the whole table's
        ``col_widths`` before wrapped text overflows, with an optional
        ``narrative`` rail and ``stats`` bar; a short table sets larger and
        a full one's caption sits at the body's foot."""
        cx, top, tw, body_h = self.story_chart_box(
            self.is_dark, bool(narrative), bool(stats))
        widths = col_widths or _table_widths(
            header, body_rows, tw, self.TABLE_HEAD_PT, self.TABLE_BODY_PT)
        groups = self.table_row_groups(header, body_rows, self.is_dark,
                                       caption, bool(narrative), bool(stats),
                                       widths)
        if len(groups) > 1:
            for index, rows in enumerate(groups):
                self.add_table_slide(
                    presentation_id, *self._part(slide_id, title, index),
                    header, rows, brand, caption, footer, page, eyebrow,
                    subhead=subhead, source=source, narrative=narrative,
                    stats=stats, col_widths=widths)
            return slide_id
        colors = self._brand_colors(brand)
        full_h = self.PAGE_H_EMU - top - self.BODY_FOOT_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow, subhead=subhead,
                                          source=source)
        reserve = self._caption_reserve(caption, tw, body_h)
        budget = body_h - reserve
        head_pt, body_pt = self.TABLE_HEAD_PT, self.TABLE_BODY_PT
        head_h, heights = _table_heights(header, body_rows, widths, head_pt,
                                         body_pt)
        ty = top
        if (not col_widths and len(body_rows) <= self.TABLE_SHORT_ROWS
                and head_h + sum(heights) < self.TABLE_SHORT_SHARE * budget):
            head_pt, body_pt = (self.TABLE_SHORT_HEAD_PT,
                                self.TABLE_SHORT_BODY_PT)
            widths = _table_widths(header, body_rows, tw, head_pt, body_pt)
            head_h, heights = _table_heights(header, body_rows, widths,
                                             head_pt, body_pt,
                                             self.TABLE_SHORT_ROW_MIN_EMU)
            if not narrative:
                ty += max(0, budget - head_h - sum(heights)) // 2
        th = min(budget, head_h + sum(heights))
        reqs += self._table_reqs(slide_id, f'{slide_id}tbl', cx, ty, tw, th,
                                 header, body_rows, colors,
                                 col_widths=widths, head_pt=head_pt,
                                 body_pt=body_pt,
                                 row_heights=[head_h] + heights)
        if caption:
            cap_h = reserve - self.CAPTION_GAP_EMU
            cap_y = ty + th + self.CAPTION_GAP_EMU
            if th > budget * self.TABLE_FILL_SHARE:
                cap_y = max(cap_y, top + body_h - cap_h)
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}c',
                _clip_box(caption, tw, cap_h, self.CAPTION_PT),
                cx, cap_y, tw, cap_h, font_pt=self.CAPTION_PT,
                align='START', color=colors['muted'])
        if stats:
            reqs += self._stat_bar_reqs(slide_id, stats, cx, tw, colors)
        if narrative:
            nx = cx + tw + self.STORY_GAP_EMU
            reqs += self._prose_reqs(
                slide_id, f'{slide_id}n', narrative,
                (nx, top, self.PAGE_W_EMU - self.MARGIN_EMU - nx, full_h),
                colors, self.RAIL_PT, 'rail')
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def _prose_reqs(self, slide_id, shape_id, text, box, colors, sizes,
                    slot):
        """Body copy in ``box`` stepped down ``sizes`` and, at the floor,
        cut to the lines the box holds; the speaker note keeps it whole."""
        x, y, w, h = box
        fit = self._fit_prose(text, w, h, sizes)
        if not fit.fits:
            self._note_fit(slide_id, slot, fit)
            lines = _lines_fit(h, fit.pt, self.NARRATIVE_SPACING)
            text = _clip_lines(text, w, fit.pt, max(1, lines),
                               PROSE_GLYPH_RATIO)
        reqs = self._text_box_reqs(slide_id, shape_id, text, x, y, w, h,
                                   font_pt=fit.pt, align='START',
                                   color=colors['ink'])
        if text:
            reqs.append(self._prose_spacing_req(shape_id))
        return reqs

    def _prose_spacing_req(self, shape_id):
        """The paragraph spacing every prose box and its fit share."""
        below = {'magnitude': self.NARRATIVE_SPACE_BELOW_PT, 'unit': 'PT'}
        return {'updateParagraphStyle': {
            'objectId': shape_id,
            'style': {'lineSpacing': self.NARRATIVE_SPACING,
                      'spaceBelow': below},
            'textRange': {'type': 'ALL'},
            'fields': 'lineSpacing,spaceBelow'}}

    EVIDENCE_LABEL_EMU = (502920, 128016, 8138160, 868680)
    EVIDENCE_FRAME_EMU = (411480, 1051560, 8321040, 3657600)
    EVIDENCE_LINE_EMU = 300000
    EVIDENCE_SMALL_EMU = 200000

    def add_evidence_slide(self, presentation_id, slide_id, label,
                           subline=None, date_line=None, image_url=None,
                           img_w=None, img_h=None, brand=None, footer=None,
                           page=None):
        """One captured placement: a label, subline and date over the
        screenshot, fitted into its frame and never cropped."""
        colors = self._brand_colors(brand)
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, None, colors,
                                          footer=footer, page=page)
        lx, ly, lw, _ = self.EVIDENCE_LABEL_EMU
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}l', label or '', lx, ly, lw,
            self.EVIDENCE_LINE_EMU, font_pt=15, bold=True,
            color=colors['ink'])
        y = ly + self.EVIDENCE_LINE_EMU
        for suffix, text, bold, font in (
                ('s', subline, True, self.eyebrow_font),
                ('d', date_line, False, None)):
            if not text:
                continue
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}{suffix}', text, lx, y, lw,
                self.EVIDENCE_SMALL_EMU, font_pt=8, bold=bold,
                color=colors['muted'], font=font)
            y += self.EVIDENCE_SMALL_EMU
        if image_url:
            x, iy, w, h = self._fit_box(img_w, img_h,
                                        *self.EVIDENCE_FRAME_EMU)
            reqs += self._image_reqs(slide_id, f'{slide_id}i', image_url,
                                     x, iy, w, h, outline=True)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_toc_slide(self, presentation_id, slide_id, entries, brand=None,
                      footer=None, page=None, eyebrow=None):
        """A table-of-contents slide: numbered section labels (gold-deck
        convention). ``entries`` is a list of section-label strings."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, 'Contents', colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        entries = [f'{i}.  {entry}'
                   for i, entry in enumerate(entries or [], 1)]
        top = self.content_top()
        body_h = self.PAGE_H_EMU - top - self.BODY_FOOT_EMU
        fit = _fit_text('\n'.join(entries), cw, body_h, self.TOC_PT)
        columns = [entries]
        if not fit.fits and len(entries) > 1:
            half = (len(entries) + 1) // 2
            columns = [entries[:half], entries[half:]]
        col_w = (cw - self.STORY_GAP_EMU * (len(columns) - 1)) // len(columns)
        for index, column in enumerate(columns):
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}b{index or ""}',
                '\n'.join(column), cx + index * (col_w + self.STORY_GAP_EMU),
                top, col_w, body_h, font_pt=fit.pt, align='START',
                color=colors['ink'])
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    PARAM_LEFT_SHARE = 0.46
    PARAM_PAD_EMU = 137160
    PARAM_FACT_PT = (14, 13, 12)
    PARAM_LABEL_PT = 11
    PARAM_BUDGET_PT = (40, 32, 28)
    PARAM_SPLIT_H_EMU = 640000
    PARAM_SPLIT_PT = (18, 16, 14)
    PARAM_SPLIT_TEXT_PT = (9, 8, 8)
    PARAM_CHIP_PT = 10
    PARAM_CHIP_H_EMU = 280000
    PARAM_BEAT_PT = 12

    def _nested_colors(self, colors):
        """Colours for a tile, pill or chip on a card: white on a light
        card, as they are on a translucent dark one."""
        if colors.get('card_alpha') is not None:
            return colors
        return dict(colors, card=self.DECK_WHITE)

    def _fact_text(self, pairs):
        """``(text, label ranges, bullet ranges)`` of upper-case labels
        over their lines, facts a blank line apart, in UTF-16 indices."""
        parts, labels, bullets, pos = [], [], [], 0
        for index, (label, lines) in enumerate(pairs):
            if index:
                parts.append('\n')
                pos += 1
            for block, ranges in ((label.upper(), labels),
                                  ('\n'.join(lines), bullets)):
                if block:
                    end = pos + self.utf16_len(block)
                    ranges.append((pos, end))
                    parts.append(f'{block}\n')
                    pos = end + 1
        return ''.join(parts).rstrip('\n'), labels, bullets

    def _fact_list_reqs(self, slide_id, shape_id, facts, box, colors):
        """A parameters slide's labelled bullet lists in one box, stepped
        down the fact ladder and then cut to the facts that fit."""
        pairs = _fact_pairs(facts)
        if not pairs:
            return []
        x, y, w, h = box
        text, _, _ = self._fact_text(pairs)
        fit = self._fit_prose(text, w, h, self.PARAM_FACT_PT)
        if not fit.fits:
            self._note_fit(slide_id, 'facts', fit)
            max_lines = _lines_fit(h, fit.pt, self.NARRATIVE_SPACING)
            pairs = _clip_pairs(pairs, w, fit.pt, max(1, max_lines))
        text, labels, bullets = self._fact_text(pairs)
        reqs = self._text_box_reqs(slide_id, shape_id, text, x, y, w, h,
                                   font_pt=fit.pt, align='START',
                                   color=colors['ink'])
        reqs.append(self._prose_spacing_req(shape_id))
        for start, end in labels:
            reqs.append({'updateTextStyle': {
                'objectId': shape_id,
                'style': {'bold': True, 'fontFamily': self.eyebrow_font,
                          'fontSize': {'magnitude': self.PARAM_LABEL_PT,
                                       'unit': 'PT'},
                          'foregroundColor': {'opaqueColor': {
                              'rgbColor': colors['muted']}}},
                'textRange': {'type': 'FIXED_RANGE',
                              'startIndex': start, 'endIndex': end},
                'fields': 'bold,fontFamily,fontSize,foregroundColor'}})
        reqs += [self._bullets_req(shape_id, start, end)
                 for start, end in bullets]
        return reqs

    def _split_tile_reqs(self, slide_id, split, x, y, w, colors):
        """The budget split as a row of small tiles on the card."""
        gap, pad = 91440, self.TILE_ROW_PAD_EMU
        tile_w = (w - gap * (len(split) - 1)) // len(split)
        text_w = tile_w - 2 * pad
        value_pt, value_h = self._value_size(split, text_w,
                                             self.PARAM_SPLIT_PT)
        text_pt = self.PARAM_SPLIT_TEXT_PT
        tile_colors = self._nested_colors(colors)
        reqs = []
        for i, tile in enumerate(split):
            reqs += self._tile_reqs(
                slide_id, f'{slide_id}s{i}', tile, x + i * (tile_w + gap), y,
                tile_w, self.PARAM_SPLIT_H_EMU, tile_colors, pad,
                (value_pt, *text_pt),
                (value_h, *_tile_block_heights(tile, text_w, text_pt)))
        return reqs

    def _pill_reqs(self, slide_id, rect_id, text_id, text, box, font_pt,
                   colors):
        """A chip or wordmark: ``text`` centred and bold on a rounded
        fill."""
        x, y, w, h = box
        fill = self._nested_colors(colors)
        reqs = self._rect_reqs(slide_id, rect_id, x, y, w, h, fill['card'],
                               'ROUND_RECTANGLE', fill.get('card_alpha'))
        return reqs + self._text_box_reqs(
            slide_id, text_id, text, x, y, w, h, font_pt=font_pt, bold=True,
            align='CENTER', color=colors['ink'])

    def _chip_row_reqs(self, slide_id, base, chips, x, y, w, bottom,
                       colors):
        """``(reqs, next y)`` of ``chips`` flowed as pills across ``w``
        and down to ``bottom``, past which none is drawn."""
        chip_h, gap = self.PARAM_CHIP_H_EMU, 60000
        reqs, chip_x, chip_y = [], x, y
        for i, chip in enumerate(chips):
            text = str(chip)
            chip_w = min(w, int(len(text) * self.PARAM_CHIP_PT * EMU_PER_PT
                                * GLYPH_WIDTH_RATIO) + 180000)
            if chip_x > x and chip_x + chip_w > x + w:
                chip_x, chip_y = x, chip_y + chip_h + gap
            if chip_y + chip_h > bottom:
                break
            reqs += self._pill_reqs(
                slide_id, f'{base}c{i}', f'{base}t{i}', text,
                (chip_x, chip_y, chip_w, chip_h), self.PARAM_CHIP_PT, colors)
            chip_x += chip_w + gap
        return reqs, (chip_y + chip_h + 91440 if reqs else y)

    def _budget_card_reqs(self, slide_id, box, budget, split, territories,
                          beats, colors):
        """One card stacking the budget figure, its split tiles,
        territory chips and the dated beats while it has room."""
        x, y, w, h = box
        pad = self.PARAM_PAD_EMU
        reqs = self._card_reqs(slide_id, f'{slide_id}bc', x, y, w, h, colors)
        ix, iw = x + pad, w - 2 * pad
        cy, bottom = y + pad, y + h - pad
        budget = budget or {}
        value = str(budget.get('value') or '')
        if value:
            font_pt = _ladder_pt(value, iw, self.PARAM_BUDGET_PT, 1)
            value_h = int(_line_pitch(font_pt)) + 60000
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}bv', value, ix, cy, iw, value_h,
                font_pt=font_pt, bold=True, color=colors['accent'])
            cy += value_h
            if budget.get('label'):
                reqs += self._text_box_reqs(
                    slide_id, f'{slide_id}bl', str(budget['label']), ix, cy,
                    iw, 260000, font_pt=12, color=colors['muted'])
                cy += 300000
        split = list(split or [])[:3]
        if split and cy + self.PARAM_SPLIT_H_EMU <= bottom:
            reqs += self._split_tile_reqs(slide_id, split, ix, cy, iw, colors)
            cy += self.PARAM_SPLIT_H_EMU + 91440
        if territories:
            chip_reqs, cy = self._chip_row_reqs(
                slide_id, f'{slide_id}t', territories, ix, cy, iw, bottom,
                colors)
            reqs += chip_reqs
        beats = [str(beat) for beat in beats or [] if str(beat or '').strip()]
        max_lines = _lines_fit(bottom - cy, self.PARAM_BEAT_PT)
        if beats and max_lines >= 1:
            kept, _ = _leading_fit(
                beats, [_wrapped_lines(beat, iw, self.PARAM_BEAT_PT)
                        for beat in beats], max_lines)
            if len(kept) < len(beats):
                kept[-1] = f'+{len(beats) - len(kept) + 1} more beats'
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}beats', '\n'.join(kept), ix, cy, iw,
                bottom - cy, font_pt=self.PARAM_BEAT_PT, color=colors['ink'])
        return reqs

    def add_parameters_slide(self, presentation_id, slide_id, title, facts,
                             budget=None, split=None, territories=None,
                             beats=None, brand=None, footer=None, page=None,
                             eyebrow=None, subhead=None, source=None):
        """``facts`` (``[{'label', 'lines'}]``) down the left column and a
        budget card on the right; nothing continues, so facts and beats
        clip to what fits."""
        colors = self._brand_colors(brand)
        cx, top, cw, body_h = self.body_box(self.is_dark)
        reqs = [self._blank_slide_req(slide_id), *self._content_chrome_reqs(
            slide_id, title, colors, footer, page, eyebrow, subhead, source)]
        left_w = int(cw * self.PARAM_LEFT_SHARE)
        reqs += self._fact_list_reqs(slide_id, f'{slide_id}facts', facts,
                                     (cx, top, left_w, body_h), colors)
        right_x = cx + left_w + self.STORY_GAP_EMU
        reqs += self._budget_card_reqs(
            slide_id, (right_x, top, cx + cw - right_x, body_h), budget,
            split, territories, beats, colors)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    PHASE_MATRIX_MAX = 6
    PHASE_LABEL_W_EMU = 1371600
    PHASE_MIN_SHARE = 0.12
    PHASE_PT = (11, 10, 9)
    PHASE_FILL_ALPHAS = (1.0, 0.75, 0.55, 0.4)

    def _phase_widths(self, width, weights, columns):
        """Column EMU for the label rail and each phase of ``columns`` by
        spend ``weights``, each at least its floor and longest word."""
        avail = width - self.PHASE_LABEL_W_EMU
        count = len(columns)
        weights = [max(float(weight or 0), 0.0)
                   for weight in (list(weights or []) + [0] * count)[:count]]
        mins = [max(self.PHASE_MIN_SHARE,
                    _longest_word_emu(cells, self.PHASE_PT[0]) / avail)
                for cells in columns]
        return ([self.PHASE_LABEL_W_EMU]
                + _weighted_widths(avail, weights, mins))

    def _phase_fills(self, count, colors):
        """One header fill per phase: the accent stepping down an alpha
        ladder on dark decks, the kit's palette cycling on light ones."""
        if self.is_dark:
            steps = self.PHASE_FILL_ALPHAS
            return [{'color': colors['accent'], 'alpha': steps[i % len(steps)]}
                    for i in range(count)]
        palette = (colors['accent'], self.DECK_INK, self.DECK_GOOD,
                   self.DECK_MUTED)
        return [{'color': palette[i % len(palette)]} for i in range(count)]

    def _matrix_fit(self, header, body, widths, budget):
        """``(pt, header height, row heights)`` on the matrix ladder: the
        largest step whose rows stand in ``budget``, else the floor."""
        for font_pt in self.PHASE_PT:
            head_h, heights = _table_heights(header, body, widths, font_pt,
                                             font_pt)
            if head_h + sum(heights) <= budget:
                break
        return font_pt, head_h, heights

    def _phase_table_reqs(self, slide_id, phases, rows, weights, colors):
        """The matrix across the body on the :data:`PHASE_PT` ladder,
        each row cut to an even share of the body past its floor."""
        cx, top, tw, body_h = self.body_box(self.is_dark)
        count = len(phases)
        header = [''] + ['\n'.join(str(phase[key]) for key in ('head', 'dates')
                                   if phase.get(key)) for phase in phases]
        body = [[label, *('' if cell is None else str(cell)
                          for cell in (cells + [''] * count)[:count])]
                for label, cells in rows]
        widths = self._phase_widths(tw, weights,
                                    list(zip(header, *body))[1:])
        font_pt, head_h, heights = self._matrix_fit(header, body, widths,
                                                    body_h)
        if head_h + sum(heights) > body_h:
            body = _clip_rows(body, widths, font_pt,
                              max(1, (body_h - head_h) // len(body)))
            heights = [_table_row_height(row, widths, font_pt)
                       for row in body]
        return self._table_reqs(
            slide_id, f'{slide_id}tbl', cx, top, tw,
            min(body_h, head_h + sum(heights)), header, body, colors,
            col_widths=widths,
            header_fills=[None] + self._phase_fills(count, colors),
            label_col=True, head_pt=font_pt, body_pt=font_pt,
            row_heights=[head_h] + heights)

    def add_phase_matrix_slide(self, presentation_id, slide_id, title,
                               phases, rows, weights=None, brand=None,
                               footer=None, page=None, eyebrow=None,
                               subhead=None, source=None):
        """``phases`` as ``weights``-sized columns beside the ``rows`` label
        rail, past :data:`PHASE_MATRIX_MAX` continuing with the labels."""
        phases = list(phases or [])
        rows = [(str(label), list(cells or [])) for label, cells in rows or []]
        weights = list(weights or [])
        limit = self.PHASE_MATRIX_MAX
        if len(phases) > limit:
            for n, start in enumerate(range(0, len(phases), limit)):
                self.add_phase_matrix_slide(
                    presentation_id, *self._part(slide_id, title, n),
                    phases[start:start + limit],
                    [(label, cells[start:start + limit])
                     for label, cells in rows],
                    weights[start:start + limit] or None, brand, footer,
                    page, eyebrow, subhead=subhead, source=source)
            return slide_id
        colors = self._brand_colors(brand)
        reqs = [self._blank_slide_req(slide_id), *self._content_chrome_reqs(
            slide_id, title, colors, footer, page, eyebrow, subhead, source)]
        if phases and rows:
            reqs += self._phase_table_reqs(slide_id, phases, rows, weights,
                                           colors)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    LOGO_GRID_COLS = 5
    LOGO_GRID_GAP_EMU = 137160
    LOGO_GROUP_PAD_EMU = 100000
    LOGO_GROUP_LABEL_H_EMU = 240000
    LOGO_ITEM_H_EMU = 457200
    LOGO_PILL_H_EMU = 260000
    LOGO_PILL_PT = 10
    LOGO_ITEM_GAP_EMU = 45720

    def _logo_item_h(self, item):
        """A grid item's height: a logo plate, or a wordmark pill."""
        return (self.LOGO_ITEM_H_EMU if item.get('logo_url')
                else self.LOGO_PILL_H_EMU)

    def _logo_group_h(self, group):
        """A group card's natural height around its items."""
        items = group['items']
        return (2 * self.LOGO_GROUP_PAD_EMU + self.LOGO_GROUP_LABEL_H_EMU
                + sum(self._logo_item_h(item) for item in items)
                + self.LOGO_ITEM_GAP_EMU * (len(items) - 1))

    def _logo_group_cap(self, group, avail_h):
        """``group`` with the items that stack in ``avail_h``, the last
        seat a "+N more" pill when some do not."""
        items = list(group['items'])
        kept, _ = _leading_fit(
            items, [self._logo_item_h(item) + self.LOGO_ITEM_GAP_EMU
                    for item in items], avail_h)
        if len(kept) < len(items):
            kept[-1] = {'name': f'+{len(items) - len(kept) + 1} more'}
        return dict(group, items=kept)

    def _logo_group_reqs(self, slide_id, base, group, x, y, w, h, colors):
        """One group card: its label, then each item as a logo plate or
        a wordmark pill, stacked."""
        pad = self.LOGO_GROUP_PAD_EMU
        reqs = self._card_reqs(slide_id, f'{base}r', x, y, w, h, colors)
        inner_x, inner_w = x + pad, w - 2 * pad
        reqs += self._text_box_reqs(
            slide_id, f'{base}l', str(group.get('label', '')).upper(),
            inner_x, y + pad, inner_w, self.LOGO_GROUP_LABEL_H_EMU,
            font_pt=8, bold=True, color=colors['muted'],
            font=self.eyebrow_font)
        item_y = y + pad + self.LOGO_GROUP_LABEL_H_EMU
        clear = self.LOGO_PAD_EMU
        for i, item in enumerate(group['items']):
            if item.get('logo_url'):
                reqs += self._logo_plate_reqs(
                    slide_id, f'{base}i{i}', item['logo_url'],
                    inner_x + clear, item_y + clear, inner_w - 2 * clear,
                    self.LOGO_ITEM_H_EMU - 2 * clear, item.get('img_w'),
                    item.get('img_h'))
            else:
                name = _clip_lines(item.get('name', ''), inner_w,
                                   self.LOGO_PILL_PT, 1, PROSE_GLYPH_RATIO)
                reqs += self._pill_reqs(
                    slide_id, f'{base}p{i}', f'{base}w{i}', name,
                    (inner_x, item_y, inner_w, self.LOGO_PILL_H_EMU),
                    self.LOGO_PILL_PT, colors)
            item_y += self._logo_item_h(item) + self.LOGO_ITEM_GAP_EMU
        return reqs

    def add_logo_grid_slide(self, presentation_id, slide_id, title, groups,
                            brand=None, footer=None, page=None,
                            eyebrow=None, subhead=None, source=None):
        """``groups`` as cards up to five across, each item a logo plate or
        a wordmark pill, the rows continuing past the body."""
        groups = [group for group in groups or [] if group.get('items')]
        colors = self._brand_colors(brand)
        cx, top, cw, body_h = self.body_box(self.is_dark)
        gap, pad = self.LOGO_GRID_GAP_EMU, self.LOGO_GROUP_PAD_EMU
        cols = min(self.LOGO_GRID_COLS, max(1, len(groups)))
        group_w = (cw - gap * (cols - 1)) // cols
        avail = body_h - 2 * pad - self.LOGO_GROUP_LABEL_H_EMU
        groups = [self._logo_group_cap(group, avail) for group in groups]
        bands = [groups[i:i + cols] for i in range(0, len(groups), cols)]
        band_h = [max(self._logo_group_h(group) for group in band)
                  for band in bands]
        slides = _fit_rows(bands, [h + gap for h in band_h], body_h + gap)
        if len(slides) > 1:
            for n, chunk in enumerate(slides):
                self.add_logo_grid_slide(
                    presentation_id, *self._part(slide_id, title, n),
                    [group for band in chunk for group in band], brand,
                    footer, page, eyebrow, subhead=subhead, source=source)
            return slide_id
        reqs = [self._blank_slide_req(slide_id), *self._content_chrome_reqs(
            slide_id, title, colors, footer, page, eyebrow, subhead, source)]
        y = top
        for r, band in enumerate(bands):
            for c, group in enumerate(band):
                reqs += self._logo_group_reqs(
                    slide_id, f'{slide_id}g{r * cols + c}', group,
                    cx + c * (group_w + gap), y, group_w, band_h[r], colors)
            y += band_h[r] + gap
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    ROLE_TOTAL_H_EMU = 520000
    ROLE_TOTAL_PT = 24
    ROLE_VALUE_PT = (32, 28, 24, 20, 18, 16)

    def _role_card_reqs(self, slide_id, base, role, x, y, w, h, colors):
        """One role card: the toned head over one to three stat tiles
        sharing a value size."""
        gap, pad = 137160, 100000
        reqs = self._card_reqs(slide_id, f'{base}r', x, y, w, h, colors)
        head_reqs, head_h = self._card_head_reqs(
            slide_id, f'{base}h', role.get('head', ''), x + pad, y + pad,
            w - 2 * pad, colors, role.get('tone'))
        reqs += head_reqs
        cards = list(role['cards'])[:3]
        tile_y = y + pad + head_h + 20000
        tile_h = max(y + h - pad - tile_y, TILE_PAD_EMU)
        tile_w = (w - 2 * pad - gap * (len(cards) - 1)) // len(cards)
        text_w = tile_w - 2 * TILE_PAD_EMU
        blocks = [_tile_block_heights(card, text_w) for card in cards]
        spare = tile_h - 2 * TILE_PAD_EMU - max(sum(block) for block in blocks)
        value_pt, value_h = self._value_size(cards, text_w,
                                             self.ROLE_VALUE_PT, spare)
        tile_colors = self._nested_colors(colors)
        for j, card in enumerate(cards):
            reqs += self._tile_reqs(
                slide_id, f'{base}c{j}', card, x + pad + j * (tile_w + gap),
                tile_y, tile_w, tile_h, tile_colors, TILE_PAD_EMU,
                (value_pt, *TILE_TEXT_PT), (value_h, *blocks[j]))
        return reqs

    def _total_bar_reqs(self, slide_id, total, x, y, w, colors):
        """The full-width bar under the role cards: an accent value over
        its label."""
        reqs = self._card_reqs(slide_id, f'{slide_id}tr', x, y, w,
                               self.ROLE_TOTAL_H_EMU, colors)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}tv', str(total.get('value') or ''),
            x + 100000, y + 20000, w - 200000, 330000,
            font_pt=self.ROLE_TOTAL_PT, bold=True, color=colors['accent'])
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}tl', str(total.get('label') or ''),
            x + 100000, y + 330000, w - 200000, 190000, font_pt=11,
            color=colors['muted'])
        return reqs

    def add_role_cards_slide(self, presentation_id, slide_id, title, roles,
                             total=None, brand=None, footer=None, page=None,
                             eyebrow=None, subhead=None, source=None):
        """Up to four ``roles``, each a card with its toned head over one
        to three stat tiles, over a full-width ``total`` bar."""
        roles = [role for role in list(roles or [])[:4] if role.get('cards')]
        colors = self._brand_colors(brand)
        cx, top, cw, body_h = self.body_box(self.is_dark)
        reqs = [self._blank_slide_req(slide_id), *self._content_chrome_reqs(
            slide_id, title, colors, footer, page, eyebrow, subhead, source)]
        gap = 137160
        if roles:
            role_h = body_h - (self.ROLE_TOTAL_H_EMU + gap if total else 0)
            role_w = (cw - gap * (len(roles) - 1)) // len(roles)
            for i, role in enumerate(roles):
                reqs += self._role_card_reqs(
                    slide_id, f'{slide_id}k{i}', role,
                    cx + i * (role_w + gap), top, role_w, role_h, colors)
        if total:
            reqs += self._total_bar_reqs(
                slide_id, total, cx, top + body_h - self.ROLE_TOTAL_H_EMU,
                cw, colors)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    NUMBERED_GUTTER_EMU = 760000
    NUMBERED_NUMERAL_PT = 28
    NUMBERED_HEAD_PT = 14
    NUMBERED_HEAD_LINES = 2
    NUMBERED_BODY_PT = (14, 13, 12)
    NUMBERED_GAP_EMU = 160000

    def _numbered_head(self, item, width):
        """An item's head cut to the lines its row gives it."""
        return _clip_lines(item.get('head', ''), width,
                           self.NUMBERED_HEAD_PT, self.NUMBERED_HEAD_LINES)

    def _numbered_fit(self, items, width, budget):
        """``(pt, head heights, row heights)`` at the largest body step
        whose rows, none shorter than its numeral, stand in ``budget``."""
        numeral_h = int(_line_pitch(self.NUMBERED_NUMERAL_PT)) + TEXT_INSET_EMU
        head_hs = [_block_height(self._numbered_head(item, width), width,
                                 self.NUMBERED_HEAD_PT) for item in items]
        for font_pt in self.NUMBERED_BODY_PT:
            row_hs = [max(head_h + _block_height(
                str(item.get('body') or ''), width, font_pt,
                self.NARRATIVE_SPACING, self.NARRATIVE_SPACE_BELOW_PT,
                PROSE_GLYPH_RATIO), numeral_h) + self.NUMBERED_GAP_EMU
                for item, head_h in zip(items, head_hs)]
            if sum(row_hs) <= budget:
                break
        return font_pt, head_hs, row_hs

    def add_numbered_slide(self, presentation_id, slide_id, title, items,
                           first=1, brand=None, footer=None, page=None,
                           eyebrow=None, subhead=None, source=None):
        """Numbered items (``[{'head', 'body'}]``) down the body with
        hairline rules between, continuing with the numbering run on."""
        items = [item for item in items or []
                 if item.get('head') or item.get('body')]
        colors = self._brand_colors(brand)
        cx, top, cw, body_h = self.body_box(self.is_dark)
        text_x = cx + self.NUMBERED_GUTTER_EMU
        text_w = cw - self.NUMBERED_GUTTER_EMU
        font_pt, head_hs, row_hs = self._numbered_fit(items, text_w, body_h)
        groups = _fit_rows(items, row_hs, body_h)
        if len(groups) > 1:
            start = first
            for n, group in enumerate(groups):
                self.add_numbered_slide(
                    presentation_id, *self._part(slide_id, title, n), group,
                    start, brand, footer, page, eyebrow, subhead=subhead,
                    source=source)
                start += len(group)
            return slide_id
        reqs = [self._blank_slide_req(slide_id), *self._content_chrome_reqs(
            slide_id, title, colors, footer, page, eyebrow, subhead, source)]
        rule = self.DECK_RULE_DARK if self.is_dark else self.DECK_RULE
        y = top
        for i, (item, head_h, row_h) in enumerate(zip(items, head_hs,
                                                      row_hs)):
            row_h = min(row_h, top + body_h - y)
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}n{i}', f'{first + i:02d}', cx, y,
                self.NUMBERED_GUTTER_EMU, row_h - self.NUMBERED_GAP_EMU // 2,
                font_pt=self.NUMBERED_NUMERAL_PT, bold=True,
                color=colors['accent'])
            head = self._numbered_head(item, text_w)
            if head:
                reqs += self._text_box_reqs(
                    slide_id, f'{slide_id}h{i}', head, text_x, y, text_w,
                    head_h, font_pt=self.NUMBERED_HEAD_PT, bold=True,
                    color=colors['ink'])
            body = str(item.get('body') or '')
            box_h = row_h - self.NUMBERED_GAP_EMU - head_h
            if body and box_h > TEXT_INSET_EMU:
                reqs += self._prose_reqs(
                    slide_id, f'{slide_id}b{i}', body,
                    (text_x, y + head_h, text_w, box_h), colors, (font_pt,),
                    'numbered')
            if i < len(items) - 1:
                reqs += self._rect_reqs(
                    slide_id, f'{slide_id}r{i}', cx,
                    y + row_h - self.NUMBERED_GAP_EMU // 2, cw, EMU_PER_PT,
                    rule)
            y += row_h
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    GALLERY_MAX = 4
    GALLERY_GAP_EMU = 137160
    GALLERY_CAPTION_H_EMU = 260000
    GALLERY_CAPTION_PT = 11

    def _gallery_cell_reqs(self, slide_id, index, image, box, colors):
        """One picture fitted into ``box``, keeping its aspect, over its
        caption cut to one line."""
        x, y, w, h = box
        caption = str(image.get('caption') or '')
        cap_h = self.GALLERY_CAPTION_H_EMU if caption else 0
        fit = self._fit_box(image.get('img_w'), image.get('img_h'), x, y, w,
                            h - cap_h)
        reqs = self._image_reqs(slide_id, f'{slide_id}g{index}',
                                image['url'], *fit, outline=True)
        if not caption:
            return reqs
        caption = _clip_lines(caption, w, self.GALLERY_CAPTION_PT, 1,
                              PROSE_GLYPH_RATIO)
        return reqs + self._text_box_reqs(
            slide_id, f'{slide_id}gc{index}', caption, x, y + h - cap_h, w,
            cap_h, font_pt=self.GALLERY_CAPTION_PT, align='CENTER',
            color=colors['muted'])

    def add_gallery_slide(self, presentation_id, slide_id, title, images,
                          brand=None, footer=None, page=None, eyebrow=None,
                          subhead=None, source=None):
        """Pictures fitted into a grid, four in two rows unless most are
        portrait; more than :data:`GALLERY_MAX` continue."""
        images = [image for image in images or [] if image.get('url')]
        limit = self.GALLERY_MAX
        if len(images) > limit:
            for n, start in enumerate(range(0, len(images), limit)):
                self.add_gallery_slide(
                    presentation_id, *self._part(slide_id, title, n),
                    images[start:start + limit], brand, footer, page,
                    eyebrow, subhead=subhead, source=source)
            return slide_id
        colors = self._brand_colors(brand)
        cx, top, cw, body_h = self.body_box(self.is_dark)
        reqs = [self._blank_slide_req(slide_id), *self._content_chrome_reqs(
            slide_id, title, colors, footer, page, eyebrow, subhead, source)]
        count, gap = len(images), self.GALLERY_GAP_EMU
        if count:
            portrait = sum(1 for image in images
                           if (image.get('img_h') or 0)
                           > (image.get('img_w') or 0))
            cols = count if count < 4 or portrait * 2 > count else 2
            n_rows = (count + cols - 1) // cols
            cell_w = (cw - gap * (cols - 1)) // cols
            cell_h = (body_h - gap * (n_rows - 1)) // n_rows
            for i, image in enumerate(images):
                r, c = divmod(i, cols)
                reqs += self._gallery_cell_reqs(
                    slide_id, i, image,
                    (cx + c * (cell_w + gap), top + r * (cell_h + gap),
                     cell_w, cell_h), colors)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def delete_non_report_slides(self, presentation_id, keep_prefix='rb'):
        """Drop the blank slide Google auto-creates with a new deck so the
        report's title slide leads. Report IDs use ``keep_prefix``."""
        pres = self.get_presentation(presentation_id)
        for slide in pres.get('slides', []):
            oid = slide.get('objectId')
            if oid and not str(oid).startswith(keep_prefix):
                self.slides_batch_update(
                    presentation_id, [{'deleteObject': {'objectId': oid}}])

    def add_notes_batch(self, presentation_id, notes_by_slide):
        """Insert speaker notes for many slides in ONE round-trip: read the
        deck once to resolve each slide's speaker-notes shape, then batch the
        inserts. Avoids the per-slide poll+sleep of ``add_speaker_notes``."""
        if not notes_by_slide:
            return None
        pres = self.get_presentation(presentation_id)
        requests = []
        for number, slide in enumerate(pres.get('slides', []), start=1):
            sid = slide.get('objectId')
            page_id = str(sid) + 'pg'
            if any(e.get('objectId') == page_id
                   for e in slide.get('pageElements', [])):
                requests += [
                    {'deleteText': {'objectId': page_id,
                                    'textRange': {'type': 'ALL'}}},
                    {'insertText': {'objectId': page_id,
                                    'insertionIndex': 0, 'text': str(number)}}]
            text = notes_by_slide.get(sid)
            if not text:
                continue
            try:
                notes_id = (slide['slideProperties']['notesPage']
                            ['notesProperties']['speakerNotesObjectId'])
            except (KeyError, TypeError):
                continue
            requests.append({'insertText': {
                'objectId': notes_id, 'insertionIndex': 0, 'text': text}})
        return self.slides_batch_update(presentation_id, requests)

    @staticmethod
    def get_s3_image_url_from_obj(s3, img_obj, img_name, folder='images'):
        if not s3:
            s3 = awss3.S3()
            s3.input_config()
        embedded_obj = img_obj['inlineObjectProperties']['embeddedObject']
        img_url = embedded_obj.get('imageProperties', {}).get('contentUri')
        key = '{}/{}'.format(folder, img_name) if folder else img_name
        try:
            response = requests.get(img_url, stream=True, timeout=10)
            if response.status_code != 200:
                logging.warning('Failed to download image %s (status %s)',
                                img_url, response.status_code)
            else:
                img_url = s3.s3_upload_file_obj(response.raw, key)
        except Exception as e:
            logging.warning('Error uploading image {}: {}'.format(img_url, e))
        return img_url

    def parse_google_doc(self, r):
        if self.body_str not in r:
            logging.warning('Body not in response {}.'.format(r))
            return pd.DataFrame()
        inline_objects = r.get('inlineObjects', {})
        r = r[self.body_str][self.cont_str]
        paragraph = []
        new_paragraph = {}
        text_run = 'textRun'
        inline_elem = 'inlineObjectElement'
        for x in r:
            if self.para_str not in x:
                continue
            tc = x[self.para_str]['elements'][0]
            if text_run in tc:
                tc = tc[text_run][self.cont_str]
                if tc == '\n':
                    paragraph.append(new_paragraph)
                    new_paragraph = {}
                    continue
                else:
                    if new_paragraph:
                        new_paragraph[self.cont_str] += tc
                    else:
                        new_paragraph[self.head_str] = tc.strip('\n')
                        new_paragraph[self.cont_str] = ''
                        new_paragraph[self.img_str] = ''
            elif inline_elem in tc:
                if not new_paragraph:
                    new_paragraph[self.head_str] = ''
                    new_paragraph[self.cont_str] = ''
                inline_obj_id = tc[inline_elem]['inlineObjectId']
                new_paragraph[self.img_str] = inline_obj_id
        # Upload images to S3 and append image URLs to the data
        s3 = awss3.S3()
        s3.input_config()
        for item in paragraph:
            if self.img_str not in item or not item[self.img_str]:
                continue
            object_id = item[self.img_str]
            obj = inline_objects[object_id]
            img_name = item['header']
            img_name = "".join(c for c in img_name if c.isalnum() or c == " ")
            img_name = img_name.replace('  ', ' ').replace(' ', '_')
            img_name = "{}.png".format(img_name)
            img_url = self.get_s3_image_url_from_obj(s3, obj, img_name)
            item[self.img_str] = img_url
        self.df = pd.DataFrame(paragraph)
        return self.df

    def create_google_doc(self, title=None):
        logging.info('Creating Google Doc: {}'.format(title))
        body = {
            "title": title,
        }
        response = self.client.post(url=self.docs_url, json=body)
        response = response.json()
        doc_id = response["documentId"]
        self.add_permissions(doc_id)
        return doc_id

    @staticmethod
    def get_format_req(start_ind=1, end_ind=1, style=text_format):
        format_req = {
            "updateParagraphStyle": {
                "range": {
                    "startIndex": start_ind,
                    "endIndex": end_ind
                },
                "paragraphStyle": {
                    "namedStyleType": style
                },
                "fields": "namedStyleType"
            }
        }
        return format_req

    null_cell_text = ('none', 'nan', 'nat', 'null')

    @staticmethod
    def utf16_len(text):
        """Length of ``text`` in UTF-16 code units — the unit every Docs
        API index counts. Python ``len`` undercounts astral chars
        (emoji), shifting every later range in the batch."""
        return len(text.encode('utf-16-le')) // 2

    @staticmethod
    def cell_text(cell):
        """One table cell as document text — missing values read blank
        rather than as the literal 'None'/'nan' a client must never see
        in a report."""
        try:
            blank = cell is None or bool(pd.isna(cell))
        except (TypeError, ValueError):  # arrays/lists aren't null-testable
            blank = False
        if blank:
            return ''
        text = str(cell).strip()
        return '' if text.lower() in GsApi.null_cell_text else text

    @staticmethod
    def pt(magnitude):
        """A Docs API dimension in points."""
        return {'magnitude': magnitude, 'unit': 'PT'}

    @staticmethod
    def _color(rgb01):
        return {'color': {'rgbColor': rgb01}}

    @classmethod
    def text_style_req(cls, start, end, style):
        """An ``updateTextStyle`` request from a friendly style dict:
        ``bold`` / ``italic`` / ``size`` (pt) / ``font`` / ``weight`` /
        ``color`` (rgb01). ``None`` for an empty style or range."""
        ts, fields = {}, []
        if style.get('bold') is not None:
            ts['bold'] = bool(style['bold'])
            fields.append('bold')
        if style.get('italic') is not None:
            ts['italic'] = bool(style['italic'])
            fields.append('italic')
        if style.get('size'):
            ts['fontSize'] = cls.pt(style['size'])
            fields.append('fontSize')
        if style.get('font'):
            ts['weightedFontFamily'] = {'fontFamily': style['font'],
                                        'weight': style.get('weight', 400)}
            fields.append('weightedFontFamily')
        if style.get('color'):
            ts['foregroundColor'] = cls._color(style['color'])
            fields.append('foregroundColor')
        if not fields or end <= start:
            return None
        return {'updateTextStyle': {
            'range': {'startIndex': start, 'endIndex': end},
            'textStyle': ts, 'fields': ','.join(fields)}}

    @classmethod
    def paragraph_style_req(cls, start, end, style):
        """An ``updateParagraphStyle`` request from a friendly style dict:
        ``alignment`` / ``space_above`` / ``space_below`` / ``indent_start``
        (pt) / ``line_spacing`` (%) / ``keep_with_next`` / ``keep_together``
        / ``page_break_before`` / ``shading`` (rgb01) / ``border_bottom``
        (``(rgb01, width_pt, padding_pt)``). ``None`` for an empty style."""
        ps, fields = {}, []
        simple = (('alignment', 'alignment'),
                  ('keep_with_next', 'keepWithNext'),
                  ('keep_together', 'keepLinesTogether'),
                  ('page_break_before', 'pageBreakBefore'),
                  ('line_spacing', 'lineSpacing'))
        for key, api_key in simple:
            if style.get(key) is not None:
                ps[api_key] = style[key]
                fields.append(api_key)
        for key, api_key in (('space_above', 'spaceAbove'),
                             ('space_below', 'spaceBelow'),
                             ('indent_start', 'indentStart')):
            if style.get(key) is not None:
                ps[api_key] = cls.pt(style[key])
                fields.append(api_key)
        if style.get('shading'):
            ps['shading'] = {'backgroundColor': cls._color(style['shading'])}
            fields.append('shading')
        if style.get('border_bottom'):
            color, width, padding = style['border_bottom']
            ps['borderBottom'] = {'color': cls._color(color),
                                  'width': cls.pt(width),
                                  'padding': cls.pt(padding),
                                  'dashStyle': 'SOLID'}
            fields.append('borderBottom')
        if not fields:
            return None
        return {'updateParagraphStyle': {
            'range': {'startIndex': start, 'endIndex': end},
            'paragraphStyle': ps, 'fields': ','.join(fields)}}

    @staticmethod
    def document_style_req(margins_pt):
        """``updateDocumentStyle`` for page margins —
        ``(top, right, bottom, left)`` in points."""
        top, right, bottom, left = margins_pt
        return {'updateDocumentStyle': {
            'documentStyle': {
                'marginTop': GsApi.pt(top), 'marginRight': GsApi.pt(right),
                'marginBottom': GsApi.pt(bottom),
                'marginLeft': GsApi.pt(left)},
            'fields': 'marginTop,marginRight,marginBottom,marginLeft'}}

    @staticmethod
    def _cell_paragraphs(cell):
        """A cell spec as ``[(text, text_style, para_style)]``; a bare
        value is one unstyled paragraph."""
        if isinstance(cell, (list, tuple)) and cell and \
                isinstance(cell[0], (list, tuple)):
            return [(GsApi.cell_text(p[0]),
                     p[1] if len(p) > 1 and p[1] else {},
                     p[2] if len(p) > 2 and p[2] else {}) for p in cell]
        return [(GsApi.cell_text(cell), {}, {})]

    @staticmethod
    def _cell_style_req(table_start, row, col, row_span, col_span, style,
                        fields):
        return {'updateTableCellStyle': {
            'tableCellStyle': style, 'fields': fields,
            'tableRange': {
                'tableCellLocation': {
                    'tableStartLocation': {'index': table_start},
                    'rowIndex': row, 'columnIndex': col},
                'rowSpan': row_span, 'columnSpan': col_span}}}

    @classmethod
    def _cell_border(cls, rgb01, width_pt):
        return {'color': cls._color(rgb01), 'width': cls.pt(width_pt),
                'dashStyle': 'SOLID'}

    def table_requests(self, cells, index, style=None):
        """Requests rendering ``cells`` as a native, styled Docs table,
        plus the index one past it (the table element itself starts one
        past ``index`` — the API writes a leading newline first).

        :param cells: rows of cell specs — a scalar (one paragraph, blank
            for null-ish values) or a list of ``(text, text_style,
            para_style)`` paragraphs.
        :param index: the end of the segment, where the table appends.
        :param style: optional dict — ``header`` (``header_bg`` fill +
            bold ``header_fg`` text on row 0), ``zebra`` (rgb01 on
            alternate body rows), ``borders`` (``'rules'`` for a
            ``rule``-colored hairline under each row, ``'none'``, a
            per-side ``{'top': (rgb01, width_pt)}`` dict, or the default
            grid), ``padding`` (pt), ``font`` / ``font_size`` / ``color``,
            ``numeric_align``, ``col_widths`` (pt per column),
            ``cell_bg``, ``top_borders`` (per-column ``(rgb01,
            width_pt)`` or ``None`` on row 0).
        :returns: ``(requests, index)``; ``([], index)`` for no rows.
        """
        style = style or {}
        rows = self._rectangular_cells(cells)
        if not rows:
            return [], index
        ncols = len(rows[0])
        start_ind, table_start = index, index + 1
        reqs = [{'insertTable': {
            'rows': len(rows), 'columns': ncols,
            'endOfSegmentLocation': {'segmentId': ''}}}]
        text_reqs, grid, index = self._lay_out_cells(rows, index + 4)
        end_ind = index - 1
        base_text = {api: style[key] for key, api in (
            ('font', 'font'), ('font_size', 'size'), ('color', 'color'))
            if style.get(key)}
        style_reqs = [
            self.get_format_req(start_ind, end_ind, self.text_format),
            self.paragraph_style_req(table_start, end_ind, {
                'line_spacing': 100, 'space_above': 0, 'space_below': 0}),
            self.text_style_req(table_start, end_ind, base_text)]
        style_reqs += self._table_frame_style_reqs(
            table_start, len(rows), ncols, style)
        style_reqs += self._table_content_style_reqs(grid, ncols, style)
        return (reqs + text_reqs + [r for r in style_reqs if r]), end_ind

    @classmethod
    def _rectangular_cells(cls, cells):
        """``cells`` as equal-length rows of paragraph lists — the Docs
        API only inserts rectangular tables. Empty for no rows."""
        rows = [[cls._cell_paragraphs(c) for c in row] for row in cells]
        rows = [r for r in rows if r]
        if not rows:
            return []
        ncols = max(len(r) for r in rows)
        for row in rows:
            row.extend([[('', {}, {})]] * (ncols - len(row)))
        return rows

    def _lay_out_cells(self, rows, index):
        """``(insert_requests, grid, index)`` — every cell's text at the
        index the API gives it: a cell costs its text plus two, a row one
        more."""
        reqs, grid = [], []
        for row in rows:
            row_cells = []
            for paragraphs in row:
                text = '\n'.join(p[0] for p in paragraphs)
                cell_start, pos, para_ranges = index, index, []
                if text:
                    reqs.append({'insertText': {
                        'text': text, 'location': {'index': index}}})
                for ptext, _, _ in paragraphs:
                    para_ranges.append((pos, pos + self.utf16_len(ptext)))
                    pos += self.utf16_len(ptext) + 1
                index += self.utf16_len(text) + 2
                row_cells.append(TableCell(cell_start, index - 1,
                                           para_ranges, paragraphs))
            index += 1
            grid.append(row_cells)
        return reqs, grid, index

    @classmethod
    def _table_borders(cls, style, paper):
        """``{borderSide: border}`` for ``borders`` (a per-side dict,
        ``'rules'`` or ``'none'``), empty for the default grid; a hidden
        side is drawn paper-colored at zero width."""
        borders = style.get('borders')
        sides = ('Top', 'Left', 'Right', 'Bottom')
        if isinstance(borders, dict):
            return {f'border{side}': cls._cell_border(
                *(borders.get(side.lower()) or (paper, 0)))
                for side in sides}
        if borders not in ('rules', 'none'):
            return {}
        rule = style.get('rule') or paper
        under = 'Bottom' if borders == 'rules' else ''
        return {f'border{side}': cls._cell_border(
            rule if side == under else paper, 0.5 if side == under else 0)
            for side in sides}

    @classmethod
    def _table_frame_style_reqs(cls, table_start, nrows, ncols, style):
        """The table's own dressing — padding, borders, header fill,
        zebra body rows, per-column top rules and fixed widths."""
        paper = style.get('paper') or {'red': 1, 'green': 1, 'blue': 1}
        cell_style = dict(cls._table_borders(style, paper))
        pad = style.get('padding')
        if pad is not None:
            for side in ('Top', 'Bottom', 'Left', 'Right'):
                cell_style[f'padding{side}'] = cls.pt(pad)
        if style.get('cell_bg'):
            cell_style['backgroundColor'] = cls._color(style['cell_bg'])
        reqs = []
        if cell_style:
            reqs.append(cls._cell_style_req(
                table_start, 0, 0, nrows, ncols, cell_style,
                ','.join(sorted(cell_style))))
        header = bool(style.get('header'))
        if header and style.get('header_bg'):
            reqs.append(cls._cell_style_req(
                table_start, 0, 0, 1, ncols,
                {'backgroundColor': cls._color(style['header_bg'])},
                'backgroundColor'))
        if style.get('zebra'):
            for row in range((1 if header else 0) + 1, nrows, 2):
                reqs.append(cls._cell_style_req(
                    table_start, row, 0, 1, ncols,
                    {'backgroundColor': cls._color(style['zebra'])},
                    'backgroundColor'))
        for col, top in enumerate(style.get('top_borders') or []):
            if top and col < ncols:
                reqs.append(cls._cell_style_req(
                    table_start, 0, col, 1, 1,
                    {'borderTop': cls._cell_border(*top)}, 'borderTop'))
        for col, width in enumerate(style.get('col_widths') or []):
            if width and col < ncols:
                reqs.append({'updateTableColumnProperties': {
                    'tableStartLocation': {'index': table_start},
                    'columnIndices': [col],
                    'tableColumnProperties': {
                        'widthType': 'FIXED_WIDTH', 'width': cls.pt(width)},
                    'fields': 'widthType,width'}})
        return reqs

    @classmethod
    def _numeric_columns(cls, grid, ncols, header):
        """The column indices whose every filled body cell reads as a
        number — the ones ``numeric_align`` right-aligns."""
        body = grid[1:] if header else grid
        out = set()
        for col in range(ncols):
            filled = [row[col].paragraphs[0][0] for row in body
                      if row[col].paragraphs and row[col].paragraphs[0][0]]
            if filled and all(cls._NUMERIC_CELL_RE.match(t) for t in filled):
                out.add(col)
        return out

    @classmethod
    def _table_content_style_reqs(cls, grid, ncols, style):
        """Per-cell styling: the header row's bold contrast text,
        right-aligned numeric columns, and each cell paragraph's own
        text/paragraph style."""
        header = bool(style.get('header'))
        numeric = (cls._numeric_columns(grid, ncols, header)
                   if style.get('numeric_align') else set())
        reqs = []
        for row_idx, row_cells in enumerate(grid):
            for col, cell in enumerate(row_cells):
                if cell.end > cell.start:
                    if header and not row_idx and style.get('header_fg'):
                        reqs.append(cls.text_style_req(
                            cell.start, cell.end,
                            {'bold': True, 'color': style['header_fg']}))
                    if col in numeric:
                        reqs.append(cls.paragraph_style_req(
                            cell.start, cell.end, {'alignment': 'END'}))
                for (ps, pe), (_, tstyle, pstyle) in zip(cell.para_ranges,
                                                         cell.paragraphs):
                    if tstyle:
                        reqs.append(cls.text_style_req(ps, pe, tstyle))
                    if pstyle:
                        reqs.append(cls.paragraph_style_req(
                            ps, pe + 1, pstyle))
        return reqs

    def add_table(self, data, index, style=None):
        """Requests rendering ``data`` (row dicts, header from the first's
        keys) as a native Docs table plus the index one past it;
        ``([], index)`` for no rows, which must not abort the document.

        :param style: :meth:`table_requests` style; ``header`` is implied.
        """
        if not data:
            return [], index
        header = list(data[0].keys())
        cells = [header] + [[row.get(k) for k in header] for row in data]
        style = dict(style or {}, header=True)
        return self.table_requests(cells, index, style=style)

    def add_image_doc(self, presigned_url, index, width_pt=250,
                      height_pt=250):
        if not presigned_url:
            return [], index
        img_request = [{'insertInlineImage': {
            'location': {
                'index': index
            },
            'uri': presigned_url,
            'objectSize': {
                'height': {
                    'magnitude': height_pt,
                    'unit': 'PT'
                },
                'width': {
                    'magnitude': width_pt,
                    'unit': 'PT'
                }
            }
        }}, {
            'insertText': {
                'location': {
                    'index': index + 1,
                },
                'text': '\n'
            }
        }]
        index += 2
        return img_request, index

    def get_file_by_name(self, name):
        self.get_client()
        q = "name = '{}'".format(name)
        params = {'q': q}
        return self.client.get(self.files_url, params=params)

    def delete_file(self, file_id):
        url = self.files_url + '/{}'.format(file_id)
        self.client.delete(url)

    @staticmethod
    def image_size_pt(item):
        """``(width_pt, height_pt)`` for an item's inline chart image —
        the item's explicit size when it carries one, else page width
        scaled to the captured PNG's aspect ratio."""
        w_pt, h_pt = item.get('img_pt_w'), item.get('img_pt_h')
        if w_pt and h_pt:
            return w_pt, h_pt
        img_w, img_h = item.get('img_w'), item.get('img_h')
        if not (img_w and img_h):
            return 320, 200
        w_pt = 468  # US-Letter content width (8.5" - 1" of margins)
        return w_pt, max(120, min(600, int(round(w_pt * img_h / img_w))))

    @staticmethod
    def media_kind(item):
        """``'image'`` / ``'cells'`` / ``'table'`` / ``None`` — the
        trailing media an item carries; a missing url / cols / rows costs
        that item its media, never the document."""
        if item.get('cells'):
            return 'cells'
        data = item.get('data') or {}
        cols = data.get('cols') or []
        if not cols:
            return None
        if 'imgURI' in cols:
            return 'image' if item.get('url') else None
        return 'table' if data.get('data') else None

    def add_media_requests(self, item, index, table_style=None):
        """``(requests, index)`` for an item's trailing media — an inline
        chart image, a styled ``cells`` table, or a native table from
        ``data`` rows.

        :param table_style: brand table styling threaded to
            :meth:`add_table`, if any; an item's own ``table_style``
            wins.
        """
        kind = self.media_kind(item)
        if kind == 'image':
            w_pt, h_pt = self.image_size_pt(item)
            reqs, index = self.add_image_doc(item.get('url'), index,
                                             w_pt, h_pt)
            para = dict(item.get('image_para_style') or {})
            if item.get('image_align'):
                para['alignment'] = item['image_align']
            if reqs and para:
                reqs.append(self.paragraph_style_req(index - 2, index, para))
            return reqs, index
        style = item.get('table_style') or table_style
        if kind == 'cells':
            return self.table_requests(item['cells'], index, style=style)
        if kind == 'table':
            return self.add_table(item['data']['data'], index=index,
                                  style=style)
        return [], index

    def get_document(self, doc_id):
        """The Docs document resource, for read-back verification."""
        return self.client.get('{}/{}'.format(self.docs_url, doc_id)).json()

    def document_is_empty(self, doc_id):
        """Whether the document has no content at all — the state a
        rejected batchUpdate leaves behind.

        A newly created doc holds one empty paragraph, so any real text,
        table or inline image proves the body landed. Read-back is the
        one check that catches a blank export regardless of *which*
        request Google refused.
        """
        doc = self.get_document(doc_id)
        if doc.get('inlineObjects'):
            return False
        for el in doc.get(self.body_str, {}).get(self.cont_str, []):
            if el.get('table'):
                return False
            for run in el.get(self.para_str, {}).get('elements', []):
                if run.get('textRun', {}).get(self.cont_str, '').strip():
                    return False
        return True

    @classmethod
    def _item_text_style_reqs(cls, item, start, end):
        """Style requests for one item's inserted text, from the optional
        keys the report exporter stamps: ``alignment`` / ``text_color`` /
        ``italic``, plus ``para_style`` and ``text_style`` friendly dicts.
        Empty when the item carries none."""
        para = dict(item.get('para_style') or {})
        if item.get('alignment'):
            para['alignment'] = item['alignment']
        text = dict(item.get('text_style') or {})
        if item.get('text_color'):
            text['color'] = item['text_color']
        if item.get('italic'):
            text['italic'] = True
        reqs = [cls.paragraph_style_req(start, end, para) if para else None,
                cls.text_style_req(start, end, text) if text else None]
        return [r for r in reqs if r]

    @staticmethod
    def _rich_text_reqs(rich, start):
        """``(style_requests, bullet_requests)`` realizing a
        ``narrative_rich`` payload at its inserted UTF-16 position; bullets
        come back apart because creating them strips the nesting tabs, so
        the writer applies them last in reverse document order."""
        reqs, bullets = [], []
        for s, e in rich.get('bold_ranges') or []:
            reqs.append({'updateTextStyle': {
                'range': {'startIndex': start + s, 'endIndex': start + e},
                'textStyle': {'bold': True}, 'fields': 'bold'}})
        for s, e in rich.get('bullet_ranges') or []:
            bullets.append({'createParagraphBullets': {
                'range': {'startIndex': start + s, 'endIndex': start + e},
                'bulletPreset': rich.get('bullet_preset')
                or 'BULLET_DISC_CIRCLE_SQUARE'}})
        return reqs, bullets

    def doc_body_requests(self, text_json, index=1, newline=True,
                          text_only=False, table_style=None):
        """Build the batchUpdate requests for a report body.

        :param text_json: the report items.
        :param index: document index to start writing at.
        :param newline: end every item's text with a newline.
        :param text_only: plain text only — no media, no styling.
        :param table_style: default table styling for native tables.
        :returns: the request list.
        """
        request = []
        format_request = []
        bullet_request = []
        for item in text_json:
            if item.get('selected') == 'false' or 'message' not in item:
                continue
            rich = None if text_only else item.get('rich')
            media = None if text_only else self.media_kind(item)
            if item.get('page_break') and not text_only:
                request.append({'insertPageBreak': {
                    'location': {'index': index}}})
                index += 2
            text = rich['text'] if rich and rich.get('text') \
                else item['message']
            if text or not media:
                if newline and media not in ('table', 'cells'):
                    text = f'{text}\n'
                if text:
                    request.append({'insertText': {
                        'location': {'index': index}, 'text': text}})
                    style = item.get('format') or self.text_format
                    end_ind = index + self.utf16_len(text) - 1
                    if end_ind <= index:
                        end_ind = index + 1
                    format_request.append(
                        self.get_format_req(index, end_ind, style))
                    if not text_only:
                        format_request += self._item_text_style_reqs(
                            item, index, end_ind)
                        if rich:
                            styles, bullets = self._rich_text_reqs(rich,
                                                                   index)
                            format_request += styles
                            bullet_request += bullets
                    index += self.utf16_len(text)
            if media:
                media_req, index = self.add_media_requests(
                    item, index, table_style=table_style)
                request += media_req
        return request + format_request + bullet_request[::-1]

    def add_text(self, doc_id, text_json=None, index=1, newline=True,
                 text_only=False, table_style=None, doc_requests=None):
        """Write a report body into a Google Doc in one batchUpdate.

        :param doc_id: the target document.
        :param text_json: the report items (headings, text, tables,
            chart images), plus the optional styling keys
            :meth:`doc_body_requests` reads.
        :param index: document index to start writing at.
        :param newline: end every item's text with a newline.
        :param text_only: skip images, tables and styling — the degraded
            retry used when the full body is rejected, so a client still
            gets the narrative rather than a blank document.
        :param table_style: default styling for native tables.
        :param doc_requests: document-level requests (margins, see
            :meth:`document_style_req`) sent ahead of the body; dropped
            under ``text_only``.
        :returns: ``(response, body)``.
        """
        logging.info('Adding text to doc.')
        url = self.docs_url + "/" + doc_id + ":batchUpdate"
        headers = {"Content-Type": "application/json"}
        request = self.doc_body_requests(
            text_json, index=index, newline=newline, text_only=text_only,
            table_style=table_style)
        if request and doc_requests and not text_only:
            request = list(doc_requests) + request
        body = {"requests": request}
        response = self.client.post(url=url, json=body, headers=headers)
        return response, body

    def add_footer(self, doc_id, text, text_style=None, para_style=None):
        """Give the document a running footer reading ``text``, created
        in one call and filled in a second. Best-effort polish: a refusal
        returns False rather than failing an export whose body landed."""
        if not (text or '').strip():
            return False
        url = '{}/{}:batchUpdate'.format(self.docs_url, doc_id)
        headers = {"Content-Type": "application/json"}
        resp = self.client.post(url=url, headers=headers, json={
            'requests': [{'createFooter': {'type': 'DEFAULT'}}]})
        if not self.request_applied(resp, doc_id):
            return False
        try:
            footer_id = resp.json()['replies'][0]['createFooter']['footerId']
        except (ValueError, KeyError, IndexError, TypeError):
            logging.warning('Docs footer: no footerId in reply')
            return False
        end = self.utf16_len(text)
        reqs = [{'insertText': {
            'location': {'segmentId': footer_id, 'index': 0},
            'text': text}}]
        for req in (self.text_style_req(0, end, text_style or {}),
                    self.paragraph_style_req(0, end, para_style or {})):
            if req:
                key = next(iter(req))
                req[key]['range']['segmentId'] = footer_id
                reqs.append(req)
        resp = self.client.post(url=url, headers=headers,
                                json={'requests': reqs})
        return self.request_applied(resp, doc_id)

    def check_sheet_id(self, results, acc_col, success_msg, failure_msg):
        self.get_client()
        url = self.create_url()
        r = self.client.get(url)
        if (r.status_code == 200 and
                'values' in r.json()):
            row = [acc_col, ' '.join([success_msg, str(self.sheet_id)]),
                   True]
            results.append(row)
        else:
            msg = ('Permissions NOT Granted. '
                   'Double Check Sheet ID and Ensure Permissions were granted.'
                   '\n Error Msg:')
            try:
                r = r.json()
                error_msg = r['error']['message']
            except json.JSONDecodeError as e:
                error_msg = e
            row = [acc_col, ' '.join([failure_msg, msg, error_msg]), False]
            results.append(row)
        return results, r

    def test_connection(self, acc_col, camp_col=None, acc_pre=None):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        results, r = self.check_sheet_id(
            [], acc_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
