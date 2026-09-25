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

config_path = utl.config_path


EMU_PER_PT = 12700
TEXT_INSET_EMU = 100000
LINE_LEADING_PT = 3
GLYPH_WIDTH_RATIO = .6
MIN_ROW_EMU = 360000
TILE_PAD_EMU = 100000
TILE_VALUE_H_EMU = 450000
TILE_MIN_H_EMU = 1370000
TILE_TEXT_PT = (11, 9, 8)


def _tile_text_height(text, width, font_pt):
    """Reserve wrapped lines and Slides' text insets before the next block.

    Slides gives no metrics back, so the wrap is estimated from the
    face's average advance width. It errs tall: a box with slack under
    it reads fine, one the next block overlaps does not.
    """
    if not text:
        return 0
    chars = max(1, int((width - TEXT_INSET_EMU)
                       / (font_pt * EMU_PER_PT * GLYPH_WIDTH_RATIO)))
    lines = sum(max(1, len(textwrap.wrap(line, chars)))
                for line in str(text).split('\n'))
    return int(lines * (font_pt + LINE_LEADING_PT) * EMU_PER_PT
               + TEXT_INSET_EMU)


def _table_row_height(row, width, font_pt):
    """Reserve native cell wrapping, including Slides' cell insets."""
    cell_w = width / max(1, len(row))
    return max(MIN_ROW_EMU, max((_tile_text_height(value, cell_w, font_pt)
                                 for value in row), default=0))


def _tile_block_heights(tile, width):
    """``(label, caption, note)`` heights for one stat tile's text — read
    once to size the grid and again to place each block, so the two
    cannot drift apart and overlap."""
    return tuple(_tile_text_height(tile.get(key), width, font_pt)
                 for key, font_pt in zip(('label', 'caption', 'note'),
                                         TILE_TEXT_PT))


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
        self.client = None
        self.df = pd.DataFrame()
        self.r = None
        self.google_doc = False
        self.on_token_refresh = None
        self.deck_layouts = {}
        self.parse_response = self.parse_sheets_response

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading GS config file:{}'.format(config))
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
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.sheet_id = self.config['sheet_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url,
                            self.sheet_id]

    def load_config_dict(self, config):
        """Populate credentials from an in-memory dict, bypassing the
        CWD-relative config file load (used by the app-layer vault)."""
        self.config = config
        self.client_id = config['client_id']
        self.client_secret = config['client_secret']
        self.access_token = config['access_token']
        self.refresh_token = config['refresh_token']
        self.refresh_url = config['refresh_url']
        self.sheet_id = config.get('sheet_id', '')
        self.config_list = [self.config, self.client_id,
                            self.client_secret, self.refresh_token,
                            self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in GS config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def parse_fields(self, fields):
        if fields:
            for field in fields:
                if field == self.doc_str:
                    self.google_doc = True
                    self.parse_response = self.parse_google_doc

    def get_client(self):
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

    def create_presentation(self, presentation_name=None, template_id=None):
        """Create a deck from a template, falling back to blank if copying
        or layout detection fails."""
        logging.info('Creating GSlides Presentation: {}'.format(
            presentation_name))
        self.deck_layouts = {}
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
        """Copy the template and load recognized layouts; return None if
        unusable."""
        response = self.client.post(
            '{}/{}/copy'.format(self.files_url, template_id),
            params={'supportsAllDrives': 'true'}, json={'name': name})
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

    def _create_permission(self, file_id, body, params=None):
        """POST a single Drive permission and return the response.

        Shared by the domain-wide and per-user share helpers so the
        request + warning-log path lives in one place."""
        url = '{}/{}/permissions'.format(self.files_url, file_id)
        response = self.client.post(
            url=url, params=params or {}, json=body)
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
    CONTENT_TOP_EMU = 980000
    CONTENT_TOP_DARK_EMU = 1180000
    TEMPLATE_LAYOUT_STEM = 'CUSTOM_14'
    TITLE_BLOCK_Y_EMU = 3950000
    TITLE_BLOCK_W_EMU = 5900000
    SECTION_X_EMU = 1600200
    CAPTION_H_EMU = 340000
    CAPTION_GAP_EMU = 80000
    CAPTION_FOOT_EMU = 380000

    def _brand_colors(self, brand):
        """Use the client palette on light slides and light ink with its
        accent on dark slides."""
        brand = brand or {}
        if self.is_dark:
            return {'accent': brand.get('accent') or self.DECK_ACCENT_DARK,
                    'ink': self.DECK_WHITE,
                    'muted': self.DECK_MUTED_LIGHT,
                    'card': self.DECK_WHITE,
                    'card_alpha': self.DECK_CARD_ALPHA_DARK}
        return {'accent': brand.get('accent') or self.DECK_ACCENT,
                'ink': brand.get('ink') or self.DECK_INK,
                'muted': brand.get('muted') or self.DECK_MUTED,
                'card': brand.get('card') or self.DECK_CARD}

    def content_top(self):
        """Where a content slide's body starts, below the chrome."""
        return self.CONTENT_TOP_DARK_EMU if self.is_dark \
            else self.CONTENT_TOP_EMU

    def slides_batch_update(self, presentation_id, requests):
        """POST a Slides batchUpdate and record whether it applied."""
        if not requests:
            return None
        url = '{}/{}:batchUpdate'.format(self.slides_url, presentation_id)
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
                       font=None):
        ink = self.DECK_WHITE if self.is_dark else self.DECK_INK
        style = {'fontSize': {'magnitude': font_pt, 'unit': 'PT'},
                 'bold': bold, 'fontFamily': font or self.DECK_FONT,
                 'foregroundColor': {'opaqueColor': {
                     'rgbColor': color or ink}}}
        return [
            {'createShape': {
                'objectId': shape_id, 'shapeType': 'TEXT_BOX',
                'elementProperties': self._elem_props(slide_id, w, h, x, y)}},
            {'insertText': {
                'objectId': shape_id, 'insertionIndex': 0,
                'text': text or ''}},
            {'updateTextStyle': {
                'objectId': shape_id, 'style': style,
                'textRange': {'type': 'ALL'},
                'fields': 'fontSize,bold,fontFamily,foregroundColor'}},
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
        """Place title and logos on the template cover, optionally
        replacing its art."""
        reqs = [self._blank_slide_req(slide_id, 'title')]
        art = meta.get('image_url')
        reqs += self._art_field_reqs(slide_id, colors, art,
                                     meta.get('img_w'), meta.get('img_h'))
        if art and meta.get('house_logo_url'):
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}house', meta['house_logo_url'],
                (self.PAGE_W_EMU - self.LOGO_W_EMU) // 2, 1500000)
        block_x = self.PAGE_W_EMU - self.MARGIN_EMU - self.TITLE_BLOCK_W_EMU
        y = self.TITLE_BLOCK_Y_EMU
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', meta.get('title', ''),
            block_x, y, self.TITLE_BLOCK_W_EMU, 420000, font_pt=20,
            bold=True, align='END', color=self.DECK_WHITE)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}s', meta.get('subtitle', ''),
            block_x, y + 430000, self.TITLE_BLOCK_W_EMU, 300000, font_pt=12,
            align='END', color=self.DECK_MUTED_LIGHT)
        foot = ' · '.join(s for s in (meta.get('brand', ''),
                                      meta.get('legal', '')) if s)
        if foot:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}legal', foot, block_x, y + 750000,
                self.TITLE_BLOCK_W_EMU, 260000, font_pt=8, align='END',
                color=self.DECK_MUTED_LIGHT)
        if meta.get('logo_url'):
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}logo', meta['logo_url'],
                self.MARGIN_EMU + 91440,
                self.PAGE_H_EMU - self.LOGO_H_EMU - 500000)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_section_slide(self, presentation_id, slide_id, heading,
                          brand=None, eyebrow=None, note=None,
                          image_url=None, img_w=None, img_h=None):
        """Build a divider with optional eyebrow, note and background art."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id, 'section')]
        reqs += self._art_field_reqs(slide_id, colors, image_url,
                                     img_w, img_h)
        align = 'START' if self.is_dark else 'CENTER'
        if self.is_dark:
            cx = self.SECTION_X_EMU
            cw = self.PAGE_W_EMU - 2 * self.SECTION_X_EMU
        head_h = 1100000 if eyebrow else 700000
        head_y = (self.PAGE_H_EMU - head_h) // 2 - 137160
        if eyebrow:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}e', eyebrow, cx, head_y - 400000,
                cw, 340000, font_pt=12, bold=True, align=align,
                color=colors['accent'])
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', heading or '',
            cx, head_y, cw, head_h,
            font_pt=24 if eyebrow else 28, bold=True, align=align,
            color=self.DECK_WHITE)
        rule_w = 2057400
        rule_y = head_y + head_h + 91440
        if not self.is_dark:
            reqs += self._rect_reqs(
                slide_id, f'{slide_id}rule', (self.PAGE_W_EMU - rule_w) // 2,
                rule_y, rule_w, 45720, colors['accent'])
        if note:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}n', note, cx, rule_y + 150000,
                cw, 400000, font_pt=12, align=align,
                color=self.DECK_MUTED_LIGHT)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_closing_slide(self, presentation_id, slide_id, heading,
                          logos=None, legal=None, brand=None,
                          image_url=None, img_w=None, img_h=None):
        """The deck's last slide: a thank-you on the art field over
        ``logos`` (``[publisher_url, liquid_url]``, either None) and the
        legal line."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id, 'section')]
        reqs += self._art_field_reqs(slide_id, colors, image_url,
                                     img_w, img_h)
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}t', heading or 'Thank you',
            cx, 1700000, cw, 900000, font_pt=36, bold=True,
            align='CENTER', color=self.DECK_WHITE)
        logo_y = self.PAGE_H_EMU - 1150000
        left, right = (list(logos or []) + [None, None])[:2]
        if left:
            reqs += self._logo_plate_reqs(slide_id, f'{slide_id}l', left,
                                          cx + 91440, logo_y)
        if right:
            reqs += self._logo_plate_reqs(
                slide_id, f'{slide_id}r', right,
                self.PAGE_W_EMU - self.MARGIN_EMU - self.LOGO_W_EMU - 91440,
                logo_y)
        if legal:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}legal', legal, cx,
                self.PAGE_H_EMU - 380000, cw, 300000, font_pt=9,
                align='CENTER', color=self.DECK_MUTED_LIGHT)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_narrative_slide(self, presentation_id, slide_id, title, text,
                            brand=None, footer=None, page=None, rich=None,
                            eyebrow=None):
        """Build a native text slide with optional bold and bullet ranges."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        body_id = f'{slide_id}b'
        body_text = (rich or {}).get('text') or text or ''
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title or '', colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        top = self.content_top()
        reqs += self._text_box_reqs(
            slide_id, body_id, body_text, cx, top,
            cw, self.PAGE_H_EMU - top - 420000, font_pt=13,
            align='START', color=colors['ink'])
        if body_text:
            reqs.append({'updateParagraphStyle': {
                'objectId': body_id,
                'style': {'lineSpacing': 115,
                          'spaceBelow': {'magnitude': 4, 'unit': 'PT'}},
                'textRange': {'type': 'ALL'},
                'fields': 'lineSpacing,spaceBelow'}})
        for start, end in (rich or {}).get('bold_ranges') or []:
            if end > start:
                reqs.append({'updateTextStyle': {
                    'objectId': body_id, 'style': {'bold': True},
                    'textRange': {'type': 'FIXED_RANGE',
                                  'startIndex': start, 'endIndex': end},
                    'fields': 'bold'}})
        for start, end in (rich or {}).get('bullet_ranges') or []:
            if end > start:
                reqs.append({'createParagraphBullets': {
                    'objectId': body_id,
                    'textRange': {'type': 'FIXED_RANGE',
                                  'startIndex': start, 'endIndex': end},
                    'bulletPreset': 'BULLET_DISC_CIRCLE_SQUARE'}})
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_chart_slide(self, presentation_id, slide_id, title=None,
                        image_url=None, caption=None, notes=None,
                        img_w=None, img_h=None, brand=None, footer=None,
                        page=None, caption_h=CAPTION_H_EMU, eyebrow=None):
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        caption_y = self.PAGE_H_EMU - self.CAPTION_FOOT_EMU - caption_h
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        if image_url:
            box_y = self.content_top() if title else 300000
            box_h = (caption_y - box_y - self.CAPTION_GAP_EMU if caption
                     else self.PAGE_H_EMU - box_y - 420000)
            x, y, w, h = self._fit_box(img_w, img_h, cx, box_y, cw, box_h)
            reqs.append({'createImage': {
                'objectId': f'{slide_id}i', 'url': image_url,
                'elementProperties': self._elem_props(slide_id, w, h, x, y)}})
            if not self.is_dark:
                reqs.append({'updateImageProperties': {
                    'objectId': f'{slide_id}i',
                    'imageProperties': {'outline': {
                        'outlineFill': {'solidFill': {
                            'color': {'rgbColor': self.DECK_RULE}}},
                        'weight': {'magnitude': 1, 'unit': 'PT'}}},
                    'fields': 'outline'}})
        if caption:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}c', caption, cx, caption_y, cw,
                caption_h, font_pt=11, align='START',
                color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        if notes:
            self.add_speaker_notes(presentation_id, slide_id, notes)
        return slide_id

    def add_sheets_chart_slide(self, presentation_id, slide_id, title=None,
                               spreadsheet_id=None, chart_id=None,
                               caption=None, notes=None, brand=None,
                               footer=None, page=None,
                               linking_mode='NOT_LINKED_IMAGE',
                               eyebrow=None):
        """Embed a native Sheets chart with the deck's content styling and
        optional caption."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        box_y = self.content_top() if title else 300000
        box_h = self.PAGE_H_EMU - box_y - (800000 if caption else 420000)
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
                self.PAGE_H_EMU - 720000, cw, 340000, font_pt=11,
                align='START', color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        if notes:
            self.add_speaker_notes(presentation_id, slide_id, notes)
        return slide_id

    def _rect_reqs(self, slide_id, shape_id, x, y, w, h, fill_color,
                   shape_type='RECTANGLE', alpha=None):
        """A filled shape (no outline) — the card behind a stat tile, the
        band behind a section heading, the ink field behind a cover;
        ``alpha`` (0-1) makes it translucent."""
        fill = {'color': {'rgbColor': fill_color}}
        fields = 'shapeBackgroundFill.solidFill.color'
        if alpha is not None:
            fill['alpha'] = alpha
            fields += ',shapeBackgroundFill.solidFill.alpha'
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

    def _image_reqs(self, slide_id, shape_id, url, x, y, w, h):
        """A picture drawn at an exact box."""
        return [{'createImage': {
            'objectId': shape_id, 'url': url,
            'elementProperties': self._elem_props(slide_id, w, h, x, y)}}]

    def _cover_fit(self, img_w, img_h):
        """``(x, y, w, h)`` that covers the page without distortion,
        centred; unknown dimensions read as 16:9."""
        img_w, img_h = img_w or 16, img_h or 9
        scale = max(self.PAGE_W_EMU / float(img_w),
                    self.PAGE_H_EMU / float(img_h))
        draw_w, draw_h = img_w * scale, img_h * scale
        return (int((self.PAGE_W_EMU - draw_w) / 2),
                int((self.PAGE_H_EMU - draw_h) / 2),
                int(draw_w), int(draw_h))

    ART_VEIL_ALPHA = 0.72
    LOGO_W_EMU, LOGO_H_EMU = 1524000, 508000

    def _art_field_reqs(self, slide_id, colors, image_url=None,
                        img_w=None, img_h=None):
        """The field behind a dark slide: full-bleed art under a
        translucent ink veil, else plain ink — or nothing at all on a
        template layout, whose own art is the field."""
        reqs = []
        if not image_url and self.is_dark:
            return reqs
        if image_url:
            x, y, w, h = self._cover_fit(img_w, img_h)
            reqs += self._image_reqs(slide_id, f'{slide_id}art', image_url,
                                     x, y, w, h)
        reqs += self._rect_reqs(
            slide_id, f'{slide_id}bg', 0, 0, self.PAGE_W_EMU,
            self.PAGE_H_EMU, self.DECK_INK if self.is_dark else colors['ink'],
            alpha=self.ART_VEIL_ALPHA if image_url else None)
        return reqs

    def _logo_plate_reqs(self, slide_id, shape_id, logo_url, x, y):
        """A brand mark at ``(x, y)`` on a white plate, so any colourway
        reads on the dark field."""
        lw, lh = self.LOGO_W_EMU, self.LOGO_H_EMU
        pad = 91440  # 0.1" plate padding
        reqs = self._rect_reqs(
            slide_id, f'{shape_id}plate', x - pad, y - pad,
            lw + 2 * pad, lh + 2 * pad, self.DECK_WHITE,
            shape_type='ROUND_RECTANGLE')
        return reqs + self._image_reqs(slide_id, shape_id, logo_url,
                                       x, y, lw, lh)

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
                             page=None, eyebrow=None):
        """Build the heading and footer for light or dark content layouts."""
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = []
        if title and self.is_dark:
            if eyebrow:
                reqs += self._text_box_reqs(
                    slide_id, f'{slide_id}e', str(eyebrow).upper(), cx,
                    120000, cw, 220000, font_pt=8, bold=True,
                    align='START', color=colors['muted'],
                    font=self.DECK_EYEBROW_FONT)
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}t', title, cx, 330000, cw, 780000,
                font_pt=22, bold=True, align='START', color=colors['ink'])
        elif title:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}t', title, cx, 228600, cw, 520000,
                font_pt=20, bold=True, align='START', color=colors['ink'])
            reqs += self._rect_reqs(slide_id, f'{slide_id}rule', cx, 800100,
                                    cw, 22860, colors['accent'])
        if footer:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}f', footer, cx,
                self.PAGE_H_EMU - 320000, cw - 700000, 260000, font_pt=9,
                align='START', color=colors['muted'])
        if page and not self.is_dark:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}pg', str(page),
                self.PAGE_W_EMU - self.MARGIN_EMU - 600000,
                self.PAGE_H_EMU - 320000, 600000, 260000, font_pt=9,
                align='END', color=colors['muted'])
        return reqs

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
                reqs += self._text_box_reqs(
                    slide_id, f'{base}{suffix}', str(tile.get(key, '')),
                    x + pad, text_y, w - 2 * pad, height, font_pt=size,
                    bold=bold, color=color)
            text_y += height
        return reqs

    def add_stat_tile_slide(self, presentation_id, slide_id, title, tiles,
                            brand=None, footer=None, page=None,
                            eyebrow=None):
        """Draw up to nine native KPI cards, continuing dense grids before
        text would overlap."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        tiles = list(tiles or [])[:9]
        if tiles:
            cols = (3 if len(tiles) in (5, 6, 9)
                    else 4 if len(tiles) > 3 else len(tiles))
            n_rows = (len(tiles) + cols - 1) // cols
            gap = 137160
            grid_y = self.content_top()
            grid_h = self.PAGE_H_EMU - grid_y - 420000
            cell_w = (cw - gap * (cols - 1)) // cols
            cell_h = (grid_h - gap * (n_rows - 1)) // n_rows
            pad, text_w = TILE_PAD_EMU, cell_w - 2 * TILE_PAD_EMU
            blocks = [_tile_block_heights(tile, text_w) for tile in tiles]
            required = max(2 * pad + TILE_VALUE_H_EMU + sum(block)
                           for block in blocks)
            if required > cell_h and n_rows > 1:
                for start in range(0, len(tiles), cols):
                    self.add_stat_tile_slide(
                        presentation_id,
                        f'{slide_id}part{start}' if start else slide_id,
                        f'{title} (continued)' if start else title,
                        tiles[start:start + cols], brand, footer, page,
                        eyebrow)
                return slide_id
            cell_h = min(cell_h, max(TILE_MIN_H_EMU, required))
            for i, tile in enumerate(tiles):
                r, c = divmod(i, cols)
                reqs += self._tile_reqs(
                    slide_id, f'{slide_id}c{i}', tile,
                    cx + c * (cell_w + gap), grid_y + r * (cell_h + gap),
                    cell_w, cell_h, colors, pad, (26, *TILE_TEXT_PT),
                    (TILE_VALUE_H_EMU, *blocks[i]))
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    TILE_ROW_RAIL_EMU = 640000
    TILE_ROW_MAX = 3
    TILE_ROW_TILES_MAX = 6
    TILE_ROW_TEXT_PT = (20, 9, 8, 7)

    def add_tile_rows_slide(self, presentation_id, slide_id, title, rows,
                            brand=None, footer=None, page=None,
                            eyebrow=None):
        """Draw up to three labeled bands of six tiles, with captions and
        driver notes."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        rows = [row for row in list(rows or [])[:self.TILE_ROW_MAX]
                if row.get('tiles')]
        if not rows:
            self.slides_batch_update(presentation_id, reqs)
            return slide_id
        gap, pad = 91440, 80000
        rail = self.TILE_ROW_RAIL_EMU
        grid_y = self.content_top()
        grid_h = self.PAGE_H_EMU - grid_y - 420000
        band_h = (grid_h - gap * (len(rows) - 1)) // len(rows)
        value_pt, label_pt, caption_pt, note_pt = self.TILE_ROW_TEXT_PT
        value_h = int(value_pt * 1.5 * EMU_PER_PT)
        label_h = int(label_pt * 1.6 * EMU_PER_PT)
        for r, row in enumerate(rows):
            y = grid_y + r * (band_h + gap)
            base = f'{slide_id}b{r}'
            reqs += self._text_box_reqs(
                slide_id, f'{base}l', str(row.get('label', '')).upper(),
                cx, y + pad, rail - pad, band_h - 2 * pad, font_pt=8,
                bold=True, align='START', color=colors['muted'],
                font=self.DECK_EYEBROW_FONT)
            tiles = list(row['tiles'])[:self.TILE_ROW_TILES_MAX]
            cell_w = (cw - rail - gap * (len(tiles) - 1)) // len(tiles)
            text_w = cell_w - 2 * pad
            for i, tile in enumerate(tiles):
                x = cx + rail + i * (cell_w + gap)
                heights = (value_h, label_h, *(
                    _tile_text_height(str(tile.get(key) or ''), text_w, pt)
                    for key, pt in zip(('caption', 'note'),
                                       (caption_pt, note_pt))))
                reqs += self._tile_reqs(
                    slide_id, f'{base}t{i}', tile, x, y, cell_w, band_h,
                    colors, pad, self.TILE_ROW_TEXT_PT, heights,
                    bold_label=True)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    STORY_CHART_SHARE = 0.6
    STORY_STAT_H_EMU = 380000

    def add_chart_story_slide(self, presentation_id, slide_id, title,
                              image_url=None, img_w=None, img_h=None,
                              narrative=None, stats=None, brand=None,
                              footer=None, page=None, eyebrow=None):
        """Place a chart beside its narrative and above a stat bar. Without
        narrative, use the full width."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        gap = 182880
        top = self.content_top()
        body_h = self.PAGE_H_EMU - top - 420000
        chart_w = int(cw * self.STORY_CHART_SHARE) if narrative else cw
        stat_h = self.STORY_STAT_H_EMU if stats else 0
        if image_url:
            x, y, w, h = self._fit_box(img_w, img_h, cx, top, chart_w,
                                       body_h - stat_h - (gap if stats
                                                          else 0))
            reqs += self._image_reqs(slide_id, f'{slide_id}i', image_url,
                                     x, y, w, h)
        if stats:
            bar_y = top + body_h - stat_h
            reqs += self._card_reqs(slide_id, f'{slide_id}sr', cx, bar_y,
                                    chart_w, stat_h, colors)
            text = '   |   '.join(f'{value} {label}' for value, label in stats)
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}s', text, cx + 60000, bar_y + 60000,
                chart_w - 120000, stat_h - 120000, font_pt=11, bold=True,
                align='CENTER', color=colors['ink'])
        if narrative:
            nx = cx + chart_w + gap
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}n', narrative, nx, top,
                cw - chart_w - gap, body_h, font_pt=10, align='START',
                color=colors['ink'])
            reqs.append({'updateParagraphStyle': {
                'objectId': f'{slide_id}n',
                'style': {'lineSpacing': 115,
                          'spaceBelow': {'magnitude': 4, 'unit': 'PT'}},
                'textRange': {'type': 'ALL'},
                'fields': 'lineSpacing,spaceBelow'}})
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_columns_slide(self, presentation_id, slide_id, title, columns,
                          brand=None, footer=None, page=None, eyebrow=None):
        """Draw up to four narrative cards with toned headings."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        columns = list(columns or [])[:4]
        if columns:
            gap, pad = 137160, 100000
            y = self.content_top()
            h = self.PAGE_H_EMU - y - 420000
            w = (cw - gap * (len(columns) - 1)) // len(columns)
            for i, column in enumerate(columns):
                x = cx + i * (w + gap)
                base = f'{slide_id}k{i}'
                head_color = self._tone_color(column.get('tone'),
                                              colors['accent'])
                reqs += self._card_reqs(slide_id, f'{base}r', x, y, w, h,
                                        colors)
                reqs += self._text_box_reqs(
                    slide_id, f'{base}h', str(column.get('head', '')),
                    x + pad, y + pad, w - 2 * pad, 400000, font_pt=13,
                    bold=True, align='START', color=head_color)
                reqs += self._text_box_reqs(
                    slide_id, f'{base}b', str(column.get('body', '')),
                    x + pad, y + pad + 420000, w - 2 * pad,
                    h - 2 * pad - 420000, font_pt=10, align='START',
                    color=colors['ink'])
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    # A display value that reads as a number/rate — right-aligned in native
    # tables ("$1,234.50", "1.2B", "0.15%", "3.4x", "120% of plan").
    _NUMERIC_CELL_RE = re.compile(
        r'^[-+(]?[$€£]?\s?\d[\d,.]*\s*(?:[KMB%]|x)?\)?'
        r'(?:\s+of plan)?$', re.IGNORECASE)

    def _table_reqs(self, slide_id, table_id, x, y, w, h, header, body_rows,
                    colors):
        """Build a native table with header fill, alternating row fills and
        aligned numeric cells. Leave empty cells unstyled because Slides
        rejects their text ranges."""
        all_rows = [list(header)] + [list(r) for r in body_rows]
        n_cols = len(header)
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
                loc = {'rowIndex': r, 'columnIndex': c}
                reqs.append({'insertText': {
                    'objectId': table_id, 'cellLocation': loc,
                    'insertionIndex': 0, 'text': val}})
                reqs.append({'updateTextStyle': {
                    'objectId': table_id, 'cellLocation': loc,
                    'style': {
                        'fontSize': {'magnitude': 11 if is_header else 10,
                                     'unit': 'PT'},
                        'bold': is_header, 'fontFamily': self.DECK_FONT,
                        'foregroundColor': {'opaqueColor': {'rgbColor': (
                            self.DECK_WHITE if is_header
                            else colors['ink'])}}},
                    'textRange': {'type': 'ALL'},
                    'fields': 'fontSize,bold,fontFamily,foregroundColor'}})
                if not is_header and self._NUMERIC_CELL_RE.match(val):
                    reqs.append({'updateParagraphStyle': {
                        'objectId': table_id, 'cellLocation': loc,
                        'style': {'alignment': 'END'},
                        'textRange': {'type': 'ALL'},
                        'fields': 'alignment'}})
        reqs.append({'updateTableCellProperties': {
            'objectId': table_id,
            'tableRange': {'location': {'rowIndex': 0, 'columnIndex': 0},
                           'rowSpan': 1, 'columnSpan': n_cols},
            'tableCellProperties': {'tableCellBackgroundFill': {
                'solidFill': {'color': {'rgbColor': colors['accent']}}}},
            'fields': 'tableCellBackgroundFill.solidFill.color'}})
        zebra = {'color': {'rgbColor': colors['card']}}
        zebra_fields = 'tableCellBackgroundFill.solidFill.color'
        if colors.get('card_alpha') is not None:
            zebra['alpha'] = colors['card_alpha']
            zebra_fields += ',tableCellBackgroundFill.solidFill.alpha'
        for r in range(2, len(all_rows), 2):
            reqs.append({'updateTableCellProperties': {
                'objectId': table_id,
                'tableRange': {'location': {'rowIndex': r, 'columnIndex': 0},
                               'rowSpan': 1, 'columnSpan': n_cols},
                'tableCellProperties': {'tableCellBackgroundFill': {
                    'solidFill': zebra}},
                'fields': zebra_fields}})
        if self.is_dark:
            reqs.append({'updateTableBorderProperties': {
                'objectId': table_id, 'borderPosition': 'ALL',
                'tableBorderProperties': {
                    'tableBorderFill': {'solidFill': {
                        'color': {'rgbColor': self.DECK_RULE_DARK}}},
                    'weight': {'magnitude': 0.5, 'unit': 'PT'}},
                'fields': 'tableBorderFill,weight'}})
        return reqs

    def add_table_slide(self, presentation_id, slide_id, title, header,
                        body_rows, brand=None, caption=None, footer=None,
                        page=None, eyebrow=None):
        """Build a native table, continuing rows across slides before
        wrapped text overflows."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page,
                                          eyebrow=eyebrow)
        ty = self.content_top()
        th = self.PAGE_H_EMU - ty - (800000 if caption else 420000)
        head_h = _table_row_height(header, cw, 11)
        heights = [_table_row_height(row, cw, 10) for row in body_rows]
        groups = _fit_rows(body_rows, heights, th - head_h)
        if len(groups) > 1:
            for index, rows in enumerate(groups):
                self.add_table_slide(
                    presentation_id,
                    slide_id + (f'part{index}' if index else ''),
                    f'{title} (continued)' if index else title, header,
                    rows, brand, caption, footer, page, eyebrow)
            return slide_id
        th = min(th, head_h + sum(heights))
        reqs += self._table_reqs(slide_id, f'{slide_id}tbl', cx, ty, cw, th,
                                 header, body_rows, colors)
        for index, height in enumerate([head_h] + heights):
            reqs.append({'updateTableRowProperties': {
                'objectId': f'{slide_id}tbl', 'rowIndices': [index],
                'tableRowProperties': {'minRowHeight': self._emu(height)},
                'fields': 'minRowHeight'}})
        if caption:
            reqs += self._text_box_reqs(
                slide_id, f'{slide_id}c', caption, cx,
                ty + th + 80000, cw, 340000, font_pt=11,
                align='START', color=colors['muted'])
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
        body = '\n'.join('{}.  {}'.format(i + 1, e)
                         for i, e in enumerate(entries or []))
        top = self.content_top()
        reqs += self._text_box_reqs(
            slide_id, f'{slide_id}b', body, cx, top, cw,
            self.PAGE_H_EMU - top - 420000, font_pt=14,
            align='START', color=colors['ink'])
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
