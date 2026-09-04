import os
import re
import sys
import json
import logging
import time
import collections
import pandas as pd
import reporting.awss3 as awss3
import reporting.utils as utl
import reporting.vmcolumns as vmc
from requests_oauthlib import OAuth2Session
import requests

config_path = utl.config_path

TableCell = collections.namedtuple(
    'TableCell', 'start end para_ranges paragraphs')


class GsApi(object):
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
                            self.refresh_token, self.refresh_url, self.sheet_id]

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

    def create_presentation(self, presentation_name=None):
        logging.info('Creating GSlides Presentation: {}'.format(
            presentation_name))
        body = {
            "title": presentation_name,
        }
        response = self.client.post(url=self.slides_url, json=body)
        response = response.json()
        presentation_id = response["presentationId"]
        self.add_permissions(presentation_id)
        return presentation_id

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

    # --- Report-deck builders (additive; used by app report_export) ------
    # 16:9 page geometry, EMU (914400 EMU/inch).
    PAGE_W_EMU = 9144000
    PAGE_H_EMU = 5143500
    MARGIN_EMU = 457200
    # Deck typography — mirrors the app export brand kit
    # (app/features/exports.py BRAND_FONT / BRAND_NAVY / BRAND_MUTED);
    # duplicated here because the submodule cannot import the app.
    DECK_FONT = 'Segoe UI'
    DECK_INK = {'red': 0.173, 'green': 0.243, 'blue': 0.314}
    DECK_MUTED = {'red': 0.533, 'green': 0.533, 'blue': 0.533}
    # Accent (3B82F6 = app BRAND_ACCENT) and tile-card fill (F4F7FA =
    # BRAND_ZEBRA). Defaults for the native tiles / scorecard / dividers; the
    # app passes a per-client ``brand`` dict of rgbColor values to override.
    DECK_ACCENT = {'red': 0.231, 'green': 0.510, 'blue': 0.965}
    DECK_CARD = {'red': 0.957, 'green': 0.969, 'blue': 0.980}
    DECK_WHITE = {'red': 1.0, 'green': 1.0, 'blue': 1.0}
    # Hairline for image frames (E0E7EE — the app BRAND_RULE).
    DECK_RULE = {'red': 0.878, 'green': 0.906, 'blue': 0.933}
    # Muted text that stays legible on the dark ink cover/dividers.
    DECK_MUTED_LIGHT = {'red': 0.722, 'green': 0.780, 'blue': 0.839}
    # Comparator tones (15803D / DC2626 — match the app success/danger).
    DECK_GOOD = {'red': 0.082, 'green': 0.502, 'blue': 0.239}
    DECK_BAD = {'red': 0.863, 'green': 0.149, 'blue': 0.149}
    # Where content starts on a chrome'd slide (below title + accent rule).
    CONTENT_TOP_EMU = 980000

    def _brand_colors(self, brand):
        """Resolve a deck color set from an optional ``brand`` dict (rgbColor
        values keyed accent/ink/muted/card), falling back to the Liquid
        DECK_* defaults so a deck with no client brand looks exactly as today."""
        brand = brand or {}
        return {'accent': brand.get('accent') or self.DECK_ACCENT,
                'ink': brand.get('ink') or self.DECK_INK,
                'muted': brand.get('muted') or self.DECK_MUTED,
                'card': brand.get('card') or self.DECK_CARD}

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

    def _blank_slide_req(self, slide_id):
        return {'createSlide': {
            'objectId': slide_id,
            'slideLayoutReference': {'predefinedLayout': 'BLANK'}}}

    def _text_box_reqs(self, slide_id, shape_id, text, x, y, w, h,
                       font_pt=18, bold=False, align='START', color=None):
        style = {'fontSize': {'magnitude': font_pt, 'unit': 'PT'},
                 'bold': bold, 'fontFamily': self.DECK_FONT,
                 'foregroundColor': {'opaqueColor': {
                     'rgbColor': color or self.DECK_INK}}}
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
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._rect_reqs(slide_id, slide_id + 'bg', 0, 0,
                                self.PAGE_W_EMU, self.PAGE_H_EMU,
                                colors['ink'])
        # Brand-accent band anchoring the foot of the cover.
        band_h = 137160
        reqs += self._rect_reqs(slide_id, slide_id + 'band', 0,
                                self.PAGE_H_EMU - band_h, self.PAGE_W_EMU,
                                band_h, colors['accent'])
        logo_url = meta.get('logo_url')
        if logo_url:
            lw, lh = 1524000, 508000  # 3:1 brand mark, centered near the top
            pad = 91440  # 0.1" plate padding
            px = (self.PAGE_W_EMU - lw) // 2 - pad
            reqs += self._rect_reqs(
                slide_id, slide_id + 'plate', px, 520000 - pad,
                lw + 2 * pad, lh + 2 * pad, self.DECK_WHITE,
                shape_type='ROUND_RECTANGLE')
            reqs.append({'createImage': {
                'objectId': slide_id + 'logo', 'url': logo_url,
                'elementProperties': self._elem_props(
                    slide_id, lw, lh, (self.PAGE_W_EMU - lw) // 2, 520000)}})
        reqs += self._text_box_reqs(
            slide_id, slide_id + 't', meta.get('title', ''),
            cx, 1600200, cw, 900000, font_pt=30, bold=True, align='CENTER',
            color=self.DECK_WHITE)
        reqs += self._text_box_reqs(
            slide_id, slide_id + 's', meta.get('subtitle', ''),
            cx, 2600000, cw, 600000, font_pt=16, align='CENTER',
            color=self.DECK_MUTED_LIGHT)
        reqs += self._text_box_reqs(
            slide_id, slide_id + 'b', meta.get('brand', ''),
            cx, self.PAGE_H_EMU - 700000, cw, 400000, font_pt=12,
            align='CENTER', color=self.DECK_MUTED_LIGHT)
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_section_slide(self, presentation_id, slide_id, heading,
                          brand=None):
        """A divider slide, gold-deck style: a full-bleed dark ink field with
        the section heading in white and a short brand-accent underline —
        richer than a colored band floating on white."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._rect_reqs(slide_id, slide_id + 'bg', 0, 0,
                                self.PAGE_W_EMU, self.PAGE_H_EMU,
                                colors['ink'])
        head_h = 700000
        head_y = (self.PAGE_H_EMU - head_h) // 2 - 137160
        reqs += self._text_box_reqs(
            slide_id, slide_id + 't', heading or '',
            cx, head_y, cw, head_h,
            font_pt=28, bold=True, align='CENTER', color=self.DECK_WHITE)
        rule_w = 2057400  # 2.25" accent underline centered below the heading
        reqs += self._rect_reqs(
            slide_id, slide_id + 'rule', (self.PAGE_W_EMU - rule_w) // 2,
            head_y + head_h + 91440, rule_w, 45720, colors['accent'])
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_narrative_slide(self, presentation_id, slide_id, title, text,
                            brand=None, footer=None, page=None, rich=None):
        """A text slide — bold title + narrative body — for the executive
        summary and any analysis not paired with a chart, so the ALI story
        carries on the deck rather than only in speaker notes.

        ``rich`` (optional) renders the body Slides-native instead of verbatim:
        ``{'text', 'bold_ranges', 'bullet_ranges'}`` with UTF-16 ``(start,
        end)`` pairs — markdown bold becomes real bold runs, glyph bullets
        become native bulleted paragraphs."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        body_id = slide_id + 'b'
        body_text = (rich or {}).get('text') or text or ''
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title or '', colors,
                                          footer=footer, page=page)
        reqs += self._text_box_reqs(
            slide_id, body_id, body_text, cx, self.CONTENT_TOP_EMU,
            cw, self.PAGE_H_EMU - self.CONTENT_TOP_EMU - 420000, font_pt=13,
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
                        page=None):
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page)
        if image_url:
            box_y = self.CONTENT_TOP_EMU if title else 300000
            box_h = self.PAGE_H_EMU - box_y - (800000 if caption else 420000)
            x, y, w, h = self._fit_box(img_w, img_h, cx, box_y, cw, box_h)
            reqs.append({'createImage': {
                'objectId': slide_id + 'i', 'url': image_url,
                'elementProperties': self._elem_props(slide_id, w, h, x, y)}})
            # Hairline frame so a white-background chart PNG doesn't float
            # edgeless on the white slide.
            reqs.append({'updateImageProperties': {
                'objectId': slide_id + 'i',
                'imageProperties': {'outline': {
                    'outlineFill': {'solidFill': {
                        'color': {'rgbColor': self.DECK_RULE}}},
                    'weight': {'magnitude': 1, 'unit': 'PT'}}},
                'fields': 'outline'}})
        if caption:
            reqs += self._text_box_reqs(
                slide_id, slide_id + 'c', caption, cx,
                self.PAGE_H_EMU - 720000, cw, 340000, font_pt=11,
                align='START', color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        if notes:
            self.add_speaker_notes(presentation_id, slide_id, notes)
        return slide_id

    def add_sheets_chart_slide(self, presentation_id, slide_id, title=None,
                               spreadsheet_id=None, chart_id=None,
                               caption=None, notes=None, brand=None,
                               footer=None, page=None,
                               linking_mode='NOT_LINKED_IMAGE'):
        """A content slide embedding a native Google Sheets chart.

        Mirrors :meth:`add_chart_slide`'s chrome (title over the accent
        rule, footer, page number, caption band) but the body is a
        ``createSheetsChart`` element instead of an image, so the deck
        needs no rasterized pixels at all. Slides renders the chart into
        the element box, so the box fills the content area directly (no
        aspect-fit math).

        :param presentation_id: the deck to add the slide to.
        :param slide_id: the new slide's object id.
        :param title: slide title for the content chrome.
        :param spreadsheet_id: the spreadsheet holding the chart.
        :param chart_id: the chart's id from the ``addChart`` reply.
        :param caption: one-line takeaway under the chart.
        :param notes: speaker-notes text.
        :param brand: optional per-client deck colors (rgbColor dict).
        :param footer: muted running footer text.
        :param page: slide number for the footer chrome.
        :param linking_mode: ``NOT_LINKED_IMAGE`` (default) renders a
            static Google-rendered chart image so the source spreadsheet
            can stay private; ``LINKED`` keeps a live link and requires
            viewers to have access to the spreadsheet.
        :returns: ``slide_id``.
        """
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page)
        box_y = self.CONTENT_TOP_EMU if title else 300000
        box_h = self.PAGE_H_EMU - box_y - (800000 if caption else 420000)
        reqs.append({'createSheetsChart': {
            'objectId': slide_id + 'i',
            'spreadsheetId': spreadsheet_id,
            'chartId': chart_id,
            'linkingMode': linking_mode,
            'elementProperties': self._elem_props(
                slide_id, cw, box_h, cx, box_y)}})
        if caption:
            reqs += self._text_box_reqs(
                slide_id, slide_id + 'c', caption, cx,
                self.PAGE_H_EMU - 720000, cw, 340000, font_pt=11,
                align='START', color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        if notes:
            self.add_speaker_notes(presentation_id, slide_id, notes)
        return slide_id

    def _rect_reqs(self, slide_id, shape_id, x, y, w, h, fill_color,
                   shape_type='RECTANGLE'):
        """A filled shape (no outline) — the card behind a stat tile, the
        band behind a section heading, the ink field behind a cover."""
        return [
            {'createShape': {
                'objectId': shape_id, 'shapeType': shape_type,
                'elementProperties': self._elem_props(slide_id, w, h, x, y)}},
            {'updateShapeProperties': {
                'objectId': shape_id,
                'shapeProperties': {
                    'shapeBackgroundFill': {'solidFill': {
                        'color': {'rgbColor': fill_color}}},
                    'outline': {'propertyState': 'NOT_RENDERED'}},
                'fields': ('shapeBackgroundFill.solidFill.color,'
                           'outline.propertyState')}},
        ]

    def _content_chrome_reqs(self, slide_id, title, colors, footer=None,
                             page=None):
        """Shared chrome for content slides — bold title over a thin
        brand-accent rule, plus a muted footer line and slide number — so
        every slide reads as part of one branded deck rather than floating
        text on white. Content starts at ``CONTENT_TOP_EMU``."""
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = []
        if title:
            reqs += self._text_box_reqs(
                slide_id, slide_id + 't', title, cx, 228600, cw, 520000,
                font_pt=20, bold=True, align='START', color=colors['ink'])
            reqs += self._rect_reqs(slide_id, slide_id + 'rule', cx, 800100,
                                    cw, 22860, colors['accent'])
        if footer:
            reqs += self._text_box_reqs(
                slide_id, slide_id + 'f', footer, cx,
                self.PAGE_H_EMU - 320000, cw - 700000, 260000, font_pt=9,
                align='START', color=colors['muted'])
        if page:
            reqs += self._text_box_reqs(
                slide_id, slide_id + 'pg', str(page),
                self.PAGE_W_EMU - self.MARGIN_EMU - 600000,
                self.PAGE_H_EMU - 320000, 600000, 260000, font_pt=9,
                align='END', color=colors['muted'])
        return reqs

    def _tone_color(self, tone, colors, default):
        """The deck colour a good/bad tone earns, else ``default``."""
        return {'good': self.DECK_GOOD,
                'bad': self.DECK_BAD}.get(tone, default)

    def add_stat_tile_slide(self, presentation_id, slide_id, title, tiles,
                            brand=None, footer=None, page=None):
        """A slide of native stat tiles — each a card with a hero number, a
        label, a one-line comparison caption and an optional muted note
        (the driver line) — built from Slides shapes so the export never
        screenshots a KPI chart. ``tiles`` = list of
        ``{'value','label','caption','note'}`` (6-9 read best; capped at
        9; five, six or nine tiles lay out three across)."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page)
        tiles = list(tiles or [])[:9]
        if tiles:
            cols = (3 if len(tiles) in (5, 6, 9)
                    else 4 if len(tiles) > 3 else len(tiles))
            n_rows = (len(tiles) + cols - 1) // cols
            gap = 137160  # 0.15"
            grid_y = self.CONTENT_TOP_EMU
            grid_h = self.PAGE_H_EMU - grid_y - 420000
            cell_w = (cw - gap * (cols - 1)) // cols
            cell_h = (grid_h - gap * (n_rows - 1)) // n_rows
            cell_h = min(cell_h, 1370000)  # ~1.5"
            pad = 100000
            for i, tile in enumerate(tiles):
                r, c = divmod(i, cols)
                x = cx + c * (cell_w + gap)
                y = grid_y + r * (cell_h + gap)
                base = f'{slide_id}c{i}'
                cap_color = self._tone_color(tile.get('tone'), colors,
                                             colors['muted'])
                reqs += self._rect_reqs(slide_id, base + 'r', x, y,
                                        cell_w, cell_h, colors['card'])
                reqs += self._text_box_reqs(
                    slide_id, base + 'v', str(tile.get('value', '')),
                    x + pad, y + pad, cell_w - 2 * pad, cell_h * 40 // 100,
                    font_pt=26, bold=True, align='START',
                    color=colors['accent'])
                reqs += self._text_box_reqs(
                    slide_id, base + 'l', str(tile.get('label', '')),
                    x + pad, y + cell_h * 42 // 100, cell_w - 2 * pad,
                    cell_h * 20 // 100, font_pt=11, align='START',
                    color=colors['ink'])
                if tile.get('caption'):
                    reqs += self._text_box_reqs(
                        slide_id, base + 'p', str(tile['caption']),
                        x + pad, y + cell_h * 62 // 100, cell_w - 2 * pad,
                        cell_h * 18 // 100, font_pt=9, align='START',
                        color=cap_color)
                if tile.get('note'):
                    reqs += self._text_box_reqs(
                        slide_id, base + 'n', str(tile['note']),
                        x + pad, y + cell_h * 80 // 100, cell_w - 2 * pad,
                        cell_h * 18 // 100, font_pt=8, align='START',
                        color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_columns_slide(self, presentation_id, slide_id, title, columns,
                          brand=None, footer=None, page=None):
        """A slide of two to four side-by-side cards — the gold deck's
        Continue / Stop / Test learnings, its numbered challenges, its
        wins — each ``{'head', 'body', 'tone'}``: a bold toned heading
        over the card's copy. Capped at four; more would not read."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page)
        columns = list(columns or [])[:4]
        if columns:
            gap, pad = 137160, 100000
            y = self.CONTENT_TOP_EMU
            h = self.PAGE_H_EMU - y - 420000
            w = (cw - gap * (len(columns) - 1)) // len(columns)
            for i, column in enumerate(columns):
                x = cx + i * (w + gap)
                base = f'{slide_id}k{i}'
                head_color = self._tone_color(column.get('tone'), colors,
                                              colors['accent'])
                reqs += self._rect_reqs(slide_id, base + 'r', x, y, w, h,
                                        colors['card'])
                reqs += self._text_box_reqs(
                    slide_id, base + 'h', str(column.get('head', '')),
                    x + pad, y + pad, w - 2 * pad, 400000, font_pt=13,
                    bold=True, align='START', color=head_color)
                reqs += self._text_box_reqs(
                    slide_id, base + 'b', str(column.get('body', '')),
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
        """Requests for a native table with a filled, white-on-accent header
        row, zebra-banded body rows, and right-aligned numeric cells. Empty
        cells are left unstyled (Slides rejects an ALL text range on an empty
        cell)."""
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
                            self.DECK_WHITE if is_header else colors['ink'])}}},
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
        # Zebra banding on alternating body rows keeps a 12-row scorecard
        # scannable without heavy gridlines.
        for r in range(2, len(all_rows), 2):
            reqs.append({'updateTableCellProperties': {
                'objectId': table_id,
                'tableRange': {'location': {'rowIndex': r, 'columnIndex': 0},
                               'rowSpan': 1, 'columnSpan': n_cols},
                'tableCellProperties': {'tableCellBackgroundFill': {
                    'solidFill': {'color': {'rgbColor': colors['card']}}}},
                'fields': 'tableCellBackgroundFill.solidFill.color'}})
        return reqs

    def add_table_slide(self, presentation_id, slide_id, title, header,
                        body_rows, brand=None, caption=None, footer=None,
                        page=None):
        """A slide with a native Slides table (styled header) — the KPI
        scorecard and any tabular/appendix panel, built native rather than
        screenshotted. ``caption`` reads under the table like a chart
        slide's, so a panel's takeaway stays on the slide."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, title, colors,
                                          footer=footer, page=page)
        ty = self.CONTENT_TOP_EMU
        th = self.PAGE_H_EMU - ty - (800000 if caption else 420000)
        reqs += self._table_reqs(slide_id, slide_id + 'tbl', cx, ty, cw, th,
                                 header, body_rows, colors)
        if caption:
            reqs += self._text_box_reqs(
                slide_id, slide_id + 'c', caption, cx,
                self.PAGE_H_EMU - 720000, cw, 340000, font_pt=11,
                align='START', color=colors['muted'])
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def add_toc_slide(self, presentation_id, slide_id, entries, brand=None,
                      footer=None, page=None):
        """A table-of-contents slide: numbered section labels (gold-deck
        convention). ``entries`` is a list of section-label strings."""
        colors = self._brand_colors(brand)
        cx, cw = self.MARGIN_EMU, self.PAGE_W_EMU - 2 * self.MARGIN_EMU
        reqs = [self._blank_slide_req(slide_id)]
        reqs += self._content_chrome_reqs(slide_id, 'Contents', colors,
                                          footer=footer, page=page)
        body = '\n'.join('{}.  {}'.format(i + 1, e)
                         for i, e in enumerate(entries or []))
        reqs += self._text_box_reqs(
            slide_id, slide_id + 'b', body, cx, self.CONTENT_TOP_EMU, cw,
            self.PAGE_H_EMU - self.CONTENT_TOP_EMU - 420000, font_pt=14,
            align='START', color=colors['ink'])
        self.slides_batch_update(presentation_id, reqs)
        return slide_id

    def delete_non_report_slides(self, presentation_id, keep_prefix='rb'):
        """Drop the blank slide Google auto-creates with a new deck so the
        report's own title slide leads (report slide ids use ``keep_prefix``)."""
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
        for slide in pres.get('slides', []):
            sid = slide.get('objectId')
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
            # Fetch image content
            response = requests.get(img_url, stream=True, timeout=10)
            if response.status_code != 200:
                logging.warning('Failed to download image: {} (Status {})'.format(img_url, response.status_code))
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
