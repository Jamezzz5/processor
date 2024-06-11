import os
import sys
import json
import logging
import time
import pandas as pd
import reporting.utils as utl
import reporting.vmcolumns as vmc
from requests_oauthlib import OAuth2Session

config_path = utl.config_path


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
    doc_str = 'Doc'
    text_format = 'NORMAL_TEXT'
    screenshot_dir = os.path.join('screenshots', 'charts/')

    def __init__(self):
        self.default_config = "gsapi_screenshots.json"
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
        response = r.json()
        self.df = self.parse_response(response)
        return self.df

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

    def add_permissions(self, presentation_id, domain="liquidarcade.com"):
        url = self.files_url + "/" + presentation_id + "/permissions"
        body = {
            "role": "writer",
            "type": "domain",
            "domain": domain,
            "allowFileDiscovery": True
        }
        response = self.client.post(url=url, json=body)
        return response

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

    def parse_google_doc(self, r):
        if self.body_str not in r:
            logging.warning('Body not in response {}.'.format(r))
            return pd.DataFrame()
        r = r[self.body_str][self.cont_str]
        paragraph = []
        new_paragraph = {}
        for x in r:
            if self.para_str in x:
                tc = x[self.para_str]['elements'][0]['textRun'][self.cont_str]
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
        self.df = pd.DataFrame(paragraph)
        return self.df

    def create_google_doc(self, title=None):
        logging.info('Creating GSlides Presentation: {}'.format(title))
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

    @staticmethod
    def fill_row(row, index):
        row_requests = []
        for cell in row:
            if not str(cell).strip():
                cell = '0'
            row_requests.append({
                "insertText":
                    {
                        "text": str(cell).strip(),
                        "location":
                            {
                                "index": index
                            }
                    }
            })
            index += len(str(cell).strip()) + 2
        index += 1
        return row_requests, index

    def add_table(self, data, index):
        """
        See for indexing: (https://stackoverflow.com/questions/75689738/
        how-can-i-dynamically-populate-a-table-in-google-doc-using-their-api)
        """
        if not data:
            return index
        start_ind = index
        table_requests = [{'insertTable': {
            'rows': len(data) + 1,
            'columns': len(data[0]),
            'endOfSegmentLocation': {
                'segmentId': ''
            }
        }}]
        index += 4
        column_req, index = self.fill_row(data[0].keys(), index)
        table_requests.append(column_req)
        for row in data:
            row_request, index = self.fill_row(row.values(), index)
            table_requests += row_request
        table_requests.append(self.get_format_req(start_ind, index - 1,
                                                  self.text_format))
        return table_requests, index - 1

    def add_image_doc(self, presigned_url, index):
        if not presigned_url:
            return [], index
        img_request = [{'insertInlineImage': {
            'location': {
                'index': index
            },
            'uri': presigned_url,
            'objectSize': {
                'height': {
                    'magnitude': 250,
                    'unit': 'PT'
                },
                'width': {
                    'magnitude': 250,
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

    def add_text(self, doc_id, text_json=None, index=1, newline=True):
        logging.info('Adding text to doc.')
        url = self.docs_url + "/" + doc_id + ":batchUpdate"
        headers = {"Content-Type": "application/json"}
        request = []
        format_request = []
        for item in text_json:
            if item['selected'] == 'false':
                continue
            if 'message' not in item:
                continue
            text = item['message']
            if newline:
                text += '\n'
            request.append({
                'insertText': {
                    'location': {
                        'index': index,
                    },
                    'text': text
                }
            })
            style = item['format'] if 'format' in item else self.text_format
            end_ind = index + len(text) - 1
            format_request.append(self.get_format_req(index, end_ind, style))
            index += len(text)
            if 'data' in item:
                table_req = []
                if 'imgURI' in item['data']['cols']:
                    presigned_url = item['url']
                    table_req, index = self.add_image_doc(presigned_url, index)
                elif item['data']['cols']:
                    table_req, index = self.add_table(item['data']['data'],
                                                      index=index)
                request += table_req
        request += format_request
        body = {"requests": request}
        response = self.client.post(url=url, json=body, headers=headers)
        return response, body

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
            r = r.json()
            row = [acc_col, ' '.join([failure_msg, msg, r['error']['message']]), False]
            results.append(row)
        return results, r

    def test_connection(self, acc_col, camp_col=None, acc_pre=None):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        results, r = self.check_sheet_id(
            [], acc_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
