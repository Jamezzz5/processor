import os
import logging
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.awss3 as awss3


class SsApi(object):
    url = 'url'
    site = 'site'
    file_name = 'file_name'
    img_url = 'img_url'
    ads = 'ads'
    date = 'date'
    device = 'device'
    device_mobile = 'Mobile'
    device_desktop = 'Desktop'
    ss_file_path = 'screenshots'
    output_file = 'sites.xlsx'
    output_csv = 'sites.csv'

    def __init__(self, file_name='site_config.csv', ss_file_path_date=None):
        logging.info('Getting config from {}.'.format(file_name))
        self.file_name = file_name
        self.ss_file_path_date = ss_file_path_date
        self.file_name = os.path.join(utl.config_path, file_name)
        self.config = self.import_config()
        self.ss_file_path_date = self.add_file_path(self.ss_file_path_date)
        self.add_device_to_config()
        self.set_all_sites()
        self.s3 = None

    def input_config(self, api_file):
        self.s3 = awss3.S3()
        self.s3.input_config(api_file)

    def import_config(self):
        df = pd.read_csv(self.file_name)
        config = df.to_dict(orient='index')
        return config

    def set_site(self, index):
        site_dict = self.config[index]
        site = Site(site_dict, ss_file_path_date=self.ss_file_path_date)
        return site

    def get_site(self, index):
        site_dict = self.config[index][self.site]
        return site_dict

    def set_all_sites(self):
        for index in self.config:
            site = self.set_site(index)
            self.config[index][self.site] = site

    def add_device_to_config(self):
        total_indices = max(self.config) + 1
        for index in range(total_indices):
            new_index = index + total_indices
            self.config[index][self.device] = self.device_desktop
            self.config[new_index] = self.config[index].copy()
            self.config[new_index][self.device] = self.device_mobile

    def get_data(self, sd, ed, fields):
        browser = utl.SeleniumWrapper()
        for index in self.config:
            site = self.get_site(index)
            if site.device == self.device_mobile and not browser.mobile:
                browser.quit()
                browser = utl.SeleniumWrapper(mobile=True)
            browser.take_screenshot(site.url, site.file_name)
            self.config[index][self.file_name] = site.file_name
        browser.quit()
        self.write_config_to_df()
        df = self.upload_screenshots()
        return df

    def take_screenshots_get_ads(self):
        browser = utl.SeleniumWrapper()
        for index in self.config:
            site = self.get_site(index)
            browser.take_screenshot(site.url, site.file_name)
            ads = browser.get_all_iframe_ads()
            self.config[index][self.ads] = ads
        browser.quit()
        self.write_config_to_df()

    def upload_screenshots(self):
        for index in self.config:
            site = self.get_site(index)
            image_data = utl.image_to_binary(site.file_name, True)
            key = site.file_name.replace('\\', '/')
            url = self.s3.s3_upload_file_obj(image_data, key)
            self.config[index][self.img_url] = url
        df = self.write_config_to_df()
        return df

    def add_file_path(self, ss_file_path_date=None):
        if not ss_file_path_date:
            ss_file_path_date = dt.datetime.today().strftime('%y%m%d_%H')
        file_path = os.path.join(self.ss_file_path, ss_file_path_date)
        utl.dir_check(file_path)
        return file_path

    def write_config_to_df(self):
        df = pd.DataFrame.from_dict(self.config, orient='index')
        date = self.ss_file_path_date.split('\\')[-1].split('/')[-1]
        date = dt.datetime.strptime(date, '%y%m%d_%H')
        df[self.date] = date
        output_file = os.path.join(self.ss_file_path_date, self.output_file)
        df.to_excel(output_file, index=False)
        df.to_excel(self.output_file, index=False)
        df.to_csv(self.output_csv, index=False)
        return df


class Site(object):
    prot = 'http'
    prots = 'https://'
    www = 'www'
    tlds = ['.com', '.net', '.de', '.org', '.gov', '.edu', '.tv']
    base_file_path = 'screenshots'

    def __init__(self, site_dict=None, url=None, file_name=None,
                 ss_file_path_date=None, device=None):
        self.url = url
        self.file_name = file_name
        self.device = device
        self.ss_file_path_date = ss_file_path_date
        self.site_dict = site_dict
        if site_dict:
            for k in site_dict:
                setattr(self, k, site_dict[k])
        self.url, self.file_name = self.check_site_params(
            self.url, self.file_name, self.device)

    def check_url(self, url=None):
        if (url[:4] != self.prot and
                (self.www not in url and url.count('.') == 1)):
            url = '{}{}.{}'.format(self.prots, self.www, url)
        elif url[:4] != self.prot:
            url = '{}{}'.format(self.prots, url)
        return url

    def check_file_name(self, url=None, file_name=None, device=None):
        if url and not file_name:
            file_name = url
            for x in [self.prots, self.www] + self.tlds:
                file_name = file_name.replace(x, '')
            file_name = file_name.replace('.', '')
            if device:
                file_name = '{}_{}'.format(file_name, device)
            file_name += '.png'
        file_name = os.path.join(self.ss_file_path_date, file_name)
        return file_name

    def check_site_params(self, url=None, file_name=None, device=None):
        file_name = self.check_file_name(url, file_name, device)
        url = self.check_url(url)
        return url, file_name
