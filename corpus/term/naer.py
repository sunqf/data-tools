#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

"""
    台湾  國家教育研究院提供
    入口 http://terms.naer.edu.tw/download/
"""

import requests
from bs4 import BeautifulSoup
import io
import zipfile
import sys
import re

import psycopg2

DSN = "dbname=sunqf user=sunqf"

def unzip_data(content):
    stream = io.BytesIO(content)
    with zipfile.ZipFile(stream) as myzip:
        for name in myzip.namelist():
            with myzip.open(name) as file:
                yield name, file.read().decode('utf-8')

def extract_term(data):
    for tr in BeautifulSoup(data, 'html.parser').find_all('tr')[1:]:
        id, en, zh, *_ = map(lambda td: td.text.strip(), tr.find_all('td'))
        yield en, zh


def get_html(url, timeout=10, try_time=1):
    if try_time > 10:
        raise RuntimeError('Getting url exceed 10 times.')

    try:
        html = requests.get(url, timeout=timeout)
        if html.status_code == 200:
            return html.content
        else:
            raise RuntimeError('Response status_code is %d' % html.status_code)
    except Exception as e:
        print(e)
        return get_html(url, try_time+1)


def get_list(output):
    list_html = get_html('http://terms.naer.edu.tw/download/')
    if list_html:
        html = BeautifulSoup(list_html, 'html.parser')
        with open(output, 'w') as output_file:
            for link in html.find(attrs={'class': 'list-tab-content'}).find_all('a'):
                prefix = 'http://terms.naer.edu.tw'
                domain = link.text
                link = prefix + link.attrs['href']
                print(domain, link)
                domain_html = get_html(link)
                domain_html = BeautifulSoup(domain_html, 'html.parser')

                for download_url in domain_html.find_all('a', text=re.compile('.*壓縮檔')):
                    print(download_url)
                    download_url = prefix + download_url.attrs['href']
                    # 文件较大，网络较慢等可能会影响下载速度，设置为10分钟超时
                    zip_data = get_html(download_url, timeout=600)

                    for name, data in unzip_data(zip_data):
                        yield (domain, name, data)
                        for en, zh in extract_term(data):
                            output_file.write('\t'.join([domain, en, zh]) + '\n')


def save_to_db(dsn: str, data_list):
    conn = psycopg2.connect(dsn)

    for domain, name, data in data_list:
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
                    cur.execute("INSERT INTO raw_terms (domain, name, data, source) VALUES (%s, %s, %s, %s)",
                                (domain, name, data, 'naer'))

        except Exception as e:
            print(e)

    conn.close()

'''
def update_db(dsn: str):
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as select_cur:
                select_cur.execute("SELECT id, data FROM raw_terms WHERE source='naer'")
                while True:
                    batch_size = 10
                    data = select_cur.fetchmany(batch_size)
                    with conn.cursor() as update_cur:
                        update_cur.execute("UPDATE raw_terms SET data = record.data"
                                           "FROM (VALUES %s) AS record (id, data)"
                                           "WHERE raw_terms.id = record.id",
                                           [(id, data.encode(encoding='latin1').decode('utf-8')) for id, data in data])

                    if len(data) < batch_size:
                        break
    except Exception as e:
        print(e)

    conn.close()
'''

data_list = get_list(sys.argv[1])

save_to_db(DSN, data_list)




