#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

# http://yingyucihui.scientrans.com

import sys
import psycopg2
import requests
from bs4 import BeautifulSoup
import traceback

from typing import Set

DSN = "dbname=sunqf user=sunqf"


def get_html(url, timeout=10, max_try_times=-1):
    try_times= 1
    while max_try_times < 0 or try_times <= max_try_times:
        try:
            html = requests.get(url, timeout=timeout)
            if html.status_code == 200:
                return html.content
            elif html.status_code in [403, 404, 500]:
                print('Response status_code is %d' % html.status_code)
                return None
            else:
                raise RuntimeError('Response status_code is %d' % html.status_code)
        except Exception as e:
            print(e)

        try_times += 1

    if try_times > max_try_times:
        print('Getting url exceed 10 times.')

    return None


def get_terms(url):
    url = url[:-9] + '%d_%d.html'

    for head in range(65, 91):
        index = 1
        total = None
        while total is None or index <= total:
            print(url % (head, index))
            response = get_html(url % (head, index))
            if response:
                html = BeautifulSoup(response, 'html.parser')
                m_list = html.find(attrs={'class': 'm_list'})
                if m_list:
                    yield from map(lambda dd: dd.text, m_list.find_all('dd')[1:])

                if total is None:
                    def _total(tag):
                        pages = tag.find(attrs={'id': 'jumpToPages'})
                        if pages:
                            options = html.find(attrs={'id': 'jumpToPages'}).find_all('option')
                            if len(options) > 0:
                                return int(options[-1].text)
                        return 0
                    total = _total(html)

            index += 1


def get_list(finished: Set[str]):
    url = 'http://yingyucihui.scientrans.com/'
    response1 = get_html(url)
    if response1:
        html1 = BeautifulSoup(response1, 'html.parser')
        #for link1 in html1.find(attrs={'class': 'm_2'}).find_all('dt'):
        for link1 in html1.select('#looseindex_r > div > ul > li > dl > dt > a'):
            domain = link1.text
            href = link1.attrs['href']
            response2 = get_html(href)
            if response2:
                html2 = BeautifulSoup(response2, 'html.parser')
                for link2 in html2.find(attrs={'class': 'm_1'}).find_all('a'):
                    href = link2.attrs['href']
                    href = href if href.startswith('http') else url + href
                    name = link2.attrs['title']
                    if '\t'.join([domain, name]) not in finished:
                        yield domain, name, '\n'.join(list(get_terms(href)))


def save_to_db(dsn: str):
    conn = psycopg2.connect(dsn)

    finished = set()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT domain, name FROM raw_terms WHERE source='scientrans'")
            records = ['\t'.join([domain, name]) for domain, name in cur.fetchall()]
            finished.update(records)
    print(finished)
    for domain, name, data in get_list(finished):
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
                    cur.execute("INSERT INTO raw_terms (domain, name, data, source) VALUES (%s, %s, %s, %s)",
                                (domain, name, data, 'scientrans'))

        except Exception as e:
            print(e)
            traceback.print_exc()

    conn.close()


save_to_db(DSN)