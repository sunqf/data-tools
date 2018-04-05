#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import argparse
import json
from typing import List, Set
import asyncpg
from tqdm import tqdm
from bs4 import BeautifulSoup, Tag
from corpus.util.config import *
from logging import *
from corpus.util.headless import Headless
from corpus.util import lang_detect
from selenium.webdriver.common.by import By
from corpus.bicorpus import db

getLogger().setLevel(INFO)

def get_url(src: str, tgt: str, word: str):
    return 'http://fanyi.baidu.com/#{}/{}/{}'.format(src, tgt, word)


def extract_pair(html: BeautifulSoup, src_lang: str, tgt_lang: str):
    html = html.find(class_='double-sample')
    if html:
        src_tags = html.select('p.sample-source')
        tgt_tags = html.select('p.sample-target')
        urls = [tag.text for tag in html.select('p.sample-resource')]

        words = set([s.text.strip() for tag in src_tags for s in tag.find_all('span')]
                    + [s.text.strip() for tag in tgt_tags for s in tag.find_all('span')])

        srcs = [t.text.strip() for t in src_tags]
        tgts = [t.text.strip() for t in tgt_tags]

        ens, zhs = (srcs, tgts) if src_lang == 'en' else (tgts, srcs)

        return list(zip(ens, zhs, urls)), words
    else:
        return [], set()


class BaiduCrawler:
    def __init__(self, keywords: Set[str], finished: Set[str], db_pool, headless):

        self.keywords = keywords
        self.finished = finished

        self.db_pool = db_pool

        self.max_try_times = 10
        self.use_proxy = False
        self.timeout = 5

        self.headless = headless

        self.locator = (By.CSS_SELECTOR, 'div.output-bd')
        #self.locator = None

    async def fetch_by_word(self, keyword: str):
        keyword = keyword.strip()
        if not keyword[0].isalnum():
            return keyword, [], set()

        src_lang, tgt_lang = ('zh', 'en') if lang_detect.detect_zh(keyword) else ('en', 'zh')

        url = get_url(src_lang, tgt_lang, keyword)
        html = await self.headless.get(url, self.locator, self.timeout)

        if html is not None:
            html = BeautifulSoup(html, 'html.parser')
            pairs, words = extract_pair(html, src_lang, tgt_lang)
            return keyword, pairs, words
        else:
            return keyword, None, None

    async def save_keyword(self, keyword):
        try:
            async with self.db_pool.acquire() as db_conn:
                async with db_conn.transaction():
                    await db_conn.execute("INSERT INTO baidu (keyword) VALUES ($1)", keyword)
        except BaseException as e:
            print(e)
            #traceback.print_exc(file=sys.stdout)

    async def save(self, pairs):
        try:
            async with self.db_pool.acquire() as db_conn:
                async with db_conn.transaction():
                    await db_conn.executemany("INSERT INTO ch2en (ch, en, url, source) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                                              [(zh, en, url, 'baidu') for en, zh, url in pairs])
        except BaseException as e:
            print(e)
            #traceback.print_exc(file=sys.stdout)

    def get_word(self):
        while len(self.keywords) > 0:
            keyword = self.keywords.pop()
            yield keyword

    """
    async def main(self):
        for keyword in tqdm(self.get_word()):
            keyword, pairs, new_words = await self.fetch_by_word(keyword)
            if pairs is not None:
                await self.save_keyword(keyword)

                self.finished.add(keyword)
                for new_word in new_words:
                    if new_word not in self.finished:
                        self.keywords.add(new_word)

                if len(pairs) > 0:
                    await self.save(pairs)

    """    
    async def main(self):
        while len(self.keywords) > 0:
            batch = set()
            tasks = []
            while len(tasks) < 10000 and len(self.keywords) > 0:
                keyword = self.keywords.pop()
                batch.add(keyword)
                tasks.append(self.fetch_by_word(keyword))

            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                keyword, pairs, new_words = await future

                if pairs is not None:
                    if len(pairs) > 0:
                        await self.save(pairs)
                    await self.save_keyword(keyword)

                    self.finished.add(keyword)
                    for new_word in new_words:
                        if new_word not in batch and new_word not in self.finished:
                            self.keywords.add(new_word)


    @classmethod
    async def create(cls, loop, keywords, headless):
        db_pool = await db.connect_pool()

        async with db_pool.acquire() as db_conn:
            async with db_conn.transaction():
                records = await db_conn.fetch('SELECT keyword from baidu')
                finished = set([r['keyword'] for r in records])

        keywords = set([word for word in keywords if word not in finished])

        return BaiduCrawler(keywords, finished, db_pool, headless)


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--dict', type=str, help='dict path')

args = arg_parser.parse_args()

words = set()
with open(args.dict) as file:
    for line in file:
        items = line.strip().split('\t')
        if len(items[0]) > 0:
            words.add(items[0])

max_worker = 5
loop = asyncio.get_event_loop()
headless = Headless(loop, use_proxy=False, pool_size=max_worker, log_path=None)
baidu = loop.run_until_complete(BaiduCrawler.create(loop, words, headless))
loop.run_until_complete(baidu.main())
loop.close()