#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import json
import argparse
from typing import List
import asyncpg
from tqdm import tqdm
import re
import traceback

from corpus.util.config import *
from corpus.bicorpus import db

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}


async def fetch_proxy():
    while True:
        try:
            async with ClientSession() as proxy_session:
                async with proxy_session.get(proxy_get_url) as proxy_response:
                    if proxy_response.status == 200:
                        content = await proxy_response.text()
                        if len(content.split(':')) == 2:
                            return content
        except BaseException as e:
            print(e)
            traceback.print_exc()


async def delete_proxy(proxy):
    async with ClientSession() as proxy_session:
        async with proxy_session.get(proxy_delete_url(proxy)) as proxy_response:
            pass


def helper(json_data, fields):
    field_len = len(fields)

    def dfs(node, index):
        if index >= field_len:
            yield node

        if index < field_len and fields[index] in node:
            next = node[fields[index]]
            if isinstance(next, list):
                for child in next:
                    yield from dfs(child, index+1)
            else:
                yield from dfs(next, index+1)

    yield from dfs(json_data, 0)


def remove_html_tag(text):

    # Let’s <i>play cards</i> . --> Let’s play cards .
    tag = re.compile(r'<([a-z]+)>(.*?)</\1>')
    text = tag.sub(r'\2', text)

    # I’ll just go up (= <em>go upstairs</em> ) and ask him what he wants. --> I’ll just go up and ask him what he wants.
    em_tag = re.compile(r'\(= <em>.*?</em> \)')
    text = tag.sub('', text)

    single = re.compile(r'<br>')
    text = single.sub('', text)
    return text


def extract_blng_sents(json_data): # -> Generator[(str, str, str)]:
    guess_lang = list(helper(json_data, ['meta', 'guessLanguage']))[0]
    pairs = helper(json_data, ['blng_sents', 'sentence-pair'])
    for pair in pairs:
        if guess_lang == 'zh':
            zh = pair['sentence'].strip()
            en = pair['sentence-translation'].strip()
        else:
            en = pair['sentence'].strip()
            zh = pair['sentence-translation'].strip()

        if en != zh:
            url = pair['url']
            yield (en, zh, url)


def extract_auth(json_data):
    for pair in helper(json_data, ['auth_sents', 'sent']):
        src = remove_html_tag(pair['foreign'])
        url = pair['url']
        yield (src, url)


def extract_media(json_data):
    for pair in helper(json_data, ['media_sents', 'sent']):
        if 'chn' in pair:
            en = remove_html_tag(pair['eng'])
            zh = remove_html_tag(pair['chn'])
            #url = pair['streamUrl']
            if len(en) > 0 and len(zh) > 0:
                yield (en, zh, 'media')


hanzi = re.compile(
    u'([^\u0000-\u007f\u00f1\u00e1\u00e9\u00ed\u00f3\u00fa\u00d1\u00c1\u00c9\u00cd\u00d3\u00da\u0410-\u044f\u0406\u0407\u040e\u0456\u0457\u045e])')


def detect_zh(text):
    zh_len = len(hanzi.findall(text))
    return zh_len/len(text) > 0.2


def correct(pairs):
    for en, zh, url in pairs:
        src_zh = detect_zh(en)
        trg_zh = detect_zh(zh)
        if src_zh:
            yield zh, en, url
        else:
            yield en, zh, url


def extract(data):
    json_data = json.loads(data)
    return list(correct(list(extract_blng_sents(json_data)) + list(extract_media(json_data))))


def is_json(text: str) -> bool:
    try:
        text = json.loads(text)
        if 'input' in text:
            return True
        else:
            return False
    except Exception:
        return False


async def fetch_data(keyword, max_try_times=-1):

    try_times = 0
    while max_try_times < 0 or try_times < max_try_times:
        try:
            proxy = await fetch_proxy()
            proxy_url = "http://{}".format(proxy)
            async with ClientSession(headers=headers) as session:
                async with session.get(get_url(keyword), proxy=proxy_url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.read()
                        if is_json(data):
                            print(get_url(keyword))
                            return keyword, data
                    raise RuntimeError('raise for try again.')
        except Exception as e:
            pass
            #print(get_url(keyword))
            #print(e)
            #traceback.print_exc()
            #traceback.print_exc(file=sys.stdout)
            #await delete_proxy(proxy)

        try_times += 1

    print("Warning: keyword %s has be fetched %d times" % (keyword, max_try_times))
    return keyword, None


async def fetch(keyword, direction, db_conn):
    data = await fetch_data(keyword)
    if data:
        try:
            async with db_conn.transaction():
                await db_conn.execute("INSERT INTO youdao_bilingual (keyword, direction, data) VALUES ($1, $2, $3)",
                                   keyword, direction, data)
        except BaseException as e:
            print(e)
            #traceback.print_exc(file=sys.stdout)


async def save_word(db_pool, keyword):
    try:
        async with db_pool.acquire() as writer:
            async with writer.transaction():
                await writer.execute("INSERT INTO youdao_bilingual (keyword) VALUES ($1)", keyword)
    except Exception as e:
        print(e)
        #traceback.print_exc(file=sys.stdout)


async def save_pairs(db_pool, pairs):
    if len(pairs) > 0:
        async with db_pool.acquire() as writer:
            async with writer.transaction():
                await writer.executemany(
                    'INSERT INTO ch2en (ch, en, url, source) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING',
                    [(ch, en, url, 'youdao') for en, ch, url in correct(pairs)])


async def bound(sem: asyncio.Semaphore, func, *args, **kwargs):
    async with sem:
        return await func(*args, **kwargs)


async def main(keywords: List[str], direction: str):
    # create instance of Semaphore
    sem = asyncio.Semaphore(200)

    db_pool = await db.connect_pool()

    async with db_pool.acquire() as reader:
        async with reader.transaction():
            records = await reader.fetch('SELECT keyword from youdao_bilingual')
            db_words = set([r['keyword'].lower() for r in records])

    keywords = [word for word in keywords if word not in db_words]
    batch_size = 10000
    for batch in tqdm(range(0, len(keywords), batch_size), total=len(keywords)//batch_size):
        futures = [bound(sem, fetch_data, word, 20) for word in keywords[batch:batch+batch_size]]
        for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
            keyword, data = await future
            if data is not None:
                await save_pairs(db_pool, extract(data))
                await save_word(db_pool, keyword)



arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--dict', type=str, help='dict path')
arg_parser.add_argument('--direction', type=str, help='direction')

args = arg_parser.parse_args()


words = []
with open(args.dict) as file:
    for line in file:
        items = line.strip().split('\t')
        if len(items[0]) > 0:
            words.append(items[0].lower())


loop = asyncio.get_event_loop()

loop.run_until_complete(main(words, args.direction))

loop.close()
