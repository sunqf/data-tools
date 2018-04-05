#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import argparse
from typing import List
import asyncpg
from tqdm import tqdm
from bs4 import BeautifulSoup
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


async def delete_proxy(proxy):
    async with ClientSession() as proxy_session:
        async with proxy_session.get(proxy_delete_url(proxy)) as proxy_response:
            pass


def get_url(word, index):
    return 'http://dj.iciba.com/%s-1-%d.html' % (word, index)

def get_count(text):
    html = BeautifulSoup(text, 'lxml')
    script = html.find('script')
    if script:
        count = script.text[11:-1]
        if count.isnumeric():
            return int(count)

    return -1

async def fetch_data(keyword, index=1, try_times=1):
    if try_times > 10:
        print('%s try times %d' % (keyword, try_times))
        return ''

    proxy = await fetch_proxy()
    try:
        total = 100
        proxy_url = "http://{}".format(proxy)
        async with ClientSession(headers=headers) as session:
            async with session.get(get_url(keyword, index), proxy=proxy_url, timeout=10) as response:
                print(get_url(keyword, index))
                if response.status == 200:
                    text = await response.text()
                    count = get_count(text)
                    if count == -1:
                        raise RuntimeError('Raise for retry.')
                    total = min(total, (count + 9) // 10)
                else:
                    raise RuntimeError('Raise for retry.')

        return text + await fetch_data(keyword, index+1) if index < total else text
    except BaseException as e:
        print(e)
        #traceback.print_exc(file=sys.stdout)
        #await delete_proxy(proxy)
        return await fetch_data(keyword, index, try_times+1)

async def fetch(keyword):
    return keyword, await fetch_data(keyword, 1)

async def save(db_conn, buffer, direction):
    try:
        async with db_conn.transaction():
            await db_conn.executemany("INSERT INTO iciba (keyword, direction, data) VALUES ($1, $2, $3)",
                                      [(word, direction, data) for word, data in buffer])
    except BaseException as e:
        print(e)
        #traceback.print_exc(file=sys.stdout)


async def bound(sem: asyncio.Semaphore, func, *args, **kwargs):
    async with sem:
        return await func(*args, **kwargs)


async def main(keywords: List[str], direction: str):
    # create instance of Semaphore
    sem = asyncio.Semaphore(100)

    db_conn = await db.connect()

    async with db_conn.transaction():
        records = await db_conn.fetch('SELECT keyword from iciba')
        db_words = set([r['keyword'] for r in records])

    keywords = [word for word in keywords if word not in db_words]
    batch_size = 10000
    for batch in tqdm(range(0, len(keywords), batch_size), total=len(keywords)//batch_size):
        futures = [bound(sem, fetch, word) for word in keywords[batch:batch+batch_size]]
        buffer = []
        for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
            result = await future
            if result is None:
                print(result)
            else:
                buffer.append(result)
            if len(buffer) > 20:
                await save(db_conn, buffer, direction)
                buffer.clear()
        if len(buffer) > 0:
            await save(db_conn, buffer, direction)


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--dict', type=str, help='dict path')
arg_parser.add_argument('--direction', type=str, help='direction')

args = arg_parser.parse_args()





words = []
with open(args.dict) as file:
    for line in file:
        items = line.strip().split('\t')
        if len(items[0]) > 0:
            words.append(items[0])


loop = asyncio.get_event_loop()

loop.run_until_complete(main(words, args.direction))

loop.close()