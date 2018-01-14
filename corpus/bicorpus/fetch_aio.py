#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import sys, traceback
import random
import json
import argparse
from typing import List
import asyncio
from aiohttp import ClientSession
import aiopg
import asyncpg
from tqdm import tqdm

from .config import *

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

def is_json(text: str) -> bool:
    try:
        text = json.loads(text)
        if 'input' in text:
            return True
        else:
            return False
    except Exception:
        return False


async def fetch_data(keyword, try_times=1):
    if try_times > 10:
        print("Warning: keyword %s has be fetched %d times" % (keyword, try_times-1))
        return None

    proxy = await fetch_proxy()
    try:
        proxy_url = "http://{}".format(proxy)
        async with ClientSession(headers=headers) as session:
            async with session.get(get_url(keyword), proxy=proxy_url, timeout=10) as response:
                if response.status == 200:
                    text = await response.text()
                    if is_json(text):
                        return keyword, text
                raise RuntimeError('raise for try again.')
    except BaseException as e:
        print(e)
        #traceback.print_exc(file=sys.stdout)
        #await delete_proxy(proxy)
        return await fetch_data(keyword, try_times + 1)


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

async def save(db_conn, buffer, direction):
    try:
        async with db_conn.transaction():
            await db_conn.executemany("INSERT INTO youdao_bilingual (keyword, direction, data) VALUES ($1, $2, $3)",
                                      [(word, direction, data) for word, data in buffer])
    except BaseException as e:
        print(e)
        #traceback.print_exc(file=sys.stdout)


async def save_aiopg(db_pool, buffer, direction):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            cursor.executemany("INSERT INTO youdao_bilingual (keyword, direction, data) VALUES (%s, %s, %s)",
                               [(word, direction, data) for word, data in buffer])


async def bound(sem: asyncio.Semaphore, func, *args, **kwargs):
    async with sem:
        return await func(*args, **kwargs)


async def main(keywords: List[str], direction: str):
    # create instance of Semaphore
    sem = asyncio.Semaphore(200)

    db_conn = await asyncpg.connect(host='localhost', user='sunqf', database='sunqf')

    async with db_conn.transaction():
        records = await db_conn.fetch('SELECT keyword from youdao_bilingual')
        db_words = set([r['keyword'] for r in records])

    keywords = [word for word in keywords if word not in db_words]
    batch_size = 10000
    for batch in tqdm(range(0, len(keywords), batch_size), total=len(keywords)//batch_size):
        futures = [bound(sem, fetch_data, word, 1) for word in keywords[batch:batch+batch_size]]
        buffer = []
        for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
            result = await future
            if result is None:
                print(result)
            else:
                buffer.append(result)
            if len(buffer) > 100:
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
