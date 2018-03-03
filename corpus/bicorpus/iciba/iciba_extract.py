#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import re
import asyncio
import asyncpg
from bs4 import BeautifulSoup
from tqdm import tqdm
import concurrent


# Do ' nt think about the way things might has been. -> Do'nt think about the way things might has been.
single_quote_raw, single_quote_fixed = re.compile("([a-zA-Z]+) '[ ]*(s|m|re|ve|d|nt|ll)"), r"\1'\2"

ANY_THING = re.compile('.*')

def fix_en(text):
    return single_quote_raw.sub(single_quote_fixed, text)


def extract_html(text):
    html = BeautifulSoup(text, 'html.parser')

    def _text(tag):
        t = tag.find(con=ANY_THING)
        return t.attrs['con'] if t else None

    for en, zh, source in zip(html.find_all(class_='stc_en'),
                      html.find_all(class_='stc_cn'),
                      html.find_all(class_='stc_from fl')):

        en = _text(en)
        zh = _text(zh)
        source = source.text.strip()

        if en and zh:
            yield fix_en(en), zh, source


def batch_extract(batch):
    return [(zh, en, source) for row in batch for en, zh, source in extract_html(row)]


async def fetch(loop, executor, db_conn):

    async with db_conn.transaction():
        cursor = await db_conn.cursor('SELECT keyword, direction, data FROM iciba')
        futures = []
        while True:
            batch = await cursor.fetch(20)
            if len(batch) == 0:
                break

            batch = [row['data'] for row in batch]
            future = loop.run_in_executor(executor, batch_extract, batch)
            futures.append(future)

            if len(futures) > 200:
                for future in asyncio.as_completed(futures):
                    yield await future

                futures.clear()

        if len(futures) > 0:
            for future in asyncio.as_completed(futures):
                yield await future

            futures.clear()


async def extract(loop, executor):

    conn_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf')

    async with conn_pool.acquire() as reader:
        async with conn_pool.acquire() as writer:
            async with reader.transaction():
                total = 0
                async for buffer in fetch(loop, executor, reader):
                    total += len(buffer)
                    print(total)
                    try:
                        async with writer.transaction():
                            await writer.executemany("INSERT INTO ch2en (ch, en, url, source) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                                                     [(zh, en, source, 'iciba') for zh, en, source in buffer])

                            buffer.clear()
                    except BaseException as e:
                        print(e)


if __name__ == '__main__':

    pool_executor = concurrent.futures.ProcessPoolExecutor(max_workers=5)
    loop = asyncio.get_event_loop()

    loop.run_until_complete(extract(loop, pool_executor))

