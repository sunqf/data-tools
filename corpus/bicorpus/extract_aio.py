#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import argparse
import re
import json

from concurrent.futures import ProcessPoolExecutor

import asyncio
import asyncpg


arg_parser = argparse.ArgumentParser()

arg_parser.add_argument('--field',
                        type=str,
                        default='blng',
                        help='field name. [blng, collins, ec21, longman, splongman, special, phrase, meida, auth]')


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
            yield (en, zh, 'media')

def list_all(fn, *args, **kwargs):

    return list(fn(*args, **kwargs))

async def extract(loop, func, filed):
    #pool_executor = ProcessPoolExecutor(max_workers=10)
    conn_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf')

    buffer = []

    async with conn_pool.acquire() as select_conn:
        async with select_conn.transaction():
            async for row in select_conn.cursor('SELECT keyword, direction, data FROM youdao_bilingual'):
                keyword = row['keyword']
                direction = row['direction']
                data = json.loads(row['data'])
                #pair = await loop.run_in_executor(pool_executor, list_all, func, data)
                #buffer.extend(pair)
                buffer.extend(func(data))

                if len(buffer) > 1000:
                    async with conn_pool.acquire() as write_conn:
                        async with write_conn.transaction():
                            await write_conn.executemany('INSERT INTO ch2en (ch, en, url, source, field) VALUES ($1, $2, $3, $4, $5)',
                                            [(ch, en, url, 'youdao', filed) for en, ch, url in buffer])
                    buffer.clear()

    if len(buffer) > 0:
        async with conn_pool.acquire() as write_conn:
            async with write_conn.transaction():
                await write_conn.executemany('INSERT INTO ch2en (ch, en, url, source, filed) VALUES ($1, $2, $3, $4, %5)',
                                             [(ch, en, url, 'youdao', filed) for en, ch, url in buffer])
        buffer.clear()


field_mapping = {'blng': extract_blng_sents,
                 'media': extract_media,
                 'auth': extract_auth
                 }


if __name__ == '__main__':

    args = arg_parser.parse_args()

    if args.field not in field_mapping:
        print('Argument field not supported.')
        exit(-1)

    func = field_mapping[args.field]

    loop = asyncio.get_event_loop()

    loop.run_until_complete(extract(loop, func, args.field))

