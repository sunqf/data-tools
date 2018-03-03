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
                        help='field name. [blng, meida, auth]')


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


async def extract(loop, func, field):
    conn_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf')

    async with conn_pool.acquire() as reader:
        async with conn_pool.acquire() as writer:
            async with reader.transaction():
                async for row in reader.cursor('SELECT data FROM youdao_bilingual'):
                    data = json.loads(row['data'])

                    pairs = list(func(data))
                    if len(pairs) > 0:
                        async with writer.transaction():
                            await writer.executemany('INSERT INTO ch2en (ch, en, url, source) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING',
                                            [(ch, en, url, 'youdao') for en, ch, url in correct(pairs)])


field_mapping = {'blng': extract_blng_sents,
                 'media': extract_media,
                 #'auth': extract_auth
                 }


if __name__ == '__main__':

    args = arg_parser.parse_args()

    if args.field not in field_mapping:
        print('Argument field not supported.')
        exit(-1)

    func = field_mapping[args.field]

    loop = asyncio.get_event_loop()

    loop.run_until_complete(extract(loop, func, args.field))

