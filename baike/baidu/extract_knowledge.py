#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup, Tag
import zlib
from urllib import parse
from corpus.util.config import headers
from collections import Iterable
import json
import asyncio
import asyncpg
import traceback
import concurrent
from typing import Union, Iterable, AsyncIterable, Generator, AsyncGenerator


type = 'baidu_baike'
host = 'https://baike.baidu.com'


def build_tree(html):
    tree = BeautifulSoup(html, 'html.parser')
    for a in tree.select('a[href]'):
        a.attrs['href'] = parse.urljoin(host, a.attrs['href'])
    return tree


def format_str(tag):
    tag.name = 'div'
    tag.attrs.clear()
    return str(tag)


def check_type(data, checked):
    #print(data.__class__.__name__, data)
    if isinstance(data, checked):
        print(checked, data)
    elif isinstance(data, dict):
        for k, v in data.items():
            check_type(k, checked)
            check_type(v, checked)
    elif isinstance(data, Iterable) and not isinstance(data, str):
        for item in data:
            check_type(item, checked)

def format_attr_name(word: str) -> str:
    return word.replace('\xa0', '').strip()

def extract(html: str):
    html = build_tree(html)

    title = html.select_one('title').text
    keywords_tag = html.select_one('meta[name=keywords]')
    keywords = keywords_tag.attrs['content'] if keywords_tag is not None else []

    attrs = {}

    '''
    polysemant_list = 'div.polysemant-list.polysemant-list-normal > ul > li'
    '''

    # poster
    poster_tag = html.find(class_='poster')
    if poster_tag:
        lemma_title = poster_tag.select_one('dd.lemmaWgt-lemmaTitle-title > h1')
        if title:
            attrs['lemma_title'] = lemma_title.text.strip()

        authority_list = [{'href': tag.attrs['href'], 'name': tag.text.strip()}
                          for tag in poster_tag.select('div.authorityListPrompt > div > a')]
        if len(authority_list) > 0:
            attrs['authority_list'] = authority_list

        summary = poster_tag.select_one('div.lemma-summary > div')
        if summary:
            attrs['lemma_summary'] = format_str(summary)

        infobox = [{format_attr_name(tag.dt.text): format_str(tag.dd)} for tag in poster_tag.select('dl-baseinfo')]
        if len(infobox) > 0:
            attrs['infobox'] = infobox

    if 'lemma_title' not in attrs:
        lemma_title = html.select_one('dd.lemmaWgt-lemmaTitle-title > h1')
        if lemma_title:
            attrs['lemma_title'] = lemma_title.text.strip()

    if 'lemma_summary' not in attrs:
        lemma_summary = html.select_one('div.lemma-summary > div')
        if lemma_summary:
            attrs['lemma_summary'] = format_str(lemma_summary)

    if 'infobox' not in attrs:
        names = [format_attr_name(tag.text) for tag in html.select('dt.basicInfo-item')]
        values = [format_str(tag) for tag in html.select('dd.basicInfo-item')]
        infobox = {name: value for name, value in zip(names, values)}
        if len(infobox) > 0:
            attrs['infobox'] = infobox


    # todo 是否锁定， 投票计数, 浏览次数

    modified_tag = html.select_one('dl.side-box.lemma-statistics > dd > ul > li:nth-of-type(2)')
    if modified_tag:
        count = modified_tag.text[5:-5]
        attrs['modified_count'] = int(count)

    open_tags = html.select_one('#open-tag-item')
    if open_tags:
        attrs['open_tags'] = [t.strip() for t in open_tags.text.split('，')]

    check_type(attrs, Tag)
    return {'title': title, 'keywords': keywords, 'attrs': attrs}


def extract_text(html):
    html = build_tree(html)
    for div in html.select('div.para'):
        if len(div.text) > 30 and div.select_one('a[href]') is not None:
            yield format_str(div)


queue = asyncio.Queue(maxsize=100)

async def fetch_worker(batch_size):
    reader = await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)
    batch = []
    async with reader.transaction():
        async for record in reader.cursor('SELECT url, html from baike_html2 where type=\'{}\''.format(type)):
            url = record['url']
            html = zlib.decompress(record['html'])
            batch.append((url, record['html']))
            if len(batch) >= batch_size:
                await queue.put(batch)
                batch = []

        if len(batch) > 0:
            await queue.put(batch)

def decomp_ext_comp(data):
    res = []
    for url, html in data:
        html = zlib.decompress(html).decode()
        # print(url)
        try:
            knowledge = extract(html)
            knowledge = json.dumps(knowledge, ensure_ascii=False)
            res.append((url, knowledge))
        except Exception as e:
            print(url, e)
            traceback.print_exc()
    return res


async def extract_worker(loop, executor):
    writer = await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)
    while True:
        batch = await queue.get()
        results = await loop.run_in_executor(executor, decomp_ext_comp, batch)
        async with writer.transaction():
            await writer.executemany(
                'INSERT INTO baike_knowledge2 (url, knowledge, type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
                [(url, knowledge, type) for url, knowledge in results])
        queue.task_done()

async def run():
    loop = asyncio.get_event_loop()
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        source = asyncio.ensure_future(fetch_worker(batch_size=100))
        tasks = [asyncio.ensure_future(extract_worker(loop, executor)) for i in range(5)]
        await source
        await queue.join()
        await asyncio.sleep(300)
        for task in tasks:
            task.cancel()


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())