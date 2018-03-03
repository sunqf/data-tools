#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup, Tag

from urllib import parse
from corpus.util.config import headers
from collections import Iterable
import json
import asyncio
import asyncpg


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


def extract(html: str):
    html = build_tree(html)

    title = html.select_one('title').text
    keywords = html.select_one('meta[name=keywords]').attrs['content']

    attrs = {}

    '''
    polysemant_list = 'body > div.body-wrapper > div.before-content > div > ul'
    if len(polysemant_list) > 0:
        attrs['polysemant'] = [ for tag in polysemant_list]
    '''

    # poster
    poster_tag = html.find(class_='poster')
    if poster_tag:
        title = poster_tag.select_one('dd.lemmaWgt-lemmaTitle-title > h1')
        if title:
            attrs['lemma_title'] = title.text.strip()

        authority_list = [{'href': tag.attrs['href'], 'name': tag.text.strip()}
                          for tag in poster_tag.select('div.authorityListPrompt > div > a')]
        if len(authority_list) > 0:
            attrs['authority_list'] = authority_list

        summary = poster_tag.select_one('div.lemma-summary > div')
        if summary:
            attrs['lemma_summary'] = format_str(summary)

        base_info = [{tag.dt.text.strip(): format_str(tag.dd)} for tag in poster_tag.select('dl-baseinfo')]
        if len(base_info) > 0:
            attrs['base_info'] = base_info

    if 'lemma_title' not in attrs:
        lemma_title = html.select_one('dd.lemmaWgt-lemmaTitle-title > h1')
        if lemma_title:
            attrs['lemma_title'] = lemma_title.text.strip()

    if 'lemma_summary' not in attrs:
        lemma_summary = html.select_one('div.lemma-summary > div')
        if lemma_summary:
            attrs['lemma_summary'] = format_str(lemma_summary)

    if 'base_info' not in attrs:
        names = [tag.text.strip() for tag in html.select('dt.basicInfo-item')]
        values = [format_str(tag) for tag in html.select('dd.basicInfo-item')]
        base_info = {name: value for name, value in zip(names, values)}
        if len(base_info) > 0:
            attrs['base_info'] = base_info


    # todo 是否锁定， 投票计数, 浏览次数

    modified_tag = html.select_one('div.side-content > dl > dd > ul > li:nth-of-type(2)')
    if modified_tag:
        count = modified_tag.text[5:-5]
        attrs['modified_count'] = int(count)

    open_tags = html.select_one('#open-tag-item')
    if open_tags:
        attrs['open_tags'] = open_tags.text.split('，')

    check_type(attrs, Tag)
    return {'title': title, 'keywords': keywords, 'attrs': attrs}


async def extract_all():
    db_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf', command_timeout=60)

    async with db_pool.acquire() as reader, db_pool.acquire() as writer:
        async with reader.transaction():
            async for record in reader.cursor('SELECT url, html from baike_html where type=\'{}\''.format(type)):
                url = record['url']
                html = record['html']
                try:
                    knowledge = json.dumps(extract(html), ensure_ascii=False)
                except Exception as e:
                    print(url)
                    print(e)
                await writer.executemany('INSERT INTO baike_knowledge (url, knowledge, type) VALUES ($1, $2, $3)',
                                         [(url, knowledge, type)])


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete(extract_all())
