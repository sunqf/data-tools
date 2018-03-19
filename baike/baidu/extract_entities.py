#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .. import db
from collections import Counter
from pprint import pprint
import json
import asyncio
import zlib
from bs4 import BeautifulSoup, NavigableString, PageElement, Tag
from typing import Mapping, TextIO, List, Set, Tuple
from urllib.parse import urljoin, unquote
import concurrent

person = {""" knowledge->'attrs'->'infobox' """: """['出生地']"""}

organization = {""" knowledge->'attrs'->'open_tags' """: """[
                                                  '组织机构', '教育机构', '科研机构', '旅游机构', '政府机构', '国家机构', '法律机构',
                                                  '医疗机构', '培训机构', '文化机构', '公共机构'
                                                  '社会团体']"""
                }
company = {""" knowledge->'attrs'->'infobox' """: """['公司名称']"""}
# product = {""" knowledge->'attrs'->'open_tags' """: """['科技产品', '化妆品', '生活用品', '家具', '汽车']"""}
location = {""" knowledge->'attrs'->'infobox' """: """['地理位置']"""}

entities = {'person': person, 'company': company, 'location': location, 'organization': organization}


async def extract_entity():
    reader = await db.connect()
    entity_dicts = {}
    for name, patterns in entities.items():
        entity_dicts[name] = set()
        for field, values in patterns.items():
            async with reader.transaction():
                query = 'select url from baike_knowledge2 where ({})::jsonb ?| array{}'.format(field, values)
                print(query)
                async for record in reader.cursor(query):
                    entity_dicts[name].add(record['url'])

    url_counter = Counter()
    for name, urls in entity_dicts.items():
        url_counter.update(urls)

    replicated_urls = set([url for url, count in url_counter.items() if count > 1])

    # 去除包含在多个类型中的urls
    for name, urls in entity_dicts.items():
        urls.difference_update(replicated_urls)

    return entity_dicts


def label(url2type: Mapping[str, str], url2count: Counter, url, html: str):
    def _label(node: PageElement):
        if isinstance(node, NavigableString):
            text = node.strip()
            parent = node.parent
            if parent.name == 'a' and 'href' in parent.attrs and parent.attrs['href'] in url2type:
                type = url2type[parent.attrs['href']]
                url2count[parent.attrs['href']] += 1
                yield '[[[{}|||{}|||{}]]]'.format(text, type, parent.attrs['href'])
            else:
                yield text
        elif isinstance(node, Tag):
            for child in node.children:
                yield from _label(child)

    html = BeautifulSoup(html, 'html.parser')
    html = clean(html)
    paras = set()
    for para in html.select('div.para'):
        found = False
        for a in para.select('a[href]'):
            new_url = unquote(urljoin(url, a.attrs['href']))
            if new_url in url2type:
                a.attrs['href'] = new_url
                found = True

        if found and len(para.text) > 20:
            paras.add(para)

    return [''.join(_label(para)) for para in paras]


def decompress_and_extract(url2entity, batch: List[Tuple[str, str]]) -> Set[str]:
    sentences = set()
    url2count = Counter()
    for url, html in batch:
        html = zlib.decompress(html).decode()
        for labeled in label(url2entity, url2count, url, html):
            for sentence in splitter(labeled):
                if sentence.find('[[[') > 0:
                    sentences.add(sentence)

    return sentences, url2count


async def extract_label(url2entity: Mapping, output: TextIO, loop):
    url2count = Counter()
    reader = await db.connect()
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        async with reader.transaction():
            async for record in reader.cursor('select url, html from baike_html where type=\'baidu_baike\''):
                url = record['url']
                html = zlib.decompress(record['html']).decode()
                for sentence in set([sentence for labeled in label(url2entity, url2count, url, html)
                                     for sentence in splitter(labeled) if sentence.find('[[[') > 0]):
                    output.write(sentence + '\n')
    return url2count

loop = asyncio.get_event_loop()
results = loop.run_until_complete(extract_entity())
with open('entity.urls', 'w') as f:
    for name, urls in results.items():
        f.write('{}\t{}\n'.format(name, '\t'.join(urls)))
        print('{}\t{}'.format(name, len(urls)))

print('labeling links in html.')
url2entity = {url: type for type, urls in results.items() for url in urls}
with open('entity.data', 'w') as data:
    url2count = loop.run_until_complete(extract_label(url2entity, data, loop))

with open('entity.count', 'w') as data:
    for url, count in url2count.most_common():
        data.write('{}\t{}\t{}'.format(url, url2entity[url], count))
