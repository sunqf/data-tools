#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .. import db
from collections import Counter
from pprint import pprint
import json
import asyncio
import zlib
from bs4 import BeautifulSoup, NavigableString, PageElement, Tag
from typing import Mapping, TextIO
from urllib.parse import urljoin, unquote
person = {""" knowledge->'attrs'->'open_tags' """: """['人物']"""}

organization = {""" knowledge->'attrs'->'open_tags' """: """['中国高校', '公办高校', '研究生院高校', '211高校', '985高校',
                                                  '综合类高校', '教育部隶属高校', '北京高校', '本科高校', '大学',
                                                  '学校', '中学', '初中', '组织机构', '教育机构', '社会团体']"""
                }
company = {""" knowledge->'attrs'->'infobox' """: """['公司名称']"""}
# product = {""" knowledge->'attrs'->'open_tags' """: """['科技产品', '化妆品', '生活用品', '家具', '汽车']"""}
location = {""" knowledge->'attrs'->'infobox' """: """['地理位置']""",
            """ knowledge->'attrs'->'open_tags' """: """['地点']"""}


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


def ChineseSplitter():
    ends = '。！？'
    pairs = {'(': ')', '{': '}', '[': ']', '<': '>',  '《': '》', '（': '）', '【': '】', '“': '”'}
    left2id = {}
    right2id = {}
    sames = {'"', '\''}
    same2id = {}
    for i, (k, v) in enumerate(pairs.items()):
        left2id[k] = i
        right2id[v] = i

    for i, s in enumerate(sames):
        same2id[s] = i

    def split_sentence(data: str):
        same_count = [0] * len(same2id)
        pair_count = [0] * len(left2id)

        begin = 0
        for pos, char in enumerate(data):
            if char in ends:
                if sum(same_count) == 0 and sum(pair_count) == 0:
                    yield ''.join(data[begin:pos + 1])
                    begin = pos + 1
            elif char in left2id:
                pair_count[left2id[char]] += 1
            elif char in right2id:
                pair_count[right2id[char]] -= 1
            elif char in same2id:
                count = same_count[same2id[char]]
                same_count[same2id[char]] = (count + 1) % 2

        if begin < len(data):
            yield ''.join(data[begin:])

    return split_sentence



def label(url2type: Mapping[str, str], url2count: Counter, url, html: str):
    def _label(node: PageElement):
        if isinstance(node, NavigableString):
            text = node
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

    divs = set()
    for a in html.select('a[href]'):
        if a.parent.name == 'div' and a.parent not in divs:
            new_url = unquote(urljoin(url, a.attrs['href']))
            if new_url in url2type:
                a.attrs['href'] = new_url
                if len(a.parent.text) > 20:
                    divs.add(a.parent)
    def clean(tag: Tag):
        for sup in tag.select('sup'):
            sup.decompose()

        for sup_ref in tag.select('a.sup-anchor'):
            sup_ref.decompose()
        return tag

    divs = [clean(div) for div in divs]
    return [''.join(_label(div)) for div in divs]


splitter = ChineseSplitter()


async def extract_label(url2entity, url2count, output: TextIO):
    reader = await db.connect()
    async with reader.transaction():
        async for record in reader.cursor('select url, html from baike_html where type=\'baidu_baike\''):
            url = record['url']
            html = zlib.decompress(record['html']).decode()
            for labeled in label(url2entity, url2count, url, html):
                for sentence in splitter(labeled):
                    if sentence.find('[[[') > 0:
                        output.write(labeled + '\n')

loop = asyncio.get_event_loop()
results = loop.run_until_complete(extract_entity())
with open('entity.urls', 'w') as f:
    for name, urls in results.items():
        f.write('{}\t{}\n'.format(name, '\t'.join(urls)))

url2entity = {url:type for type, urls in results.items() for url in urls}
url2count = Counter()
with open('entity.data', 'w') as data:
    loop.run_until_complete(extract_label(url2entity, url2count, data))

with open('entity.count', 'w') as data:
    for url, count in url2count.most_common():
        data.write('{}\t{}\t{}'.format(url, url2entity[url], count))
