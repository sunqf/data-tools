#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from typing import Mapping, TextIO, List, Set, Tuple
from collections import Counter
import concurrent
from .. import utils
import json
from bs4 import BeautifulSoup, PageElement, NavigableString, Tag
from urllib.parse import urljoin, unquote


# reference https://thesai.org/Downloads/Volume5No7/Paper_25-Identifying_and_Extracting_Named_Entities.pdf

class Entity:
    @staticmethod
    def walk(knowledge: dict, path: list):
        curr = knowledge
        for key in path:
            if isinstance(curr, dict) and key in curr:
                curr = curr[key]
            else:
                return None
        return curr

    def infobox(self, knowledge):
        return self.walk(knowledge, ['attrs', 'infobox'])

    def open_tags(self, knowledge):
        return self.walk(knowledge, ['attrs', 'open_tags'])

    def named(self, knowledge: dict) -> str:
        raise NotImplementedError


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key 
    from baike_knowledge2
    where ( knowledge->'attrs'->'infobox' )::jsonb ?| array['邮政区码', '面积', '人口', '坐标']) as keys
  group by key
  order by count
'''


class Location(Entity):
    name = 'LOCATION'
    population = {'人口'}
    area = {'面积', '耕地面积'}
    address = {'地理位置', '小区地址', '坐标'}
    NO = {'电话区号', '邮政区码', '邮编', '邮政编码', '车牌代码'}

    def named(self, knowledge: dict) -> str:
        infobox = self.infobox(knowledge)
        if infobox:
            if len(self.address.intersection(infobox)) > 0 and \
                    (len(self.area.intersection(infobox)) > 0 or
                     len(self.NO.intersection(infobox)) > 0):
                return self.name
        return None


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge2
    where ( knowledge->'attrs'->'infobox' )::jsonb ?| array['出生地']) as keys
  group by key
  order by count
'''


class Person(Entity):
    name = 'PERSON'
    address = {'出生地', '国籍', '籍贯', '祖籍', '现居地', '现居'}
    date = {'生日', '出生时间', '逝世日期', '去世时间', '入党时间'}
    alias = {'别号', '别名', '别称', '本名', '字号', '谥号', '原名'}
    misc = {'年龄', '毕业院校', '身高', '体重', '星座', '民族族群', '性别'}

    def named(self, knowledge: dict) -> object:
        infobox = self.infobox(knowledge)
        if infobox:
            if len(self.address.intersection(infobox)) > 0 and \
                    len(self.date.intersection(infobox)) > 0 and \
                    (len(self.alias.intersection(infobox)) > 0 or
                     len(self.misc.intersection(infobox)) > 0):
                return self.name
        return None


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge2
    where (knowledge->'attrs'->'open_tags')::jsonb ?| array[
                                                  '组织机构', '教育机构', '科研机构', '旅游机构', '政府机构', '国家机构', '法律机构',
                                                  '医疗机构', '培训机构', '文化机构', '公共机构'
                                                  '社会团体']
          or (knowledge->'attrs'->'infobox' )::jsonb ?| array['公司名称']) as keys
  group by key
  order by count
'''


class Organization(Entity):
    name = 'ORGANIZATION'
    found_date = {'成立时间', '创办时间', '成立于', '建立时间', '创建时间',
                  '创立时间', '成立日期', '创建于', '组建时间', }
    address = {'总部地点', '总部地址', '地点', '地址', '地理位置', '学校地址', '医院地址'}

    NO = {'社会信用代码', '组织机构代码'}
    company_info = {'公司名称', '公司类型', '发照时间', '公司性质',
                    '创始人', '董事长', 'CEO', '法人代表', '总经理',
                    '股票代码', '证券代码'}
    school_info = {'学校类型', '现任校长', '主要院系', '知名校友', '学校代码'}

    hospital_info = {'医院院长', '医院类别', '医院性质', '医院等级', '医院名称', '医院性质', '医保定点'}
    misc_info = {'业务主管', '主席', '现任院长', '拥有者', '局长', '党委书记', '创办人',
                 '组织状态', '主管单位', '主管部门'}

    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and '中国高校' in open_tags:
            return self.name

        infobox = self.infobox(knowledge)
        if infobox:
            if len(self.found_date.intersection(infobox)) > 0 and \
                    len(self.address.intersection(infobox)) > 0 and \
                    (len(self.company_info.intersection(infobox)) > 0 or
                     len(self.school_info.intersection(infobox)) > 0 or
                     len(self.hospital_info.intersection(infobox)) > 0 or
                     len(self.NO.intersection(infobox)) > 0 or
                     len(self.misc_info.intersection(infobox)) > 0):
                return self.name
        return None


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge2
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['CAS号', 'CAS RN', '分子式']) as keys
  group by key
  order by count
'''


class ChemicalSubstance(Entity):
    name = 'CHEMICAL_SUBSTANCE'
    formula = {'分子式', '化学式', '结构式'}
    NO = {'CAS', 'CAS号', 'CAS登录号', 'CAS No', 'CAS NO', 'CAS NO.'
          'EINECS号', 'EINECS', 'EINECS登录号',
          'PubChem号'}

    def named(self, knowledge: dict) -> str:
        infobox = self.infobox(knowledge)
        if infobox:
            if len(self.formula.intersection(infobox.keys())) > 0 \
                    and len(self.NO.intersection(infobox.keys())) > 0:
                return self.name
        return None


class Disease(Entity):
    name = 'DISEASE'
    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and '科学百科疾病症状分类' in open_tags:
            return self.name
        return None


class Species(Entity):
    name = 'SPECIES'
    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and '科学百科生命科学分类' in open_tags:
            return self.name
        return None


entities = [Location(), Person(), Organization(),
            ChemicalSubstance(), Disease(), Species()]


async def extract_entity():
    reader = await utils.connect()
    url2entity = dict()
    async with reader.transaction():
        async for record in reader.cursor('select url, knowledge from baike_knowledge2 where type=\'baidu_baike\''):
            url = record['url']
            knowledge = json.loads(record['knowledge'])
            names = []
            for entity in entities:
                name = entity.named(knowledge)
                if name:
                    names.append(name)
            if len(names) == 1:
                url2entity[url] = names[0]
    return url2entity

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
    html = utils.clean_tag(html)
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


async def extract_label(url2entity: Mapping, output: TextIO, loop):
    url2count = Counter()
    reader = await utils.connect()
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        async with reader.transaction():
            async for record in reader.cursor('select url, html from baike_html where type=\'baidu_baike\''):
                url = record['url']
                html = utils.decompress(record['html'])
                for sentence in set([sentence for labeled in label(url2entity, url2count, url, html)
                                     for sentence in utils.splitter(labeled) if sentence.find('[[[') > 0]):
                    output.write(sentence + '\n')
    return url2count


import asyncio
loop = asyncio.get_event_loop()
url2entity = loop.run_until_complete(extract_entity())

print('labeling links in html.')
with open('entity.data', 'w') as data:
    url2count = loop.run_until_complete(extract_label(url2entity, data, loop))

with open('entity.count', 'w') as data:
    for url, count in url2count.most_common():
        data.write('{}\t{}\t{}'.format(url, url2entity[url], count))