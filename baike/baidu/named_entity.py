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
    date = {'生日', '出生时间', '出生日期' '逝世日期', '去世时间', '入党时间'}
    alias = {'别号', '别名', '别称', '本名', '字号', '谥号', '原名'}
    misc = {'年龄', '毕业院校', '身高', '体重', '星座', '民族族群', '性别', '主要成就'}

    def named(self, knowledge: dict) -> object:
        infobox = self.infobox(knowledge)
        if infobox:
            if (len(self.address.intersection(infobox)) > 0 or len(self.date.intersection(infobox)) > 0) and \
                (len(self.alias.intersection(infobox)) > 0 or len(self.misc.intersection(infobox)) > 0):
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
    infos = {'界', '门', '亚门', '纲', '亚纲', '目', '亚目', '科', '亚科', '属', '亚属', '种', '亚种'}
    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and '科学百科生命科学分类' in open_tags:
            return self.name
        infobox = self.infobox(knowledge)
        if infobox and len(self.infos.intersection(infobox)) >= 2:
            return self.name

        return None


# 作品，包括 书，影视作品，歌曲
class Works(Entity):
    name = 'WORK'
    book = {'出版时间', 'ISBN', '页数', '书名', '出版社', '作者'}
    music = {'所属专辑', '歌曲原唱', '填词', '歌曲语言', '演唱者', '创作年代', '作者'}
    movie = {'拍摄地点', '拍摄日期', '导演', '主演', '上映时间', '片长'}
    tv = {'导演', '出品时间', '制片地区', '拍摄地点', '首播时间', '导演', '编剧', '主演', '集数', '制片人', '上映时间'}

    def named(self, knowledge: dict) -> str:
        infobox = self.infobox(knowledge)
        if infobox:
            if len(self.book.intersection(infobox)) > 0 or \
                    len(self.music.intersection(infobox)) > 0 or \
                    len(self.movie.intersection(infobox)) > 0 or \
                    len(self.tv.intersection(infobox)) > 0:
                return self.name
        return None

'''
select key, count(*) as count
  from (
    select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge2
    where (knowledge->'attrs'->'open_tags')::jsonb ?| array['奖项']
        and (knowledge->'attrs'->'infobox')::jsonb ?| array['创立时间', '颁发机构', '颁奖地点', '举办者', '历届得主', '获奖人', '提名单位', '颁发时间', '奖励范围', '开始评奖', '表扬对象']) as keys
  group by key
  order by count
'''


class Award(Entity):
    name = 'AWARD'
    tags = set(['奖项'])
    attrs = set(['创立时间', '颁发机构', '颁奖地点', '举办者', '历届得主',
                 '获奖人', '提名单位', '颁发时间', '奖励范围', '开始评奖', '表扬对象'])
    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.open_tags(knowledge)
        if open_tags and infobox:
            if len(self.tags.intersection(open_tags)) > 0 and len(self.attrs.intersection(open_tags)):
                return self.name
        return None

'''
select key, count(*) as count
  from (
    select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge2
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['语系']
      and (knowledge->'attrs'->'open_tags')::jsonb ?| array['语言']) as keys
  group by key
  order by count
'''
class Language(Entity):
    name = 'LANGUAGE'

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if '语系' in infobox and '语言' in open_tags:
                return self.name
        return None

# 事件
class Event(Entity):
    name = 'EVENT'
    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if '时间' in infobox and '历史事件' in open_tags:
                return self.name
        return None

# 专业， 学科
''' 
    select *
    from baike_knowledge2
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['学科代码', '授予学位']
'''


class Subject(Entity):
    name = 'SUBJECT'
    attrs = set(['学科代码', '授予学位'])

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.attrs.intersection(infobox)) > 0:
                return self.name
        return None

'''
select *
    from baike_knowledge2
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['政治体制', '国歌', '国家领袖']
'''
class Country(Entity):
    name = 'COUNTRY'
    attrs = set(['政治体制', '国歌', '国家领袖'])
    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.attrs.intersection(infobox)) > 0:
                return self.name
        return None

class Constellation(Entity):
    name = 'CONSTELLATION'
    tags = set(['科学百科天文学分类', '天体'])
    attrs = set(['绕转周期', '星座', '距离', '发现时间', '发现者', '直径', '距地距离', '公转周期'])
    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.tags.intersection(open_tags)) > 0 and len(self.attrs.intersection(infobox)) > 0:
                return self.name
        return None


'''
class Food(Entity):
    name = 'FOOD'
    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
'''

# 交通线路
class TrafficLine(Entity):
    name = 'TRAFFIC_LINE'
    attrs = set(['国家编号', '编号', '起点', '终点', '全程'])
    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if '交通线路' in open_tags and len(self.attrs.intersection(infobox)) > 0:
                return self.name

        return None

# 车站
class Station(Entity):
    name = 'STATTION'
    attrs = set(['车站等级', '车站位置', '车站地址', '隶属铁路局', '途经路线', '规划线路', '车站编号', '车站性质'])

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if '路线' in open_tags and len(self.attrs.intersection(infobox)) > 0:
                return self.name

        return None


class CommonWord(Entity):
    name = 'O'

    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and (len(open_tags) == 1 and ('成语' in open_tags or '字词' in open_tags) or
                          (len(open_tags) >= 2 and '字词' in open_tags and '语言' in open_tags)):
            return self.name
        return None


entities = [Location(), Person(), Organization(),
            ChemicalSubstance(), Disease(), Species(),
            Works(), Award(), Language(), Country(), Subject(),
            Constellation(), Station(), TrafficLine(), CommonWord()]


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
            if parent.name == 'a' and 'href' in parent.attrs:
                if parent.attrs['href'] in url2type:
                    type = url2type[parent.attrs['href']]
                    url2count[parent.attrs['href']] += 1
                    yield '[[[{}|||{}|||{}]]]'.format(text, type, parent.attrs['href'])
                else:
                    yield '[[[{}|||{}|||{}]]]'.format(text, '*', parent.attrs['href'])
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
            a.attrs['href'] = new_url
            if new_url in url2type:
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


if __name__ == '__main__':
    import asyncio
    import sys

    loop = asyncio.get_event_loop()
    url2entity = loop.run_until_complete(extract_entity())

    print('labeling links in html.')
    with open(sys.argv[1], 'w') as data:
        url2count = loop.run_until_complete(extract_label(url2entity, data, loop))

    with open('entity.count', 'w') as data:
        for url, count in url2count.most_common():
            data.write('{}\t{}\t{}'.format(url, url2entity[url], count))