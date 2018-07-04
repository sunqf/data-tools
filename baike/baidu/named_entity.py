#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from typing import Mapping, TextIO, List, Set, Tuple
from collections import Counter
import concurrent
from .. import utils
import json
from bs4 import BeautifulSoup, PageElement, NavigableString, Tag
from urllib.parse import urljoin, unquote
from functools import reduce
import traceback

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

    def title(self, knowledge):
        return self.walk(knowledge, ['title'])

    def infobox(self, knowledge):
        return self.walk(knowledge, ['attrs', 'infobox'])

    def open_tags(self, knowledge):
        return self.walk(knowledge, ['attrs', 'open_tags'])

    def named(self, knowledge: dict) -> str:
        raise NotImplementedError


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key 
    from baike_knowledge
    where ( knowledge->'attrs'->'infobox' )::jsonb ?| array['邮政区码', '面积', '人口', '坐标']) as keys
  group by key
  order by count
'''


class Location(Entity):
    name = 'LOCATION'
    population = {'人口'}
    area = {'面积', '耕地面积', '占地总面积', '规划建筑面积'}
    address = {'地理位置', '小区地址', '位于', '坐标', '所属城市', '所属国家'}
    NO = {'电话区号', '邮政区码', '邮编', '邮政编码', '车牌代码'}

    airport = {'机场代码', '航站楼面积', '通航日期', '机场类型', '航站楼面积', '旅客吞吐量', '起降架次'}

    station = {'车站等级', '车站位置', '车站地址', '隶属铁路局', '途经路线', '规划线路', '车站编号', '车站性质'}

    river = {'流域总面积', '河宽', '河长', '所属水系', '流经地区', '流域面积', '世界长河排名', '主要支流'}

    scenic_spots = {'最佳游玩季节', '建议游玩时长', '开放时间', '适宜游玩季节', '景点级别', '门票价格'}

    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if infobox:
            if (len(self.address.intersection(infobox)) > 0 or len(self.population.intersection(infobox)) > 0) and \
                    (len(self.area.intersection(infobox)) > 0 or
                     len(self.NO.intersection(infobox)) > 0):
                return self.name

            if len(self.airport.intersection(infobox)) >= 2:
                return self.name

            if len(self.river.intersection(infobox)) >= 2:
                return self.name

            if len(self.station.intersection(infobox)) >= 2:
                return self.name

            if len(self.scenic_spots.intersection(infobox)) >= 1:
                return self.name

        return None


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge
    where ( knowledge->'attrs'->'infobox' )::jsonb ?| array['出生地']) as keys
  group by key
  order by count
'''


class Person(Entity):
    name = 'PERSON'
    address = {'出生地', '国籍', '籍贯', '祖籍', '现居地', '现居'}
    date = {'生日', '出生时间', '出生日期' '逝世日期', '去世时间', '逝世日期', '入党时间'}
    alias = {'别号', '别名', '别称', '本名', '字号', '谥号', '原名'}
    misc = {'年龄', '毕业院校', '身高', '体重', '星座', '民族族群', '性别', '主要成就', '代表作品', '职业', '所属运动队', '师从'}

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
    from baike_knowledge
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
    address = {'总部地点', '总部地址', '机构地点', '注册地址', '地点', '地址', '地理位置', '学校地址', '医院地址'}

    NO = {'社会信用代码', '组织机构代码', '批准机构'}
    company_info = {'公司名称', '公司类型', '发照时间', '公司性质',
                    '创始人', '董事长', 'CEO', '法人代表', '总经理',
                    '股票代码', '证券代码'}
    school_info = {'学校类型', '现任校长', '主要院系', '知名校友', '学校代码'}

    hospital_info = {'医院院长', '医院类别', '医院性质', '医院等级', '医院名称', '医院性质', '医保定点'}
    misc_info = {'业务主管', '主席', '现任院长', '拥有者', '局长', '党委书记', '创办人',
                 '组织状态', '主管单位', '主管部门', '会长', '名誉会长'}

    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and '中国高校' in open_tags:
            return self.name

        infobox = self.infobox(knowledge)
        if infobox:
            if (len(self.found_date.intersection(infobox)) > 0 or len(self.address.intersection(infobox)) > 0 or len(
                    self.NO.intersection(infobox)) > 0) \
                    and (len(self.company_info.intersection(infobox)) > 0 or
                         len(self.school_info.intersection(infobox)) > 0 or
                         len(self.hospital_info.intersection(infobox)) > 0 or
                         len(self.misc_info.intersection(infobox)) > 0):
                return self.name
        return None


'''
select key, count(*) as count
  from (select json_object_keys(knowledge->'attrs'->'infobox') as key
    from baike_knowledge
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
    attrs = {'发病部位', '就诊科室', '常见发病部位', '常见病因', '主要症状', '多发群体', '传染性'}

    def named(self, knowledge: dict) -> str:
        open_tags = self.open_tags(knowledge)
        if open_tags and '科学百科疾病症状分类' in open_tags:
            return self.name
        infobox = self.infobox(knowledge)
        if infobox and len(self.attrs.intersection(infobox)) > 2:
            return self.name
        return None


class Species(Entity):
    name = 'SPECIES'
    infos = {'界', '门', '亚门', '纲', '亚纲', '目', '亚目', '科', '亚科', '属', '亚属', '种', '亚种'}

    def named(self, knowledge: dict) -> str:
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
    from baike_knowledge
    where (knowledge->'attrs'->'open_tags')::jsonb ?| array['奖项']
        and (knowledge->'attrs'->'infobox')::jsonb ?| array['创立时间', '颁发机构', '颁奖地点', '举办者', '历届得主', '获奖人', '提名单位', '颁发时间', '奖励范围', '开始评奖', '表扬对象']) as keys
  group by key
  order by count
'''


class Award(Entity):
    name = 'AWARD'
    tags = {'奖项'}
    attrs = {'创立时间', '时间', '原则', '立足于', '颁发机构', '颁奖地点', '举办者', '历届得主', '获奖人', '提名单位', '颁发时间', '奖励范围', '开始评奖', '表扬对象'}

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
    from baike_knowledge
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['语系']
      and (knowledge->'attrs'->'open_tags')::jsonb ?| array['语言']) as keys
  group by key
  order by count
'''


class Language(Entity):
    name = 'LANGUAGE'
    attrs = {'语系', '语族'}

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.attrs.intersection(infobox)) > 0 and '语言' in open_tags:
                return self.name
        return None


# 事件
class Event(Entity):
    name = 'EVENT'
    attrs = {'发生时间', '时间', '持续时间', '结束时间', '发生地'}
    tags = {'事件', '历史事件', '外国历史事件', '古代历史事件', '现代历史事件'}

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
    from baike_knowledge
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['学科代码', '授予学位']
'''


class Subject(Entity):
    name = 'SUBJECT'
    attrs = {'学科代码', '授予学位'}

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.attrs.intersection(infobox)) > 0:
                return self.name
        return None


'''
select *
    from baike_knowledge
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['政治体制', '国歌', '国家领袖']
'''


class Country(Entity):
    name = 'COUNTRY'
    attrs = {'政治体制', '国歌', '国家领袖'}

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.attrs.intersection(infobox)) > 0:
                return self.name
        return None


class Constellation(Entity):
    name = 'CONSTELLATION'
    tags = {'科学百科天文学分类', '天体'}
    attrs = {'绕转周期', '星座', '距离', '发现时间', '发现者', '直径', '距地距离', '公转周期'}

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if len(self.tags.intersection(open_tags)) > 0 and len(self.attrs.intersection(infobox)) > 0:
                return self.name
        return None


class Food(Entity):
    name = 'FOOD'
    attrs = {'主要营养成分', '主要食用功效', '是否含防腐剂', '储藏方法', '口味'}

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if infobox:
            if len(self.attrs.intersection(infobox)) > 0:
                return self.name
            if '分类' in infobox and infobox['分类'] == '小吃':
                return self.name
        return None


# 交通线路
class TrafficLine(Entity):
    name = 'TRAFFIC_LINE'
    attrs = {'国家编号', '编号', '起点', '终点', '全程'}

    def named(self, knowledge: dict):
        open_tags = self.open_tags(knowledge)
        infobox = self.infobox(knowledge)
        if open_tags and infobox:
            if '交通线路' in open_tags and len(self.attrs.intersection(infobox)) > 0:
                return self.name

        return None


class CommonWord(Entity):
    name = 'O'
    lang_detector = Language()
    attrs = {'词义', '意思', '释义', '基本解释'}

    def named(self, knowledge: dict) -> str:
        if self.lang_detector.named(knowledge):
            return None
        open_tags = self.open_tags(knowledge)
        if open_tags and (('成语' in open_tags and len(open_tags) == 1) or
                          ('字词' in open_tags and '语言' in open_tags)):
            return self.name

        infobox = self.infobox(knowledge)
        if infobox and len(self.attrs.intersection(infobox)) > 0:
            return self.name

        return None


entities = [Location(), Person(), Organization(),
            ChemicalSubstance(), Disease(), Species(),
            Works(), Award(), Language(), Country(), Subject(),
            Food(), Event(),
            Constellation(), TrafficLine(), CommonWord()]


async def _extract_entity(num_workers, worker_id):
    reader = await utils.connect()
    writer = await utils.connect()
    url2entity = dict()
    async with reader.transaction():
        buffer = []
        async for record in reader.cursor(
                f'select url, knowledge from baike_knowledge where type=\'baidu_baike\' and id % {num_workers} = {worker_id}'):
            url = record['url']
            knowledge = json.loads(record['knowledge'])
            names = []
            for entity in entities:
                name = entity.named(knowledge)
                if name:
                    names.append(name)
            buffer.append((url, names))
            if len(buffer) > 1000:
                try:
                    async with writer.transaction():
                        await writer.executemany(
                            'INSERT INTO url_label (url, label, type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
                            [(url, json.dumps({'entity_type': names}), 'baidu_baike') for url, names in buffer])
                except Exception as e:
                    print(e)
                    traceback.print_exc()

                buffer = []
            if len(names) == 1:
                url2entity[url] = names[0]
        if len(buffer) > 0:
            async with writer.transaction():
                await writer.executemany(
                    'INSERT INTO url_label (url, label, type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
                    [(url, json.dumps({'entity_type': names}), 'baidu_baike') for url, names in buffer])
    return url2entity


def extract_worker(num_workers, worker_id):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(_extract_entity(num_workers, worker_id))


async def extract_entity(loop, num_workers):
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        workers = [loop.run_in_executor(executor, extract_worker, num_workers, id)
                   for id in range(num_workers)]
        def _merge(d1, d2):
            d1.update(d2)
            return d1
        return reduce(_merge, [await w for w in asyncio.as_completed(workers)])


def label(url, html: str):
    def _label(node: PageElement):
        if isinstance(node, NavigableString):
            text = node.strip()
            parent = node.parent
            if parent.name == 'a' and 'href' in parent.attrs:
                yield '[[[{}|||{}]]]'.format(text, parent.attrs['href'])
            else:
                yield text
        elif isinstance(node, Tag):
            for child in node.children:
                yield from _label(child)

    html = BeautifulSoup(html, 'html.parser')
    html = utils.clean_tag(html)
    paras = set()
    for para in html.select('div.para'):
        href_len = 0
        for a in para.select('a[href]'):
            new_url = unquote(urljoin(url, a.attrs['href']))
            a.attrs['href'] = new_url
            href_len += len(a.text)

        if len(para.text) - href_len > 0:
            paras.add(para)

    return [''.join(_label(para)) for para in paras]


async def _extract_label(output, num_workers, worker_id):
    reader = await utils.connect()
    async with reader.transaction():
        with open('{}.{}'.format(output, worker_id), 'w') as output:
            async for record in reader.cursor(
                    f'select url, html from baike_html where type=\'baidu_baike\' and id % {num_workers} = {worker_id}'):
                url = record['url']
                print(url)
                html = utils.decompress(record['html'])
                for sentence in set([sentence for labeled in label(url, html)
                                     for sentence in utils.splitter(labeled) if sentence.find('[[[') >= 0]):
                    output.write(sentence + '\n')


def label_worker(output, num_workers, worker_id):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(_extract_label(output, num_workers, worker_id))


async def extract_label(loop, output, num_workers):
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        workers = [loop.run_in_executor(executor, label_worker, output, num_workers, id)
                   for id in range(num_workers)]


if __name__ == '__main__':
    import asyncio
    import sys

    loop = asyncio.get_event_loop()
    # url2entity = loop.run_until_complete(extract_entity(loop, 10))


    print('labeling links in html.')
    loop.run_until_complete(extract_label(loop, sys.argv[1], 5))

'''
    with open('entity.count', 'w') as data:
        for url, count in url2count.most_common():
            data.write('{}\t{}\t{}'.format(url, url2entity[url], count))
'''
