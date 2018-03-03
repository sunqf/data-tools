#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class Enread(HttpCrawler):
    def __init__(self):
        super(Enread, self).__init__(max_worker=1, timeout=20, max_try_times=10,
                                     type='enread', host='http://www.enread.com/')
        '''
        self.seeds = {
            'http://www.enread.com/essays/', # 英语美文
            'http://www.enread.com/science/', # 英语科普
            'http://www.enread.com/novel/', # 英语小说

            'http://www.enread.com/news/business/',  # 财经新闻
            'http://www.enread.com/news/politics/',  # 时事要闻
            'http://www.enread.com/news/life/',  # 社会生活
            'http://www.enread.com/news/cultureandedu/',  # 文化教育
            'http://www.enread.com/news/sciandtech/',  # 科学技术
            'http://www.enread.com/news/sports/',  # 体育新闻
            'http://www.enread.com/story/fairy/',  # 童话故事
            'http://www.enread.com/story/love/',  # 情感故事
            'http://www.enread.com/story/folk/',  # 民间故事
            'http://www.enread.com/story/fable/',  # 寓言故事
            'http://www.enread.com/story/shuangyu/',  # 双语故事
            'http://www.enread.com/story/biography/',  # 名人传记
            'http://www.enread.com/humors/kids/',  # 儿童笑话
            'http://www.enread.com/humors/Religious/',  # 宗教笑话
            'http://www.enread.com/humors/animals/',  # 动物笑话
            'http://www.enread.com/humors/lawyers/',  # 司法笑话
            'http://www.enread.com/humors/dumbpeople/',  # 愚人笑话
            'http://www.enread.com/humors/sexrelated/',  # 成人笑话
            'http://www.enread.com/humors/shuangyu/',  # 双语笑话
            'http://www.enread.com/humors/blonde/',  # 女性笑话
            'http://www.enread.com/entertainment/horoscope/',  # 星座英语
            'http://www.enread.com/entertainment/bookreview/',  # 图书评论
            'http://www.enread.com/entertainment/festival/',  # 节日大观
            'http://www.enread.com/entertainment/todayinhistory/',  # 史上今日
            'http://www.enread.com/entertainment/culture/',  # 西方文化
            'http://www.enread.com/entertainment/songs/',  # 英文歌曲
            'http://www.enread.com/entertainment/cartoons/',  # 幽默漫画
            'http://www.enread.com/entertainment/life/',  # 生活英语
            'http://www.enread.com/entertainment/travel/',  # 世界风光
            'http://www.enread.com/entertainment/fashion/',  # 时尚英语
            'http://www.enread.com/entertainment/riddles/',  # 英语智力谜语
            'http://www.enread.com/entertainment/rkl/',  # 绕口令
            'http://www.enread.com/entertainment/movies/',  # 影视英语
            'http://www.enread.com/entertainment/hyy/',  # 黑英语
            'http://www.enread.com/poems/children/',  # 儿童诗歌
            'http://www.enread.com/poems/zhongying/',  # 中英对照赏析
            'http://www.enread.com/poems/logion/',  # 名人名言
            'http://www.enread.com/poems/famous/',  # 名人诗歌
            'http://www.enread.com/poems/famous/emily/',  # 埃米莉·迪金森
            'http://www.enread.com/poems/famous/Shakespeare/',  # 莎士比亚
            'http://www.enread.com/poems/famous/classic/',  # 经典赏析
            'http://www.enread.com/poems/famous/pound/',  # 艾茲拉·庞德
            'http://www.enread.com/poems/famous/cummings/',  # 卡明斯
            'http://www.enread.com/poems/famous/lowell/',  # 艾米·洛威尔

            'http://www.enread.com/job/resume/',  # 职场英语
            'http://www.enread.com/job/words/',  # 专业词汇
            'http://www.enread.com/job/trade/',  # 商贸英语
            'http://www.enread.com/job/sport/',  # 体育英语
            'http://www.enread.com/job/law/',  # 法律英语
            'http://www.enread.com/job/ywyy/',  # 医务英语
        }
        self.article_selector = 'body > div.wrap > div.main > table > tbody > tr > td.left > div > div > div.list > div > div.title > h2 > a[href]'
        self.page_selector = 'body > div.wrap > div.main > table > tbody > tr > td.left > div > div > div.list > div.page > li > a[href]'
    '''

        self.article_selector = 'body > div.wrap > div.main > table > tbody > tr > td.left > div > div > div.list > div > div.title > h2 > a[href]'
        self.page_selector = 'a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(set([self.host]), self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    crawler = Enread()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()