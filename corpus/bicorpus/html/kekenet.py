#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio


class Kekenet(HttpCrawler):
    def __init__(self):
        super(Kekenet, self).__init__(max_worker=10,
                                      timeout=10,
                                      max_try_times=10,
                                      type='kekenet',
                                      host='http://www.kekenet.com')

        self.seeds = {'http://www.kekenet.com/read/pic/',
                 'http://www.kekenet.com/read/ss/',
                 'http://www.kekenet.com/read/news/work/',
                 'http://www.kekenet.com/read/news/Economics/',
                 'http://www.kekenet.com/read/news/Sports/',
                 'http://www.kekenet.com/read/news/keji/',
                 'http://www.kekenet.com/read/news/politics/',
                 'http://www.kekenet.com/read/news/entertainment/',
                 'http://www.kekenet.com/read/essay/',
                 'http://www.kekenet.com/read/story/',
                 'http://www.kekenet.com/read/ss/culture/',

                 'http://www.kekenet.com/broadcast/voaspecial/',
                 'http://www.kekenet.com/broadcast/Normal/',
                 'http://www.kekenet.com/broadcast/BBC/',
                 'http://www.kekenet.com/broadcast/CNN/',
                 'http://www.kekenet.com/broadcast/CRI/',
                 'http://www.kekenet.com/broadcast/NPR/',
                 'http://www.kekenet.com/broadcast/Science/',
                 'http://www.kekenet.com/broadcast/ABC/',
                 'http://www.kekenet.com/broadcast/APnews/',
                 'http://www.kekenet.com/broadcast/PBS/',

                 'http://www.kekenet.com/video/movie/',
                 'http://www.kekenet.com/video/tv/',
                 'http://www.kekenet.com/video/englishplay/',
                 'http://www.kekenet.com/song/tingge/',
                 'http://www.kekenet.com/song/fanchang/',

                 'http://www.kekenet.com/Article/chuji/',
                 'http://www.kekenet.com/Article/videolis/',
                 'http://www.kekenet.com/Article/practical/',
                 'http://www.kekenet.com/Article/media/',
                 'http://www.kekenet.com/Article/duwu/',
                 'http://www.kekenet.com/Article/enjoy/',
                 'http://www.kekenet.com/Article/brand/',
                 'http://www.kekenet.com/Article/jiaoxue/',
                 'http://www.kekenet.com/Article/kkspeech/',
                 'http://www.kekenet.com/Article/media/economist/',

                 'http://www.kekenet.com/kouyu/bizoral/',
                 'http://www.kekenet.com/kouyu/training/',
                 'http://www.kekenet.com/kouyu/hyoral/',
                 'http://www.kekenet.com/kouyu/slang/',
                 'http://www.kekenet.com/kouyu/original/',

                 }
        self.article_selector = '#menu-list > li > h2 > a[href]'
        self.page_selector = '.page.th > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url
    '''
    async def crawl(self, finished: Set[str]):
        async for url in self.get_urls():
            if url not in finished:
                content = await self.get_html(url)
                finished.add(url)
                if content:
                    try:
                        content = content.decode('utf-8')
                    except Exception as e:
                        log(WARNING, e)
                    yield url, content

                    html = BeautifulSoup(content, 'html.parser')
                    for sub_url in html.select('#contentText > ul > li > a'):
                        sub_url = urljoin(self.host, sub_url.attrs['href'])
                        if sub_url not in finished:
                            sub_html = await self.get_html(sub_url)
                            if sub_html:
                                yield sub_url, sub_html
                            finished.add(sub_url)
                else:
                    log(WARNING, 'Can\'t get %s' % url)

        '''


if __name__ == '__main__':

    crawler = Kekenet()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()
