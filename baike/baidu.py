#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from .base import BaseCrawler
import asyncio
import uvloop


def Baidu(loop):
    return BaseCrawler('baidu_baike',
                       'https://baike.baidu.com', 'https://baike.baidu.com/search?word={}&pn=0&rn=0&enc=utf8', 5,
                       '#body_wrapper > div.searchResult > dl > dd > a', None,
                       'https://baike.baidu.com/item', 10,
                       loop)


if __name__ == '__main__':
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dict', type=str, help='dict path')

    args = arg_parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    crawler = Baidu(loop)

    loop.run_until_complete(crawler.run(args.dict))