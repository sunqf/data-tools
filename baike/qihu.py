#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .base import BaseCrawler
import asyncio
import uvloop


def Qihu(loop):
    return BaseCrawler('360_baike',
                       'https://baike.so.com', 'https://baike.so.com/search/?q={}', 5,
                       '#main > ul > li > h3 > a', '#page > a',
                       'https://baike.so.com/doc', 10, loop)


if __name__ == '__main__':
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dict', type=str, help='dict path')

    args = arg_parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    crawler = Qihu(loop)

    loop.run_until_complete(crawler.run(args.dict))