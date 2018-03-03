#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from .base import BaseCrawler
import asyncio
import uvloop


def Hudong(loop):
    return BaseCrawler('hudong_baike',
                       'http://so.baike.com', 'http://so.baike.com/doc/{}&prd=button_doc_search', 5,
                       '#search-wiki > div > h3 > a', '#page > div > a',
                       'http://www.baike.com/wiki', 10,
                       loop)


if __name__ == '__main__':
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dict', type=str, help='dict path')

    args = arg_parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    crawler = Hudong(loop)

    loop.run_until_complete(crawler.run(args.dict))