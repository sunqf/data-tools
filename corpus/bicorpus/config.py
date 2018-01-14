#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


DSN = "dbname=sunqf user=sunqf"

proxy_get_url = 'http://127.0.0.1:5010/get/'


def proxy_delete_url(proxy):
    return "http://127.0.0.1:5010/delete/?proxy={}".format(proxy)


def get_url(keyword):
    return 'http://dict.youdao.com/jsonapi?q=lj:%s&doctype=json' % keyword

