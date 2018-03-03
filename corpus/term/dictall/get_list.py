#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import requests
import json
from urllib.parse import urljoin

headers = {
    'Accept': 'text/plain, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Length': '27',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

host = 'http://www.dictall.com'
node_url = 'http://www.dictall.com/dictall/node.jsp'


def get_list(root: str, id: int, pId: int, grade: int =0):
    def _request(id, pId, grade, isInit=1):
        for t in range(10):
            post_data = 'id={}&pId={}&grade={}&isInit={}'.format(id, pId, grade, isInit)
            try:
                res = requests.post(node_url, data=post_data, headers=headers)
                if res.status_code == 200:
                    return res.text
            except Exception as e:
                print(e)
        return None

    data = _request(id, pId, grade)
    items = json.loads(data)
    for item in items:
        if item['grade'] == grade+1:
            _root = root + '/' + item['name']
            if item['isParent']:
                get_list(_root, item['id'], item['pId'], item['grade'])
            else:
                print('\t'.join([_root, urljoin(host, item['url'])]))


get_list('', 0, 0)