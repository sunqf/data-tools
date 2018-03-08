#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import sys
import collections

cached = []
def get_url(word):
    return 'https://baike.baidu.com/search/none?word={}&pn=0&rn=10&enc=utf8'.format(word)


word_counter = dict()
for line in sys.stdin:
    items = line.strip().split('\t')
    if len(items) == 2:
        type, word = items
        type = '/'.join(type.split('/')[0:2])
        if type in word_counter:
            word_counter[type].update([word])
        else:
            word_counter[type] = collections.Counter()
            word_counter[type].update([word])

for type, counter in word_counter.items():
    for word, count in counter.most_common():
        print('\t'.join([type, word, str(count)]))