#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from bs4 import Tag

def connect():
    import asyncpg
    return asyncpg.connect(host='localhost',
                           user='sunqf', password='840422',
                           database='sunqf',
                           command_timeout=60)


import zlib


def compress(html: str) -> bytes:
    return zlib.compress(html.encode())


def decompress(data: bytes) -> str:
    return zlib.decompress(data).decode()


def ChineseSplitter():
    ends = '。！？\n'
    pairs = {'(': ')', '{': '}', '[': ']', '<': '>', '《': '》', '（': '）', '【': '】', '“': '”'}
    left2id = {}
    right2id = {}
    sames = {'"', '\', '}
    same2id = {}
    for i, (k, v) in enumerate(pairs.items()):
        left2id[k] = i
        right2id[v] = i

    for i, s in enumerate(sames):
        same2id[s] = i

    def split_sentence(data: str):
        same_count = [0] * len(same2id)
        pair_count = [0] * len(left2id)

        begin = 0
        for pos, char in enumerate(data):
            if char in ends:
                if sum(same_count) == 0 and sum(pair_count) == 0:
                    if pos - begin > 1:
                        yield ''.join(data[begin:pos + 1])
                    begin = pos + 1
            elif char in left2id:
                pair_count[left2id[char]] += 1
            elif char in right2id:
                pair_count[right2id[char]] -= 1
            elif char in same2id:
                count = same_count[same2id[char]]
                same_count[same2id[char]] = (count + 1) % 2

        if begin < len(data) - 1:
            yield ''.join(data[begin:])

    return split_sentence


splitter = ChineseSplitter()


def clean_tag(tag: Tag):
    for img in tag.select('div.lemma-picture'):
        img.decompose()

    for img in tag.select('div.lemma-album'):
        img.decompose()

    for img in tag.select('a.lemma-album'):
        img.decompose()

    for img in tag.select('a.lemma-picture'):
        img.decompose()

    for sup in tag.select('sup'):
        sup.decompose()

    for sup_ref in tag.select('a.sup-anchor'):
        sup_ref.decompose()

    for table in tag.select('table'):
        table.decompose()

    return tag