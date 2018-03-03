#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-



import simplejson

import os
import re

import argparse

import gevent
from gevent.queue import JoinableQueue
from gevent import monkey; monkey.patch_all()

arg_parser = argparse.ArgumentParser()

arg_parser.add_argument('--input', type=str, default='', help='input dir')
arg_parser.add_argument('--output', type=str, default='', help='output path')
arg_parser.add_argument('--field',
                        type=str,
                        default='blng',
                        help='field name. [blng, collins, ec21, longman, splongman, special, phrase, meida, auth]')


def helper(json, fields):
    field_len = len(fields)

    def dfs(node, index):
        if index >= field_len:
            yield node

        if index < field_len and fields[index] in node:
            next = node[fields[index]]
            if isinstance(next, list):
                for child in next:
                    yield from dfs(child, index+1)
            else:
                yield from dfs(next, index+1)

    yield from dfs(json, 0)


def remove_html_tag(text):

    # Let’s <i>play cards</i> . --> Let’s play cards .
    tag = re.compile(r'<([a-z]+)>(.*?)</\1>')
    text = tag.sub(r'\2', text)

    # I’ll just go up (= <em>go upstairs</em> ) and ask him what he wants. --> I’ll just go up and ask him what he wants.
    em_tag = re.compile(r'\(= <em>.*?</em> \)')
    text = tag.sub('', text)
    return text


def extract_blng_sents(json): # -> Generator[(str, str, str)]:
    pairs = helper(json, ['blng_sents', 'sentence-pair'])
    for pair in pairs:
        en = pair['sentence']
        zh = pair['sentence-translation']
        url = pair['url']
        yield (en, zh, url)


def extract_auth(json):
    for pair in helper(json, ['auth_sents', 'sent']):
        en = remove_html_tag(pair['foreign'])
        url = pair['url']
        yield (en, url)

def extract_media(json):
    for pair in helper(json, ['media_sents', 'sent']):
        if 'chn' in pair:
            en = remove_html_tag(pair['eng'])
            zh = remove_html_tag(pair['chn'])
            #url = pair['streamUrl']
            yield (en, zh)


def extract(input_dir, output_path, func):
    with open(output_path, 'w') as output:

        tasks = JoinableQueue()
        for file_name in os.listdir(input_dir):
            tasks.put(file_name)

        def _extract(file_name):
            file_path = os.path.join(input_dir, file_name)

            with open(file_path) as f:
                try:
                    json = simplejson.load(f)
                except Exception as e:
                    print(str(e))
                    print('Failed to load json file {}'.format(file_path))

                for pair in func(json):
                    output.write('\t'.join([str(x) for x in pair]) + '\n')

        def worker():
            while True:
                file_name = tasks.get()
                _extract(file_name)
                print(file_name)
                tasks.task_done()


        for i in range(10):
            gevent.spawn(worker)

        tasks.join()

field_mapping = {'blng': extract_blng_sents,
                 'media': extract_media,
                 'auth': extract_auth
                 }


if __name__ == '__main__':

    args = arg_parser.parse_args()

    if args.field not in field_mapping:
        print('Argument field not supported.')
        exit(-1)

    func = field_mapping[args.field]

    extract(args.input, args.output, func)
