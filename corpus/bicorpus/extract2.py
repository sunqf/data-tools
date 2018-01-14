#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
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


def extract_collins(json): #-> Generator[(str, str)]:
    fields = ['collins', 'collins_entries', 'entries', 'entry', 'tran_entry', 'exam_sents', 'sent']

    for sent in helper(json, fields):
        en = sent['eng_sent'].strip()
        zh = sent['chn_sent'].strip()
        if len(en) > 0 and len(zh) > 0:
            yield (en, zh)

def extract_ec21(json): #-> Generator[(str, str)]:
    for p in helper(json,  fields = ['ec21', 'word', 'trs', 'tr', 'exam']):
        en = list(helper(p, ['i', 'f', 'l', 'i']))[0].strip()
        zh = list(helper(p, ['i', 'n', 'l', 'i']))[0].strip()
        if len(en) > 0 and len(zh) > 0:
            yield (en, zh)

def remove_html_tag(text):

    # Let’s <i>play cards</i> . --> Let’s play cards .
    tag = re.compile(r'<([a-z]+)>(.*?)</\1>')
    text = tag.sub(r'\2', text)

    # I’ll just go up (= <em>go upstairs</em> ) and ask him what he wants. --> I’ll just go up and ask him what he wants.
    em_tag = re.compile(r'\(= <em>.*?</em> \)')
    text = tag.sub('', text)
    return text

def extract_longman(json): # -> Generator[(str, str)]:
    for sense in helper(json, ['longman', 'wordList', 'Entry', 'PhrVbEntry', 'Sense']):
        if 'EXAMPLE' in sense:
            ens = remove_html_tag(sense['EXAMPLE'])
            zhs = remove_html_tag(sense['EXAMPLETRAN'])
            yield from zip(ens, zhs)

    for sub_sense in helper(json, ['longman', 'wordList', 'Entry', 'Sense', 'Subsense']):
        if 'EXAMPLE' in sub_sense:
            ens = remove_html_tag(sub_sense['EXAMPLE'])
            zhs = remove_html_tag(sub_sense['EXAMPLETRAN'])
            yield from zip(ens, zhs)


def extract_splongman(json): # -> Generator[(str, str)]:
    for sense in helper(json, ['splongman', 'wordList', 'Entry', 'PhrVbEntry', 'Sense']):
        if 'EXAMPLE' in sense:
            ens = remove_html_tag(sense['EXAMPLE'])
            zhs = remove_html_tag(sense['EXAMPLETRAN'])
            yield from zip(ens, zhs)

    for sub_sense in helper(json, ['splongman', 'wordList', 'Entry', 'Sense', 'Subsense']):
        if 'EXAMPLE' in sub_sense:
            ens = remove_html_tag(sub_sense['EXAMPLE'])
            zhs = remove_html_tag(sub_sense['EXAMPLETRAN'])
            yield from zip(ens, zhs)


def extract_special(json): # -> Generator[(str, str, str)]:
    entries = helper(json, ['special', 'entries', 'entry', 'trs'])
    for t in entries:
        en = list(helper(t, ['tr', 'engSent']))
        zh = list(helper(t, ['tr', 'chnSent']))
        url = list(helper(t, ['tr', 'url']))
        if len(en) > 0 and len(zh) > 0:
            en = remove_html_tag(en[0])
            zh = remove_html_tag(zh[0])
            if len(en) > 0 and len(zh) > 0:
                yield (en, zh, url[0])


def extract_blng_sents(json): # -> Generator[(str, str, str)]:
    pairs = helper(json, ['blng_sents_part', 'sentence-pair'])
    for pair in pairs:
        en = pair['sentence']
        zh = pair['sentence-translation']
        url = pair['url']
        yield (en, zh, url)


def extract_phrase(json):
    for phrase in helper(json, ['phrs', 'phrs', 'phr']):
        en = phrase['headword']['l']['i']
        zh = ';;;'.join([t['tr']['l']['i'] for t in phrase['trs']])
        yield (en, zh)


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
            url = pair['streamUrl']
            yield (en, zh, url)


def extract_web_trans(json):
    for i, p in enumerate(helper(json, ['web_trans', 'web-translation'])):
        if i > 0:
            key = p['key']
            trans = '####'.join(list(helper(p, ['trans', 'value'])))
            yield (key, trans)

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
                 'collins': extract_collins,
                 'ec21': extract_ec21,
                 'longman': extract_longman,
                 'splongman': extract_splongman,
                 'special': extract_special,
                 'phrase': extract_phrase,
                 'media': extract_media,
                 'auth': extract_auth,
                 'web_trans': extract_web_trans
                 }


if __name__ == '__main__':

    args = arg_parser.parse_args()

    if args.field not in field_mapping:
        print('Argument field not supported.')
        exit(-1)

    func = field_mapping[args.field]

    extract(args.input, args.output, func)
