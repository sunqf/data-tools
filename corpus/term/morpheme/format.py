#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import sys
import re


noise = re.compile('(\[[0-9]+\]| \(BrE\)| \(AmE\))')

variant = re.compile('\(([a-z])\)')

suffixies = []
prefixies = []
for line in sys.stdin:
    line = line.strip()
    if len(line) == 0 or line.startswith('#'):
        continue

    affixies, meaning, source, examples = line.split('\t')
    for affix in affixies.split(', '):
        affix = noise.sub('', affix)

        def _all(affix):
            if len(variant.findall(affix)) > 0:
                return [variant.subn('', affix)[0], variant.subn('\\1', affix)[0]]
            else:
                return [affix]

        for a in _all(affix):
            if a.endswith('-'):
                prefixies.append(a)
            elif a.startswith('-'):
                suffixies.append(a)
            else:
                print('unknow ', a)


print('\t'.join(prefixies))
print()
print('\t'.join(suffixies))
print()


