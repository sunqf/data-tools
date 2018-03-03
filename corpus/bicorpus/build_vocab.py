#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


# 从抓取的数据中重新整理一份词表

import spacy
import sys

from collections import Counter

from spacy.lang.en import English

english = English()

counter = Counter()
for line in sys.stdin:
    words = [str(w) for w in english(line)]
    words = [w.lower() for w in words if w.isalpha()]

    counter.update(words)

print('\n'.join(['%s\t%d' % (w, c) for w, c in counter.most_common()]))