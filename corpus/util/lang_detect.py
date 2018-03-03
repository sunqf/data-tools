#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import re

hanzi = re.compile(
    u'([^\u0000-\u007f\u00f1\u00e1\u00e9\u00ed\u00f3\u00fa\u00d1\u00c1\u00c9\u00cd\u00d3\u00da\u0410-\u044f\u0406\u0407\u040e\u0456\u0457\u045e])')

def detect_zh(text):
    zh_len = len(hanzi.findall(text))
    return zh_len/len(text) > 0.2