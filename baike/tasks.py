#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from .baidu.named_entity import entities
import json
from baike.celery import app


@app.task(bind=True)
def named_url(self, url: str, knowledge: str):
    knowledge = json.loads(knowledge)
    names = []
    for entity in entities:
        name = entity.named(knowledge)
        if name:
            names.append(name)
    return url, names



