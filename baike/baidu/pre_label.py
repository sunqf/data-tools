#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import sys
import json
from .named_entity import entities
from .. import utils
import asyncio


async def label(path):
    db = await utils.connect()
    with open(path) as reader:
        for line in reader:
            fields = line.rsplit('\t', maxsplit=3)
            if len(fields) != 4:
                print(fields)
                continue

            url, count, ratio, data = fields
            count = int(count)
            ratio = float(ratio)
            knowledge = json.loads(data)

            names = []
            for entity in entities:
                name = entity.named(knowledge)
                if name:
                    names.append(name)

            types = json.dumps({'entity_type': names})

            async with db.transaction():
                await db.execute('INSERT INTO manual_label (url, count, ratio, label, flag, knowledge, type) '
                                 'VALUES ($1, $2, $3, $4, $5, $6, $7)',
                                 url, count, ratio, types, 'auto', data, 'baidu_baike')


loop = asyncio.get_event_loop()
loop.run_until_complete(label(sys.argv[1]))