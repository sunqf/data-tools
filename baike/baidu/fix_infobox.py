
#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from .. import utils
import json
import asyncio
import re


def fix_key(text) -> str:
        text = re.sub(r'(?=[^0-9a-zA-Z])( |　|\xa0)+(?=[^0-9a-zA-Z])', r'', text)
            text = re.sub(r'( |　|\xa0)*(:|：)( |　|\xa0)*$', r'', text)
                return text


            def fix_value(text) -> str:
                    return text.replace('\u0000', '')


                def fix(data):
                        modified = [False]
                            
                                def _fix(data):
                                            if isinstance(data, str):
                                                            new_data = fix_value(data)
                                                                        if data != new_data:
                                                                                            modified[0] = True
                                                                                                        return data
                                                                                                            elif isinstance(data, dict):
                                                                                                                            ndata = {}
                                                                                                                                        for k, v in data.items():
                                                                                                                                                            fk = fix_key(k)
                                                                                                                                                                            if fk != k:
                                                                                                                                                                                                    modified[0] = True
                                                                                                                                                                                                                    ndata[fk] = _fix(v)
                                                                                                                                                                                                                                return ndata
                                                                                                                                                                                                                                    elif isinstance(data, list):
                                                                                                                                                                                                                                                    return list(_fix(i) for i in data)
                                                                                                                                                                                                                                                        else:
                                                                                                                                                                                                                                                                        return data

                                                                                                                                                                                                                                                                        data = _fix(data)
                                                                                                                                                                                                                                                                            return data, modified[0]


                                                                                                                                                                                                                                                                        async def extract_relation_table():
                                                                                                                                                                                                                                                                                db = await utils.connect()
                                                                                                                                                                                                                                                                                    fixed = []
                                                                                                                                                                                                                                                                                        async with db.transaction():
                                                                                                                                                                                                                                                                                                    async for record in db.cursor('select id, knowledge from baike_knowledge'):
                                                                                                                                                                                                                                                                                                                    id = record['id']
                                                                                                                                                                                                                                                                                                                                knowledge = json.loads(record['knowledge'])
                                                                                                                                                                                                                                                                                                                                            nknowledge, modified = fix(knowledge)
                                                                                                                                                                                                                                                                                                                                                        if modified:
                                                                                                                                                                                                                                                                                                                                                                            fixed.append((id, json.dumps(nknowledge)))

                                                                                                                                                                                                                                                                                                                                                                                async with db.transaction():
                                                                                                                                                                                                                                                                                                                                                                                            async with db.transaction():
                                                                                                                                                                                                                                                                                                                                                                                                            await db.executemany(
                                                                                                                                                                                                                                                                                                                                                                                                                                    'UPDATE baike_knowledge set knowledge = $2 where id = $1', fixed)

                                                                                                                                                                                                                                                                                                                                                                                                            loop = asyncio.get_event_loop()
                                                                                                                                                                                                                                                                                                                                                                                                            loop.run_until_complete(extract_relation_table())
