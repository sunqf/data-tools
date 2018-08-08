#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import zlib
from ..utils import connect, clean_tag
import asyncio
import concurrent
from typing import Generator
from bs4 import BeautifulSoup
import argparse
from tqdm import tqdm


type = 'baidu_baike'

def extract_text(html: str): 
    html = BeautifulSoup(html, 'html.parser')
    for para in html.select('div.para'):
        yield clean_tag(para).getText().replace('\n', '').strip()


async def shard_extract(output: str, shard_size: int, shard_id: int, batch_size=1000):
    reader = await connect()
    with open('%s.%d' % (output, shard_id), 'w') as f, tqdm(desc='shard ' + str(shard_id)) as t:
        batch = []
        async with reader.transaction(isolation='serializable', readonly=True):
            async for record in reader.cursor('SELECT url, html from baike_html where type=\'{}\' and id%{}={}'.format(type, shard_size, shard_id)):
                t.update(1)
                url = record['url']
                html = zlib.decompress(record['html'])
                for sen in extract_text(html):
                    if len(sen) > 10:
                        batch.append(sen+'\n')

                    if len(batch) > batch_size:
                        f.writelines(batch)
                        batch = []

            if len(batch) > 0:
                f.writelines(batch)
                batch = []

def worker(output: str, shard_size: int, worker_id: int):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(shard_extract(output, shard_size, worker_id))

async def run(loop, output: str, shard_size: int):
    with concurrent.futures.ProcessPoolExecutor(max_workers=shard_size) as executor:
        workers = [loop.run_in_executor(executor, worker, output, shard_size, id) for id in range(shard_size)]
        for w in workers:
            await w


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--num', type=int, default=5, help='number of worker')
    parser.add_argument('--output', type=str, required=True, help='output path')

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, args.output, args.num))
