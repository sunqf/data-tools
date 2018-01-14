
from gevent import monkey; monkey.patch_all()
import gevent
from gevent.queue import JoinableQueue, Queue, Empty
import requests
import os

import simplejson
import argparse

import psycopg2


DSN = "dbname=sunqf user=sunqf"


def to_url(word: str) -> str:
    return 'http://dict.youdao.com/jsonapi?q=%s&doctype=json' % word


def to_lj_url(word: str) -> str:
    return 'http://dict.youdao.com/jsonapi?q=lj:%s&doctype=json' % word


headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}


def get_words(dict_path):
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT keyword from youdao_bilingual')
            fetched_words = set([d[0] for d in cursor.fetchall()])

    words = []
    with open(dict_path) as file:
        for line in file:
            items = line.split('\t')
            if items[0] not in fetched_words:
                words.append(items[0])
            """
            words = line.split()
            w_len = len(words)
            dict.update([' '.join(words[i:j]) for i in range(w_len) for j in range(i + 2, w_len)])
            """

    return words


def get_proxy() -> str:
    while True:
        proxy = requests.get('http://127.0.0.1:5010/get/')
        if proxy.status_code == 200 and len(proxy.text.split(':')) == 2:
            return proxy.text


def delete_proxy(proxy: str):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))


def validate(response: requests.Response) -> bool:
    return response.status_code == 200 and is_json(response.text)


def is_json(text: str) -> bool:
    try:
        json = simplejson.loads(text)
        if 'input' in json:
            return True
        else:
            return False
    except Exception:
        return False


def remove_invalid(filename) -> bool:
    if os.path.exists(filename):
        with open(filename) as f:
            text = f.read()
        if not is_json(text):
            os.remove(filename)
            print('remove {}'.format(filename))


def fetch(word: str):
    url = to_lj_url(word)

    retry: int = 0
    while retry < 5:
        try:
            proxy_ip = get_proxy()
            proxies = {"http": "http://{}".format(proxy_ip)}
            response = requests.get(url, headers=headers, timeout=10, proxies=proxies)

            if validate(response):
                return response.text
            else:
                delete_proxy(proxy_ip)
        except Exception as e:
            print('%s failed. exception: %s' % (url, str(e)))

        retry += 1

    return None


def fetch_worker(fetch_queue: JoinableQueue, save_queue: JoinableQueue, direction: str):

    while True:
        word = fetch_queue.get()
        print(word)
        res = fetch(word)
        if res:
            save_queue.put((word, direction, res))
            fetch_queue.task_done()
        else:
            fetch_queue.put(word)


def save_worker(dsn: str, save_queue: JoinableQueue):
    conn = psycopg2.connect(dsn)
    while True:
        word, direction, data = save_queue.get()
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
                    cur.execute("INSERT INTO youdao_bilingual (keyword, direction, data) VALUES (%s, %s, %s)",
                                (word, direction, data))
            save_queue.task_done()

        except Exception as e:
            print(e)
            save_queue.put((word, direction, data))

    conn.close()



arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--dict', type=str, help='dict path')
arg_parser.add_argument('--direction', type=str, help='direction')

args = arg_parser.parse_args()

word_list = get_words(args.dict)
print('word list size = %d' % (len(word_list)))
print(word_list[0:10])

fetch_queue = JoinableQueue()

save_queue = JoinableQueue()

for i in range(100):
    gevent.spawn(fetch_worker, fetch_queue, save_queue, args.direction)

gevent.spawn(save_worker, DSN, save_queue)

for word in word_list:
    fetch_queue.put(word)

fetch_queue.join()
save_queue.join()
