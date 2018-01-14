#!/usr/bin/env bash

redis-server ./redis.conf
git clone https://github.com/jhao104/proxy_pool.git
cd proxy_pool
pip install -r requirements.txt
cd Run
python3.6 main.py