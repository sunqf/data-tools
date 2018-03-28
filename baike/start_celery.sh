#!/usr/bin/env bash


# sudo rabbitmq-server -detached
# sudo rabbitmqctl add_user sunqf 840422
# sudo rabbitmqctl add_vhost baike
# sudo rabbitmqctl set_user_tags sunqf super
# sudo rabbitmqctl set_permissions -p baike sunqf ".*" ".*" ".*"


python3.6 -m celery multi stopwait 0 1 baike.celery
python3.6 -m celery multi start 0 1 -A baike.celery -P eventlet -c 100
python3.6 -m celery flower -A baike.celery --address=127.0.0.1 --port=5555
# python3.6 -m celery multi stopwait 0 1 2 3 baike.celery

