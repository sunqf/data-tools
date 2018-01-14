#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


import os
import simplejson

path = './youdao'


def get_correct_words(json):
    if 'typos' in json:
        return [w['word'] for w in json['typos']['typo']]
    else:
        return []

with open('words_alpha.txt') as f:
    dict = set([w.strip() for w in f])

fetched_words = set(os.listdir(path))

new_word_dict = set()
for file in os.listdir(path):
    file_path = os.path.join(path, file)
    with open(file_path) as f:
        try:
            json = simplejson.load(f)
            for w in get_correct_words(json):
                if w not in fetched_words:
                    print(w)
                    new_word_dict.add(w)

        except Exception as e:
            print('file: {} \n{}'.format(file, e))
            print('remove {}'.format(file_path))
            os.remove(file_path)

            print(w)
            new_word_dict.add(file)

new_word_dict.update(dict.difference(fetched_words))

with open('youdao-left', 'w') as file:
    file.write('\n'.join(new_word_dict))