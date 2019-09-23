#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import bs4 as BeautifulSoup
import requests
import yaml
import re
from redcap import Project
import json


# Lecture du fichier de configuration
with open('config.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)
with open('secret_config.yml', 'r') as ymlfile:
    secret_config = yaml.load(ymlfile)
config.update(secret_config)

# Partie API redcap
api_url = config['redcap_api_url']
project = Project(api_url, config['api_key'])

page = requests.get(config['url_cng'], auth=(config['login_cng'], config['password_cng']),
    timeout=(3.05, 27))
soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

# liste des att. des tag <a> avec pour nom 'set'
href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d+/$', a.string)]
list_set_cng = [href[:-1] for href in href_set]

dict_barcode_cng = {}

for set_cng in list_set_cng:
    set_url = config['url_cng'] + set_cng
    page = requests.get(set_url, auth=(config['login_cng'], config['password_cng']))
    soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')
    fastq_gen = (file.string for file in soup.find_all('a') if re.search(r'fastq\.((gz)|(bz)|(zip)|(bz2)|(tgz)|(tbz2))$',
    file.string))
    for fastq in fastq_gen:
        project, kit_code, barcode, lane, read, end_of_file = fastq.split('_')
        dict_barcode_cng.setdefault(barcode, set_cng)

print(dict_barcode_cng)