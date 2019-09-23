#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Ce script est destiné à être appelé par un cron pour interagir avec le CNG.
"""
import os
import sys
import bs4 as BeautifulSoup
import requests
import yaml
import re
import logging
import json


# Création du logger

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

formatter = logging.Formatter('%(levelname)s :: %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# Lecture du fichier de configuration
with open('config_cng.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)


def get_filenames(set_url):
    """ Return all filenames in the set."""

    page = requests.get(set_url, auth=(config['login'], config['password']))
    soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

    return [filename.string for filename in soup.find_all('a') if re.search(r'fastq\.gz$', filename.string)]


def get_md5(fastq_path):
    """ Get md5 value with path to fastq file name."""

    md5_path = fastq_path + '.md5'

    response = requests.get(md5_path, auth=(config['login'], config['password'])).content

    md5 = response.decode().split(' ')[0]

    return md5

page = requests.get(config['url_cng'], auth=(config['login'], config['password']))
soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')
href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d/$', a.string)]

# Structure:
# {set1: {filename1, filename2, ...}
filenames_by_set = {}

# Structure:
# {fastq_path: md5, fastq_path: md5, ...}
md5_by_path = {}

for set in href_set:
    set_url = config['url_cng'] + set
    filenames = get_filenames(set_url)
    for filename in filenames:
        filenames_by_set.setdefault(set[:-1], []).append(filename)
        fastq_path = set_url + filename
        md5 = get_md5(fastq_path)
        md5_by_path.setdefault(fastq_path, md5)

with open(os.path.join('data', 'cng_filenames_dump.json'), 'w') as jsonfile:
    json.dump(filenames_by_set, jsonfile)

with open(os.path.join('data', 'cng_md5_dump.json'), 'w') as jsonfile:
    json.dump(md5_by_path, jsonfile)
