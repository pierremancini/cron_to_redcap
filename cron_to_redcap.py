#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Ce script est destiné à être appelé par un cron.
"""

import bs4 as BeautifulSoup
import requests
import yaml
import re
from redcap import Project

# TODO: Faire un objet set_parser

# Lecture du fichier de configuration
with open('config.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

# Parser la page à l'adresse :
page = requests.get(config['url_cng'], auth=(config['login'], config['password']))

soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

# liste des tag <a> avec pour nom 'set'
set_cng = [l.string for l in soup.find_all('a') if re.search(r'^set\d/$', l.string)]


# Partie API
# Est-ce que c'est la bonne adresse ?
api_url = 'http://ib101b/html/redcap/api'

# project = Project(config['api_url'], api_key)

# to_import = [{'record': 'foo', 'test_score': 'bar'}] 
# response = project.import_records(to_import)

# On va chercher la valeur des champs path_on_cng | path_on_cng_rna | path_on_cng_constit 
# Example de valeur 
path_on_cng = 'https://www.cng.fr/data/MULTIPLI/fastq/set7'
print(path_on_cng.split("/")[-1])
