#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Ce script est destiné à être appelé par un cron pour interagir avec le CNG.
"""
import sys
import bs4 as BeautifulSoup
import requests
import yaml
import re
from redcap import Project

# TODO: Mettre logger les erreurs si le script est en production
# Avec rotation de fichiers ? Plusieurs fichiers ?


def get_md5(fastq_path):
    """ Get md5 value with path to fastq file name."""

    md5_path = fastq_path + '.md5'

    response = requests.get(md5_path, auth=(config['login'], config['password'])).content

    md5 = response.decode().split(' ')[0]

    return md5


def get_instrument(patient_id):
    """ Get instrument type."""

    instrument_type = 'tumor_dna_sequencing'

    return instrument_type


def get_filenames(set_url):
    """ Return all filenames in the set."""

    page = requests.get(set_url, auth=(config['login'], config['password']))
    soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

    return [filename.string for filename in soup.find_all('a') if re.search(r'fastq\.gz$', filename.string)]


def update_record(record):

    print(record)

    # md5 = get_md5(config['url_cng'] + set + filename)

    # 'path_on_cng': config['url_cng'] + set + filename,
    # 'fastq_filename_cng': filename,
    # 'md5_value': md5

    # 'path_on_cng_constit': config['url_cng'] + set + filename,
    # 'fastq_filename_cng_constit': filename,
    # 'md5_value_constit': md5

    # 'path_on_cng_rna': config['url_cng'] + set + filename,
    # 'fastq_filename_cng_rna': filename,
    # 'md5_value_rna': md5

    # instrument_type = get_instrument(patient_id)

    # record.update(dict)
    # records[patient_id].append(record)

    return None


# Lecture du fichier de configuration
with open('config_cng.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)


# Partie API redcap
api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

# Donne tout les records mais pas tout les champs des records
fields_path = ['path_on_cng', 'path_on_cng_rna', 'path_on_cng_constit']
# À aller chercher dans config_crf.yml ?
barcode_index = ['germline_dna_cng_barcode', 'tumor_dna_barcode', 'rna_cng_barcode']

set_index = ['set_on_cng', 'set_on_cng_rna', 'set_on_cng_constit']


records = project.export_records()

# Record pas totalement vide mais n'ayant pas de les champs path de remplis
to_complete = []
# Liste des déjà présent sur RedCap
sets_completed = []
for record in records:
    empty_path = True
    for index in record:
        if index in fields_path and record[index]:
            empty_path = False
    if empty_path:
        if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
            to_complete.append(record)
    else:
        # On retrouve le set dans le champ set
        set_completed = [record[index] for index in record if index in set_index if record[index]]

        sets_completed.extend(set_completed)


# Parser la page à l'adresse :
page = requests.get(config['url_cng'], auth=(config['login'], config['password']))
soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

# liste des att. des tag <a> avec pour nom 'set'
href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d/$', a.string)]
list_set_cng = [href[:-1] for href in href_set]

set_to_complete = set(list_set_cng) - set(sets_completed)


# on créé avec un yield la liste des barcodes appartenant aux record incomplet
# any() pour avoir n'importe quel barcodes quelque soit le type de barcode en clé
for record in to_complete:
    print(record)

sys.exit('exit')
project.import_records(updated_records)
