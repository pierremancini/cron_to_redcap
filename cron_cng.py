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
# import pprint
# pp = pprint.PrettyPrinter(indent=1)

# TODO: Mettre logger les erreurs si le script est en production
# Avec rotation de fichiers ? Plusieurs fichiers ?


def get_barcode(fastq):
    """ Extract barcode from fastq file name ."""

    return fastq.split('_')[2]


def get_patient_id(barcode, assoc_array):
    """ Get patient id from barcode according to CRF's associative array."""

    # TODO

    # Pour l'instant ça renvoie une seule valeur, par défault
    return barcode + 'DEFAULT'


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


def get_filename(set_url):
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

# # Parser la page à l'adresse :
# page = requests.get(config['url_cng'], auth=(config['login'], config['password']))
# soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

# # liste des att. des tag <a> avec pour nom 'set'
# href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d/$', a.string)]

# list_path_cng = [config['url_cng'] + href for href in href_set]

# Partie API redcap
api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

# Donne tout les records mais pas tout les champs des records
fields_path = ['path_on_cng', 'path_on_cng_rna', 'path_on_cng_constit']
# À aller chercher dans config_crf.yml ?
barcode_index = ['germline_dna_cng_barcode', 'tumor_dna_barcode', 'rna_cng_barcode']

records = project.export_records()

# print(records_path)

# /!\ Sera dans "to_complete" les records qui n'ont pas tout les champs
# de "fields_path" de remplit

# On n'utilise pas les records totalement vides

# Record pas totalement vide mais n'ayant pas de les champs path de remplis
to_complete = []

for record in records:
    empty_path = True
    for index in record:
        if index in fields_path and record[index]:
            empty_path = False
    if empty_path and record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
        to_complete.append(record)

# # Pour tout les set incomplets il nous faut les barcodes (à extraire des filenames)
# # faire un dict {barcode: filename} à la volée
# nested_list_filename = [get_filename(config['url_cng'] + set) for set in set_incomplete]

# # A changer 1 barcode <-> n filename (n redcap_repeated_instance)
# dict_file_barcodes_cng = {get_barcode(filename): filename
#                           for sublist in nested_list_filename
#                           for filename in sublist}

# barcodes_cng = [barcode for barcode in dict_file_barcodes_cng]

# # Les indices d'un record correspondant au barcode
# barcode_index = ['germline_dna_cng_barcode', 'tumor_dna_barcode', 'rna_gcn_barcode']

# response_barcode = project.export_records(fields=barcode_index)

# # Création du dict par barcode
# record_by_barcode = {record[index]: record
#                   for record in response_barcode
#                   for index in record
#                   if index in barcode_index and record[index]}

# redcap_barcodes = [barcode for barcode in record_by_barcode]

# # CNG - redcap = les barcodes/record qui posent problème
# if set(barcodes_cng) - set(redcap_barcodes):
#     for barcode in set(barcodes_cng) - set(redcap_barcodes):
#         # TODO: log à la place d'un print pour la production
#         # print('Warning: le barcode ' + barcode + ' est présent dans le CNG et pas dans le RedCap.')
#         pass

# # redcap & CNG = record à completé dans redcap
# to_update = set(barcodes_cng) & set(redcap_barcodes)
# for barcode in to_update:

#     updated_record = update_record(record_by_barcode[barcode])


sys.exit('exit')
project.import_records(updated_records)
