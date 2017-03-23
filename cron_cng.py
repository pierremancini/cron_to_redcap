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
    """ Return all filenames in the set ."""

    page = requests.get(set_url, auth=(config['login'], config['password']))
    soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

    return [filename.string for filename in soup.find_all('a') if re.search(r'fastq\.gz$', filename.string)]


# TODO: Faire un objet set_parser

# Lecture du fichier de configuration
with open('config_cng.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

# Parser la page à l'adresse :
page = requests.get(config['url_cng'], auth=(config['login'], config['password']))
soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')


# liste des att. des tag <a> avec pour nom 'set'
href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d/$', a.string)]

list_path_cng = [config['url_cng'] + href for href in href_set]

# liste des tag <a> avec pour nom 'set'
set_cng = [l.string for l in soup.find_all('a') if re.search(r'^set\d/$', l.string)]

# Partie API redcap
api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

# Regarde quels set sont déjà sur redcap
fields_of_interest = ['path_on_cng', 'path_on_cng_rna', 'path_on_cng_constit']
response = project.export_records(fields=fields_of_interest)
set_redcap = [dict[field].split('/')[-1] for dict in response for field in dict
    if field in fields_of_interest and dict[field] is not '']

# On fait le différence entre les set du cng et les set de redcap
set_import = set([l[:-1] for l in href_set]) - set(set_redcap)



# TODO:
# Partie interogation CRF: les données du CRF seront stockées sur RedCap
# dans les instruments du projet multipli

def set_to_records(set):
    """ Return a list of record dictionary from all files in a set."""

    filenames = get_filename(config['url_cng'] + set)

    records = {}

    for filename in filenames:

        md5 = get_md5(config['url_cng'] + set + filename)
        barcode = get_barcode(config['url_cng'] + set + filename)
        patient_id = get_patient_id(barcode, ['toto', 'au chateau'])
        instrument_type = get_instrument(patient_id)

        records.setdefault(patient_id, [])


        # Ces info seront déjà présentent sur le RedCap après intérogation du CRF
        record = {'patient_id': patient_id, 'redcap_repeat_instrument': instrument_type,
        'redcap_repeat_instance': len(records[patient_id]) + 1}

        # Ambranchement en fonction de instrument_type:
        # tumor_dna_sequencing | germline_dna_sequencing | rna_sequencing
        if instrument_type == 'tumor_dna_sequencing':
            # Partie de dict spécifique à l'instrument, à fusionner avec record
            dict = {
                'path_on_cng': config['url_cng'] + set + filename,
                'tumor_dna_barcode': barcode,
                'fastq_filename_cng': filename,
                'md5_value': md5
            }

        elif instrument_type == 'germline_dna_sequencing':

            dict = {
                'path_on_cng_constit': config['url_cng'] + set + filename,
                'germline_dna_cng_barcode': barcode,
                'fastq_filename_cng_constit': filename,
                'md5_value_constit': md5
            }

        elif instrument_type == 'rna_sequencing':

            dict = {
                'path_on_cng_rna': config['url_cng'] + set + filename,
                'rna_gcn_barcode': barcode,
                'fastq_filename_cng_rna': filename,
                'md5_value_rna': md5
            }

        record.update(dict)
        records[patient_id].append(record)

    return records


records = [record for record_dict in map(set_to_records, set_import) for record in record_dict]


print(records)
# Partie importation des set
project.import_records(records)
