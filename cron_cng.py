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


def info_from_set(set_to_complete):
    """ Get data from url's set on CNG.

        Return:
        barcode, project, kit-code, lane, read, flowcell, tag
    """

    # Strucure
    # {barcode:
    #    [{project, kit_code, barcode, lane, read, end_of_file, flowcell, tag}},
    #        ... ]
    dicts_fastq_info = {}

    for set in set_to_complete:
        set_url = config['url_cng'] + '/' + set
        page = requests.get(set_url, auth=(config['login'], config['password']))
        soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

        fastq_gen = (file.string for file in soup.find_all('a') if re.search(r'fastq\.gz$', file.string))
        for fastq in fastq_gen:
            project, kit_code, barcode, lane, read, end_of_file = fastq.split('_')
            flowcell, tag = end_of_file.split('.')[:-2]
            dict_fastq_info = {'set': set,
                               'fullname': fastq,
                               'project': project,
                               'kit_code': kit_code,
                               'barcode': barcode,
                               'lane': lane,
                               'read': read,
                               'flowcell': flowcell,
                               'tag': tag}
            dicts_fastq_info.setdefault(barcode, []).append(dict_fastq_info)

    return dicts_fastq_info


def get_filenames(set_url):
    """ Return all filenames in the set."""

    page = requests.get(set_url, auth=(config['login'], config['password']))
    soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

    return [filename.string for filename in soup.find_all('a') if re.search(r'fastq\.gz$', filename.string)]


def clone_chain_record(clone_chain, type_barcode_to_instrument, records_by_couple):
    """
        TODO: A addapter à ce script

        1. Create a chain of cloned record
        2. add it to other chained record i.e. clone_chain

        :param clone_chain: other chained record
    """

    new_records = []
    instance_number = max_instance_number((patient_id, instrument),
        records_by_couple) + 1
    for barcode in couple_count[couple]['barcode']:
        if barcode not in redcap_barcodes:
            new_records.append({'redcap_repeat_instrument': instrument,
                              'patient_id': patient_id,
                              type_barcode: barcode,
                              'redcap_repeat_instance': instance_number})
            instance_number += 1

    clone_chain += new_records

    return clone_chain


def max_instance_number(couple, records_by_couple):
    """ Return maximal intance number from a list of record.

        List of record: Combination between the parameter records_by_couple
        and the couple (patient_id, instrument).

        Nb: patient_id, instrument are variable global to the upper function.
    """

    # on utilise le param: couple

    # On determine l'instance number
    # et on incrémente
    max_instance_number = 0
    for record in records_by_couple[couple]:
        if int(record['redcap_repeat_instance']) > max_instance_number:
            max_instance_number = int(record['redcap_repeat_instance'])

    return max_instance_number


# Dans le cas d'un duplicat de barcode ce script doit cloner le record redcap correspondant
# Nb: ce cas sera exceptionnel
def clone_record(record_to_clone, type_barcode_to_instrument, records_by_couple):
    """
        Create record that is a clone of RedCap record.
    """

    for index in record_to_clone:
        if index in barcode_index and record_to_clone[index]:
            type_barcode = index
            barcode = record_to_clone[index]

    patient_id = record_to_clone['patient_id']
    instrument = type_barcode_to_instrument[type_barcode]

    instance_number = max_instance_number((patient_id, instrument),
        records_by_couple) + 1
    new_record = {'redcap_repeat_instrument': instrument,
                  'patient_id': patient_id,
                  type_barcode: barcode,
                  'redcap_repeat_instance': instance_number
                  }

    return new_record


# Lecture du fichier de configuration
with open('config_cng.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

with open('config_crf.yml', 'r') as crfyml:
    config_crf = yaml.load(crfyml)

# Partie API redcap
api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

# Donne tout les records mais pas tout les champs des records
fields_path = ['path_on_cng', 'path_on_cng_rna', 'path_on_cng_constit']
# À aller chercher dans config_crf.yml ?
type_barcode_to_instrument = config_crf['type_barcode_to_instrument']
barcode_index = list(type_barcode_to_instrument.keys())

set_index = ['set_on_cng', 'set_on_cng_rna', 'set_on_cng_constit']

response = project.export_records()

# Record pas totalement vide mais n'ayant pas de les champs path de remplis
# {barcode: [record, record, ...]}
to_complete = {}
# Liste des déjà présent sur RedCap
sets_completed = []

# Variable nécessaire à la création de clone de le cas où
# on trouve un record avec le même barcode dans le CNG
# Strucutre:
# {(patient_id, type_barcode): [record, record]}
records_by_couple = {}

for record in response:
    # Creation de records_by_couple
    if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
        patient_id = record['patient_id']
        for index in record:
            if index in barcode_index and record[index]:
                records_by_couple.setdefault((patient_id, record['redcap_repeat_instrument']), []).append(record)
    empty_path = True
    for index in record:
        if index in fields_path and record[index]:
            empty_path = False
    if empty_path:
        if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
            for index in barcode_index:
                if record[index]:
                    barcode = record[index]
            # Un seul record par clé barcode ?
            if to_complete.setdefault(barcode, record) != record:
                print('Warning: dans le redcap il y a plusieurs records sans path partageant le même barcode')
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


def update(record, about_record):
    """ Update RedCap record with CNG data. """

    print('about_record')
    print(about_record)
    sys.exit()


updated_records = []

# Dictionnaire avec les barcodes en 1ère clé
dicts_fastq_info = info_from_set(set_to_complete)
for barcode in dicts_fastq_info:
    if len(dicts_fastq_info[barcode]) > 1:
        try:
            to_complete[barcode]
        except KeyError as e:
            print('Le barcode-duplicat n\'est pas présent dans le RedCap: ' + barcode)
        # Clonage
        else:
            # On determine si on chain_clone et si on clone:
            if len(dicts_fastq_info[barcode]) > 2:
                # Chain clone (cas très exceptionnel)
                # TODO: utiliser la fonction
                # to_update = clone_chain_record(dicts_fastq_info[barcode], type_barcode_to_instrument,
                #     records_by_couple)
                pass
            else:
                # Clone simple
                new_record = clone_record(to_complete[barcode], type_barcode_to_instrument,
                    records_by_couple)

                # Completion de l'original et du clone
                updated_new_record = update(new_record, dicts_fastq_info[barcode])
                updated_model_record = update(to_complete[barcode], dicts_fastq_info[barcode])

                updated_records += [updated_new_record, updated_model_record]
    else:
        pass
        print(len(dicts_fastq_info[barcode]))

sys.exit('exit')
project.import_records(updated_records)
