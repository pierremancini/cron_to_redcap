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


def multiple_update(record_list, info_cng_list):
    """
        :param info_cng:
        :param record:
    """

    records = []
    index = 0

    for record in record_list:
        record.update(info_cng_list[index])
        fastq_path = config['url_cng'] + '/' + record['set'] + '/' + record['fullname']
        record['md5_value'] = get_md5(fastq_path)

        records.append(record)
        index += 1

    return records


def update(record, info_cng):
    """ Update RedCap record with CNG data.

        :parama info_cng: information du tirées du nom de fichier fastq avec le barcode
        correspondant.
    """

    record.update(info_cng)
    fastq_path = config['url_cng'] + '/' + record['set'] + '/' + record['fullname']
    record['md5_value'] = get_md5(fastq_path)

    return record


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


def clone_chain_record(record_to_clone, type_barcode_to_instrument, records_by_couple, num_of_clone):
    """
        Créer un série de record chainés.

        C'est à dire une série de record dont les instance_number se suivent.

        :param num_of_clone: number of clone we are looking for

        -------------
        :return: Les clone ET le record original

    """

    records = []

    for index in record_to_clone:
        if index in barcode_index and record_to_clone[index]:
            type_barcode = index
            barcode = record_to_clone[index]

    patient_id = record_to_clone['patient_id']
    instrument = type_barcode_to_instrument[type_barcode]

    count = 0
    instance_number = max_instance_number((patient_id, instrument),
        records_by_couple) + 1
    while count != num_of_clone:
        records.append({'redcap_repeat_instrument': instrument,
                  'patient_id': patient_id,
                  type_barcode: barcode,
                  'redcap_repeat_instance': instance_number})
        instance_number += 1
        count += 1

    records.append(record_to_clone)

    return records


updated_records = []

# Dictionnaire avec les barcodes en 1ère clé
dicts_fastq_info = info_from_set(set_to_complete)

for barcode in dicts_fastq_info:
    try:
        to_complete[barcode]
    except KeyError as e:
        print('Warning: Le barcode-duplicat n\'est pas présent dans le RedCap: ' + barcode)
    else:
        if len(dicts_fastq_info[barcode]) > 1:
            # On determine si on chain_clone et si on clone:
            if len(dicts_fastq_info[barcode]) > 2:
                # Chain clone (cas très exceptionnel)
                # Comme c'est exceptionnel il fuat logger
                multile_to_update = clone_chain_record(to_complete[barcode], type_barcode_to_instrument,
                    records_by_couple, len(dicts_fastq_info[barcode]) - 1)

                updated_records += multiple_update(multile_to_update, dicts_fastq_info[barcode])

            else:
                # Clone simple
                new_record = clone_record(to_complete[barcode], type_barcode_to_instrument,
                    records_by_couple)

                # Completion de l'original et du clone
                updated_new_record = update(new_record, dicts_fastq_info[barcode][0])
                updated_model_record = update(to_complete[barcode], dicts_fastq_info[barcode][1])

                updated_records += [updated_new_record, updated_model_record]
        else:
            # Update classique sans clonage
            updated_records.append(update(to_complete[barcode], dicts_fastq_info[barcode][0]))


sys.exit('exit')
project.import_records(updated_records)
