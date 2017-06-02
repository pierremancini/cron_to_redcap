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
import logging
from logging.handlers import RotatingFileHandler


def set_logger(logger_level):
    """ Set logger in rotating files and stream """

    # Création du logger

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logger_level)

    path = '/var/log/cron_to_redcap/cron_cng/cron_cng.log'
    max_size = 10485760  # 10MB
    backupCount = 20
    handler = RotatingFileHandler(path, 'a', max_size, backupCount)
    formatter = logging.Formatter('%(process)d :: %(asctime)s :: %(levelname)s :: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    formatter = logging.Formatter('%(levelname)s :: %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def get_md5(fastq_path):
    """ Get md5 value with path to fastq file name."""

    md5_path = fastq_path + '.md5'

    response = requests.get(md5_path, auth=(config['login'], config['password'])).content

    md5 = response.decode().split(' ')[0]

    return md5


def info_from_set(set_to_complete):
    """ Get and transform data from url's set on CNG."""

    # Strucure
    # {barcode:
    #    [{project, kit_code, barcode, lane, read, end_of_file, flowcell, tag}},
    #        ... ]
    dicts_fastq_info = {}

    for set in set_to_complete:
        set_url = config['url_cng'] + set
        page = requests.get(set_url, auth=(config['login'], config['password']))
        soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

        fastq_gen = (file.string for file in soup.find_all('a') if re.search(r'fastq\.gz$', file.string))
        for fastq in fastq_gen:
            project, kit_code, barcode, lane, read, end_of_file = fastq.split('_')
            flowcell, tag = end_of_file.split('.')[:-2]
            md5 = get_md5(set_url + '/' + fastq)
            local_filename = '{}_{}_{}_{}.{}_{}_{}.fastq.gz'.format(
                project, kit_code, barcode, flowcell, tag, lane, read)
            dict_fastq_info = {'Set': set,
                               'FastQ filename CNG': fastq,
                               'FastQ filename Local': local_filename,
                               'Path on cng': set_url,
                               'md5 value': md5,
                               'Project': project,
                               'Kit code': kit_code,
                               'Barcode': barcode,
                               'Lane': lane,
                               'Read': read,
                               'Flowcell': flowcell,
                               'Tag': tag}
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


def multiple_update(record_list, redcap_fields, info_cng):
    """ Update RedCap records with CNG data.

        :param record_list: Liste des records a update
        :param info_cng: information du tirées du nom de fichier fastq avec le barcode
        correspondant.
    """

    records = []
    count = 0

    info_for_record = {}
    for record in record_list:
        instrument = record['redcap_repeat_instrument']
        for index in info_cng[count]:
            info_for_record[redcap_fields[index][instrument]] = info_cng[count][index]
        record.update(info_for_record)
        records.append(record)
        count += 1

    return records


def update(record, redcap_fields, info_cng):
    """ Update RedCap record with CNG data.

        :param record: Record to update
        :param info_cng: information du tirées du nom de fichier fastq avec le barcode
        correspondant.
    """

    info_for_record = {}
    instrument = record['redcap_repeat_instrument']
    for index in info_cng:
        info_for_record[redcap_fields[index][instrument]] = info_cng[index]

    record.update(info_for_record)

    return record


# Dans le cas d'un duplicat de barcode ce script doit cloner le record redcap correspondant
# Nb: ce cas sera exceptionnel
def clone_record(record_to_clone, redcap_fields, records_by_couple):
    """
        Create record that is a clone of RedCap record.
    """

    instrument = record_to_clone['redcap_repeat_instrument']
    type_barcode = redcap_fields['Barcode'][instrument]

    for index in record_to_clone:
        if record_to_clone[type_barcode]:
            barcode = record_to_clone[type_barcode]

    patient_id = record_to_clone['patient_id']

    instance_number = max_instance_number((patient_id, instrument),
        records_by_couple) + 1
    new_record = {'redcap_repeat_instrument': instrument,
                  'patient_id': patient_id,
                  type_barcode: barcode,
                  'redcap_repeat_instance': instance_number
                  }

    return new_record


def clone_chain_record(record_to_clone, redcap_fields, records_by_couple, num_of_clone):
    """
        Créer un série de record chainés.

        C'est à dire une série de record dont les instance_number se suivent.

        :param num_of_clone: number of clone we are looking for

        -------------
        :return: Les clone ET le record original

    """

    records = []
    instrument = record_to_clone['redcap_repeat_instrument']
    type_barcode = redcap_fields['Barcode'][instrument]

    for index in record_to_clone:
        if record_to_clone[type_barcode]:
            barcode = record_to_clone[type_barcode]

    patient_id = record_to_clone['patient_id']

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


def handle_uncaught_exc(exc_type, exc_value, exc_traceback):
    """ Handle uncaught exception."""

    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# On log les uncaught exceptions
sys.excepthook = handle_uncaught_exc

logger = set_logger(logging.WARNING)

# Lecture du fichier de configuration
with open('config_cng.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

# Partie API redcap
api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

# Strucure:
# {field_label: {instrument: field_name}}
redcap_fields = {}

# Définition dynamique (par rapport au champs créer dans RedCap) des types
for metadict in project.metadata:
    redcap_fields.setdefault(metadict['field_label'], {}).setdefault(metadict['form_name'], metadict['field_name'])

response = project.export_records()

# Record n'ayant pas de les champs path de remplis
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
    instrument = record['redcap_repeat_instrument']
    if record['redcap_repeat_instance'] and instrument:
        patient_id = record['patient_id']
        if record[redcap_fields['Barcode'][instrument]]:
            records_by_couple.setdefault((patient_id, record['redcap_repeat_instrument']), []).append(record)
        empty_path = True
        if not record[redcap_fields['Path on cng'][instrument]]:
            if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
                if record[redcap_fields['Barcode'][instrument]]:
                    barcode = record[redcap_fields['Barcode'][instrument]]
                to_complete.setdefault(barcode, []).append(record)
        else:
            # On retrouve le set dans le champ set
            sets_completed.append(record[redcap_fields['Set'][instrument]])

page = requests.get(config['url_cng'], auth=(config['login'], config['password']))
soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

# liste des att. des tag <a> avec pour nom 'set'
href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d/$', a.string)]
list_set_cng = [href[:-1] for href in href_set]

set_to_complete = set(list_set_cng) - set(sets_completed)

updated_records = []

# Dictionnaire avec les barcodes en 1ère clé
dicts_fastq_info = info_from_set(set_to_complete)


# TODO
"""
- Lever le warning sur to_complete.setdefault(barcode, record) != record
car avoir plusieur record pour un même barcode est normal.
- Il n'est pas normal d'avoir un même barcode sur different instrument. (Mettre un warning ?)

1. Comparer   

    to_complete[barcode]  avec  dicts_fastq_info[barcode] 

    Mettre un warning 'duplicat dans CNG par rapport au RedCap' si 
    dicts_fastq_info[barcode] > to_complete[barcode] 
        -> clone et chain clone
        -> désactiver le section clon et chain clone pour le debug

"""

# for barcode in dicts_fastq_info:
#     try:
#         to_complete[barcode]
#     except KeyError as e:
#         logger.warning('Le barcode {} n\'est pas présent dans le RedCap alors qu\'il est sur le CNG'.format(barcode))
#     else:




for barcode in dicts_fastq_info:
    try:
        to_complete[barcode]
    except KeyError as e:
        logger.warning('Le barcode {} n\'est pas présent dans le RedCap alors qu\'il est sur le CNG'.format(barcode))
    else:
        print(barcode)
        print('dicts_fastq_info[{}] : {}'.format(barcode, len(dicts_fastq_info[barcode])))
        print('to_complete[{}] : {}'.format(barcode, len(to_complete[barcode])))

        if len(dicts_fastq_info[barcode]) > 1:
            # On determine si on chain_clone et si on clone:
            if len(dicts_fastq_info[barcode]) > 2:
                # Chain clone (cas très exceptionnel)
                # Comme c'est exceptionnel il faut logger
                multiple_to_update = clone_chain_record(to_complete[barcode], redcap_fields,
                    records_by_couple, len(dicts_fastq_info[barcode]) - 1)

                updated_records += multiple_update(multiple_to_update, redcap_fields, dicts_fastq_info[barcode])

            else:
                # Clone simple
                new_record = clone_record(to_complete[barcode], redcap_fields,
                    records_by_couple)

                # Completion de l'original et du clone
                updated_new_record = update(new_record, redcap_fields, dicts_fastq_info[barcode][0])
                updated_model_record = update(to_complete[barcode], redcap_fields, dicts_fastq_info[barcode][1])

                updated_records += [updated_new_record, updated_model_record]
        else:
            print('else')
            # Update classique sans clonage
            updated_records.append(update(to_complete[barcode], redcap_fields, dicts_fastq_info[barcode][0]))

sys.exit('exit')
project.import_records(updated_records)
