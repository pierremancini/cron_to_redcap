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
from redcap import Project
import logging
import logging.config
import json
import argparse

from project_logging import set_root_logger


logger = logging.getLogger(__name__)


def get_md5(fastq_path, mock=False):
    """ Get md5 value with path to fastq file name.

        :param mock: Tell if the data taken from md5_by_path gobal variable.
    """
    if mock:

        return md5_by_path[fastq_path]
    else:

        md5_path = fastq_path + '.md5'

        response = requests.get(md5_path, auth=(config['login_cng'], config['password_cng'])).content
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
        page = requests.get(set_url + '/', auth=(config['login_cng'], config['password_cng']))

        if page.status_code == requests.codes.ok:

            soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

            fastq_gen = []

            for a_tag in soup.find_all('a'):
                if re.search(r'fastq\.((gz)|(bz)|(zip)|(bz2)|(tgz)|(tbz2))$', a_tag.get('href')):
                    for child in a_tag.children:
                        if isinstance(child, str):
                            fastq_gen.append(child)

            for fastq in fastq_gen:
                project, kit_code, barcode, lane, read, end_of_file = fastq.split('_')
                flowcell, tag = end_of_file.split('.')[:-2]
                # Passe la variable mock pour lire les md5 depuis le dump json et pas le CNG
                # pour avoir un debug/developpement plus rapide
                md5 = get_md5(set_url + '/' + fastq, args.mock)
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

        else:
            logger.error('Response de {}: {}'.format(config['url_cng'], page.status_code))

    return dicts_fastq_info


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

        :param record_to_clone: record(s) à cloner, dans le cas où on a plusieurs records
        (liste de record) ils seront traités comme étant identiques. Passer une liste de record
        permet de renvoyer retourner un nombre cohérents de record à updater: les records modèle
        (à compléter) et les clones. 
        :param num_of_clone: number of clone we are looking for

        -------------
        :return: Les clone ET le ou les original record originaux

    """
    records = []

    if isinstance(record_to_clone, (list, tuple)):
        records += record_to_clone
        record_to_clone = record_to_clone[0]
    else:
        records.append(record_to_clone)

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

    return records


def multiple_update(record_list, redcap_fields, info_cng):
    """ Update RedCap records with CNG data.

        :param record_list: Liste des records a update
        :param info_cng: information du tirées du nom de fichier fastq avec le barcode
        correspondant.
    """

    records = []
    count = 0

    try:
        info_for_record = {}
        for record in record_list:
            instrument = record['redcap_repeat_instrument']
            for index in info_cng[count]:
                info_for_record[redcap_fields[index][instrument]] = info_cng[count][index]
            record.update(info_for_record)
            records.append(record)
            count += 1
    except IndexError as e:
        raise e

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


def handle_uncaught_exc(exc_type, exc_value, exc_traceback):
    """ Handle uncaught exception."""

    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def args():
    """Parse options."""
    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('-m', '--mock', required=False, action='store_true',
    help='Active le mocking des données md5 des fastq en lisant fichier dump du CNG (pour le debug).')
    opt_parser.add_argument('-c', '--config', default="config.yml", help='config file.')
    opt_parser.add_argument('-s', '--secret', default="secret_config.yml", help='secret config file.')
    opt_parser.add_argument('-l', '--log', default="logging.yml", help='logging configuration file.')
    return opt_parser.parse_args()



if __name__ == '__main__':
    # On log les uncaught exceptions
    sys.excepthook = handle_uncaught_exc

    args = args()

    with open(args.config, 'r') as ymlfile:
        config = yaml.load(ymlfile)
    with open(args.secret, 'r') as ymlfile:
        secret_config = yaml.load(ymlfile)
    config.update(secret_config)

    logger = set_root_logger(config['path_to_log'], os.path.basename(__file__))

    # Partie API redcap
    api_url = config['redcap_api_url']
    project = Project(api_url, config['api_key'])

    # Strucure:
    # {field_label: {instrument: field_name}}
    redcap_fields = {}

    # Définition dynamique (par rapport au champs créer dans RedCap) des types
    for metadict in project.metadata:
        redcap_fields.setdefault(metadict['field_label'], {}).setdefault(metadict['form_name'],
            metadict['field_name'])

    response = project.export_records()

    # Record n'ayant pas les champs path remplis
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
                records_by_couple.setdefault((patient_id, record['redcap_repeat_instrument']),
                    []).append(record)
            if not record[redcap_fields['Path on cng'][instrument]]:
                if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
                    if record[redcap_fields['Barcode'][instrument]]:
                        barcode = record[redcap_fields['Barcode'][instrument]]
                    to_complete.setdefault(patient_id, {}).setdefault(barcode, []).append(record)
            else:
                # On retrouve le set dans le champ set
                sets_completed.append(record[redcap_fields['Set'][instrument]])

    page = requests.get(config['url_cng'], auth=(config['login_cng'], config['password_cng']),
        timeout=(3.05, 27))
    soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

    href_set = [a.get('href') for a in soup.find_all('a') if re.search(r'^set\d+/$', a.get('href'))]
    list_set_cng = [href[:-1] for href in href_set]

    # On ignore les sets déjà cloturés
    set_to_complete = set(list_set_cng) - set(sets_completed)

    # On force la lecture de certains sets
    if config['mandatory_set']:
        set_to_complete.update(config['mandatory_set'])

    # On ignore 'manuellement' certains sets
    set_to_complete = set_to_complete - set(config['ignored_set'])

    updated_records = []

    # Dictionnaire avec les barcodes en 1ère clé
    if args.mock:
        from dump_dicts_fastq_info import dicts_fastq_info
    else:
        dicts_fastq_info = info_from_set(set_to_complete)

    for barcode in dicts_fastq_info:
        for patient_id in to_complete:
            try:
                to_complete[patient_id][barcode]
            except KeyError as e:
                # On cherche les sets (cng) associé au barcode ayant déclenché le warning.
                sets = {dico['Set'] for dico in dicts_fastq_info[barcode]}
                sets_warning = ', '.join(sets)
                warn_msg = 'Le barcode {}, patient_id {} ({}) n\'est pas présent dans le RedCap alors qu\'il est sur le CNG.'.format(barcode, patient_id, sets_warning)
                logger.warning(warn_msg)
            else:
                # SARC2 et SARC3 doivent être filtré
                clone_nb = len(dicts_fastq_info[barcode]) - len(to_complete[patient_id][barcode])

                # 1er cas: le nombre de fastq CNG correspond au nombre de record à completer dans
                # RedCap -> pas de clonage
                if clone_nb == 0:
                    updated_records += multiple_update(to_complete[patient_id][barcode], redcap_fields,
                            dicts_fastq_info[barcode])

                # 2em cas: le nombre de fastq CNG et suppérieur au nombre de record à completer
                # dans RedCap -> clonage, cas classique
                elif clone_nb > 2:
                    multiple_to_update = clone_chain_record(to_complete[patient_id][barcode], redcap_fields,
                        records_by_couple, clone_nb)
                    updated_records += multiple_update(multiple_to_update, redcap_fields,
                        dicts_fastq_info[barcode])

    if dicts_fastq_info:
        for patient_id in to_complete:
            for barcode in to_complete[patient_id]:
                try:
                    dicts_fastq_info[barcode]
                except KeyError:
                    warn_msg = 'Analyse(s) déclarée(s) sur le CRF est manquante sur le site du CNG '\
                        'ou appartient à un set déjà intégré dans RedCap:\n'
                    for i in range(len(to_complete[patient_id][barcode])):
                        instrument = to_complete[patient_id][barcode][i]['redcap_repeat_instrument']
                        warn_msg += 'patient_id: {}, type d\'analyse: {}, barcode: {}\n'.format(patient_id,
                            instrument, barcode)
                    logger.warning(warn_msg)
    else:
        logger.debug('dicts_fastq_info is empty.')

    project.import_records(updated_records)
