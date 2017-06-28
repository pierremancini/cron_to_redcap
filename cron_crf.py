#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Ce script est destiné à être appelé par un cron pour une interaction
    entre le CRF et l'instance de Bergonié de RedCap.
"""

import os
import sys
import csv
import yaml
from redcap import Project
import itertools
import logging
import logging.config
import inspect

# TODO: Mettre logger les erreurs si le script est en production
# Avec rotation de fichiers ? Plusieurs fichiers ?


def set_logger(config_dict):

    logging.config.dictConfig(config_dict)

    # formatter = logging.Formatter('%(process)d :: %(asctime)s :: %(levelname)s :: %(message)s')

    # Création du logger

    # Root logger
    logger = logging.getLogger()

    # path = '/var/log/cron_to_redcap/cron_crf/cron_crf.log'
    # max_size = 10485760  # 10MB
    # backupCount = 20
    # handler = RotatingFileHandler(path, 'a', max_size, backupCount)
    # handler.setFormatter(formatter)
    # logger.addHandler(handler)

    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # logger.addHandler(stream_handler)


    return logger


def treat_crf(reader, barcode_index):
    """
        Transform data from CRF.

        :param reader: Content of .tsv's CRF file
        :param barcode_index: different types of index corresponding to barcode
    """

    # Couple = patient_id & type_barcode,
    # est utilisé comme clé primaire dans le fichier CRF et le RedCap

    # Structure
    # {(patient_id, type_barcode): nb}
    couple_count = {}

    for line in reader:
        patient_id = line['patient_id']
        for index in line:
            if index in barcode_index and line[index]:
                couple_count.setdefault((patient_id, index), {'count': 0, 'barcode': []})
                couple_count[(patient_id, index)]['count'] += 1
                couple_count[(patient_id, index)]['barcode'].append(line[index])

    return couple_count


def create_n_clone(couple_count, redcap_couple, redcap_barcodes, redcap_records, type_barcode_to_instrument):
    """
        Create and clone records

        :param couple_count: couples from CRF file with their occurences
        :param redcap_couple: couples from RedCap instance
        :param redcap_barcodes: barcodes values of RedCap
        :param redcap_records: records sorted by couple (patient_id, type_barcode)
        :param type_barcode_to_instrument: give instrument type with barcode type
    """

    def max_instance_number():
        """ Return maximal intance number from a list of record.

            List of record: Combination between the parameter redcap_records
            and the couple (patient_id, instrument).

            Nb: patient_id, instrument are variable global to the upper function.
        """

        # On determine l'instance number
        # et on incrémente

        # Log: Y a-t-il un trou dans la série des intance number ?
        max_instance_number = 0
        for record in redcap_records[(patient_id, instrument)]:
            if int(record['redcap_repeat_instance']) > max_instance_number:
                max_instance_number = int(record['redcap_repeat_instance'])

        return max_instance_number

    def clone_record(to_clone_barcode):
        """
            Create record that is a clone of RedCap record.
        """

        new_record = {}

        if barcode[0] not in redcap_barcodes:
            instance_number = max_instance_number() + 1
            new_record = {'redcap_repeat_instrument': instrument,
                          'patient_id': patient_id,
                          type_barcode: barcode[0],
                          'redcap_repeat_instance': instance_number
                          }
            to_clone_barcode.append(new_record)

        return to_clone_barcode

    def clone_chain_record(clone_chain):
        """
            1. Create a chain of cloned record
            2. add it to other chained record i.e. clone_chain

            :param clone_chain: other chained record
        """

        new_records = []

        instance_number = max_instance_number() + 1

        for barcode in couple_count[couple]['barcode']:
            if barcode not in redcap_barcodes:
                new_records.append({'redcap_repeat_instrument': instrument,
                                  'patient_id': patient_id,
                                  type_barcode: barcode,
                                  'redcap_repeat_instance': instance_number})
                instance_number += 1

        clone_chain += new_records

        return clone_chain

    def create_record(to_create_barcode):
        """
            Create a record that has no duplicate (same patient_di, type barcode) in
            RedCap instance.
        """

        if barcode[0] not in redcap_barcodes:
            new_record = {'redcap_repeat_instrument': instrument,
                          'patient_id': patient_id,
                          type_barcode: barcode[0]}
            to_create_barcode.append(new_record)

        return to_create_barcode

    def create_chain_record(create_chain):
        """
            1. Create a chain of record
            2. Add it to other chained record i.e. create_chain.

            :param create_chain: other chained record
        """

        new_records = []
        instance_number = 1

        for barcode in couple_count[couple]['barcode']:
            if barcode not in redcap_barcodes:
                new_records.append({'redcap_repeat_instrument': instrument,
                                   'patient_id': patient_id,
                                   type_barcode: barcode,
                                   'redcap_repeat_instance': instance_number})
                instance_number += 1

        create_chain += new_records

        return create_chain

    to_clone_barcode = []
    clone_chain = []
    to_create_barcode = []
    create_chain = []

    for couple in couple_count:

        # Closure pour les fonctions clone/create/chain
        patient_id = couple[0]
        type_barcode = couple[1]
        instrument = type_barcode_to_instrument[couple[1]]

        # Doublon dans le fichier CRF
        doublon = couple_count[couple]['count'] > 1

        # Existe-t-il déjà un record dans redcap avec le même patient_id et le même type de barcode:
        clone = (couple[0], couple[1]) in redcap_couple

        barcode = couple_count[couple]['barcode']

        # Log: info des différents create et clone de record ?
        if (not doublon) and (not clone):
            to_create_barcode = create_record(to_create_barcode)

        if (not doublon) and clone:
            to_clone_barcode = clone_record(to_clone_barcode)

        if doublon and (not clone):
            create_chain = create_chain_record(create_chain)

        if doublon and clone:
            clone_chain = clone_chain_record(clone_chain)

    return (to_clone_barcode, clone_chain, to_create_barcode, create_chain)


def treat_redcap_response(response, redcap_fields):
    """
        Transform RedCap API's response.

        :param response: 'response' list from RedCap API
        :param redcap_fields: Use to get different type of barcode according to instrument
    """

    # Strucuture:
    # (patient_id, type_barcode)
    redcap_couple = []

    redcap_barcodes = []

    # Structure:
    # {patient_id: {instrument: {record}}}
    redcap_records = {}

    for record in response[1:]:
        if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
            patient_id = record['patient_id']
            instrument = record['redcap_repeat_instrument']
            index = redcap_fields['Barcode'][instrument]
            if record[index]:
                redcap_couple.append((patient_id, index))
                redcap_barcodes.append(record[index])
                redcap_records.setdefault((patient_id, instrument), []).append(record)
            else:
                logger.warning('Un record associé au patient_id {} et à l\'instrument {} ne possède pas de barcode'.format(patient_id, instrument))

    return redcap_couple, redcap_barcodes, redcap_records


with open('logging.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)
# Génération du path des logs dynamiquement en fonction du nom du script
path = '/var/log/cron_to_redcap/'
config['handlers']['file_handler']['filename'] = path
logger = set_logger(config)


def handle_exception(exc_type, exc_value, exc_traceback):

    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# On log les uncaught exceptions
sys.excepthook = handle_exception

with open('secret_config.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

# Strucure:
# {field_label: {instrument: field_name}}
redcap_fields = {}

# Définition dynamique (par rapport au champs créer dans RedCap) des types
for metadict in project.metadata:
    redcap_fields.setdefault(metadict['field_label'], {}).setdefault(metadict['form_name'], metadict['field_name'])

# Correspondance des champs barcode et redcap_repeated_instrument
# dans un résultat d'exportation de données via l'api redcap
type_barcode_to_instrument = {field_name: instrument for instrument, field_name in redcap_fields['Barcode'].items()}
barcode_index = list(redcap_fields['Barcode'].values())

response = project.export_records()

with open(os.path.join('data', 'CRF_mock.tsv'), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')
    couple_count = treat_crf(dict_reader, barcode_index)

# Les couple patient_id, type_barcode des records non vide de redcap
pack = treat_redcap_response(response, redcap_fields)
redcap_couple, redcap_barcodes, redcap_records = pack

pack = create_n_clone(couple_count, redcap_couple,
    redcap_barcodes, redcap_records, type_barcode_to_instrument)
to_clone_barcode, clone_chain, to_create_barcode, create_chain = pack

records_to_import = list(itertools.chain(to_clone_barcode, clone_chain,
                                    to_create_barcode, create_chain))

project.import_records(records_to_import)
