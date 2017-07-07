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
import ftplib
import socket

# TODO: Mettre logger les erreurs si le script est en production
# Avec rotation de fichiers ? Plusieurs fichiers ?


def set_logger(config_dict):
    """ Gère le système de log.

        1. Check et création des dossier de log.
        2. Instanciation de l'objet logger pour le reste du script.
    """

    # Génération du path des logs dynamiquement en fonction du nom du script
    project_folder = os.path.relpath(__file__, '..').split('/')[0]
    name = os.path.splitext(__file__)[0]

    path = '/var/log/{}/{}/{}'.format(project_folder, name, name + '.log')
    config['handlers']['file_handler']['filename'] = path

    try:
        logging.config.dictConfig(config_dict)
    except ValueError:

        if not os.path.exists('/var/log/{}/{}'.format(project_folder, name)):
            os.makedirs('/var/log/{}/{}'.format(project_folder, name))
        # Il faut créé les dossiers de log
        logging.config.dictConfig(config_dict)

    logger = logging.getLogger()

    return logger


def treat_crf(reader, corresp):
    """
        Transform data from CRF.

        :param reader: Content of .tsv's CRF file
        :param barcode_index: Correspondance colonne fichier/champ redcap
    """

    # Couple = patient_id & type_barcode,
    # est utilisé comme clé primaire dans le fichier CRF et le RedCap

    # Structure
    # {(patient_id, type_barcode): nb}
    couple_count = {}

    for line in reader:
        patient_id = line['USUBJID']
        for index in line:
            if index in corresp and line[index]:
                couple_count.setdefault((patient_id, corresp[index]), {'count': 0, 'barcode': []})
                couple_count[(patient_id, corresp[index])]['count'] += 1
                couple_count[(patient_id, corresp[index])]['barcode'].append(line[index])

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

logger = set_logger(config)

def handle_exception(exc_type, exc_value, exc_traceback):

    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# On log les uncaught exceptions
sys.excepthook = handle_exception

with open('config.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)
with open('secret_config.yml', 'r') as ymlfile:
    secret_config = yaml.load(ymlfile)
config.update(secret_config)


api_url = config['redcap_api_url']
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

response = project.export_records()

path_crf_file = config['path_crf_file']
head_crf, tail_crf = os.path.split(path_crf_file)


# get crf file with ftps
with ftplib.FTP_TLS(config['crf_host']) as ftps:
    ftps = ftplib.FTP_TLS(config['crf_host'])
    ftps.login(config['login_crf'], config['password_crf'])
    # Encrypt all data, not only login/password
    ftps.prot_p()
    # Déclare l'IP comme étant de la famille v6 pour être compatible avec ftplib (même si on reste en v4)
    # cf: stackoverflow.com/questions/35581425/python-ftps-hangs-on-directory-list-in-passive-mode
    ftps.af = socket.AF_INET6
    ftps.cwd(head_crf)

    try:
        os.mkdir(os.path.join('data', 'crf_extraction'))
    except FileExistsError:
        pass

    with open(os.path.join('data', 'crf_extraction', tail_crf), 'wb') as f:
        ftps.retrbinary('RETR {}'.format(tail_crf), lambda x: f.write(x.decode("ISO-8859-1").encode("utf-8")))

with open(os.path.join('data', 'crf_extraction', tail_crf), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')
    couple_count = treat_crf(dict_reader, config['corresp'])

# Les couple patient_id, type_barcode des records non vide de redcap
pack = treat_redcap_response(response, redcap_fields)
redcap_couple, redcap_barcodes, redcap_records = pack

pack = create_n_clone(couple_count, redcap_couple,
    redcap_barcodes, redcap_records, type_barcode_to_instrument)
to_clone_barcode, clone_chain, to_create_barcode, create_chain = pack

records_to_import = list(itertools.chain(to_clone_barcode, clone_chain,
                                    to_create_barcode, create_chain))

project.import_records(records_to_import)
