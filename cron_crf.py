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
import argparse

from project_logging import set_root_logger

from pprint import pprint

logger = logging.getLogger(__name__)


def treat_crf(file_handle, corresp, project_metadata):
    """
        Transform data from CRF.

        :param file_handle: Content of .tsv's CRF file
        :param barcode_index: Correspondance colonne fichier/champ redcap
        :param project_metadata: Redcap project metadata

        :return: - couple_count: data relative to barcode
                 - other_data: data contained in other fields of crf file. Includes clinical data.
    """

    # Couple = patient_id & type_barcode,
    # est utilisé comme clé primaire dans le fichier CRF et le RedCap

    # Structure
    # {(patient_id, type_barcode): nb}
    couple_count = {}

    # {patient_id: {champ_redcap: valeur}}
    other_data = {}

    dict_reader = csv.DictReader(file_handle, delimiter='\t')

    # Exploitation des métadata
    metadata, redcap_fields, choices_map = {}, {}, {}

    def choices_mapping(v):
        """
            Create mapping between value and label des champs radio et switch
            form project metadata.


            :param v: Value of 'select_choices_or_calculations' from project metadata.

            :return: Map of value and label of a select_choices field.
                     Return None if no value as argument is provided.
        """

        if v:
            a = v.split('|')
            map = {sub_a.split(', ')[1].strip(): sub_a.split(', ')[0].strip() for sub_a in a}
            return map
        else:
            return None

    def reverse_map(map):

        reverse_map = {}

        for key, value in map.items():
            if isinstance(value, list):
                for i in value:
                    reverse_map[i] = key
            else:
                reverse_map[value] = key

        return reverse_map

    inv_corresp = reverse_map(corresp['other'])

    for metadata_dict in project_metadata:
        metadata[metadata_dict['field_name']] = {'field_type': metadata_dict['field_type'],
            'form_name': metadata_dict['form_name'],
            'select_choices_or_calculations': metadata_dict['select_choices_or_calculations']}
        choices_map[metadata_dict['field_name']] = choices_mapping(metadata_dict['select_choices_or_calculations'])


    for colomn in dict_reader.fieldnames:
        if colomn not in corresp['barcode'] and colomn not in corresp['other']:
            logger.info('{} colomn is ignored by the script'.format(colomn))

    for line in dict_reader:
        patient_id = line[inv_corresp['patient_id']]
        other_data[patient_id] = {}

        # Gestion des champs histotype
        try:
            if line[inv_corresp['histotype_multisarc']] and line[inv_corresp['histotype_multisarc_other']]:
                logger.warning('Les colonnes \'histotype_multisarc\' et \'histotype_multisarc_other\''
                ' sont remplis, \'histotype_multisarc_other\' sera ignorée')

            if line[inv_corresp['histotype_acompli']] and line[inv_corresp['histotype_acompli_other']]:
                logger.warning('Les colonnes \'histotype_acompli\' et \'histotype_multisarc_other\''
                ' sont remplis, \'histotype_acompli_other\' sera ignorée.')
        except KeyError as e:
            # Passe l'erreur si l'une des 4 colonnes histotype non présente dans le fichier
            key = str(e).replace("'", "")
            if key not in [item for item in corresp['other'].keys()]:
                raise e

        for index in line:
            if index in corresp['barcode'] and line[index]:
                couple_count.setdefault((patient_id, corresp['barcode'][index]), {'count': 0, 'barcode': []})
                couple_count[(patient_id, corresp['barcode'][index])]['count'] += 1
                couple_count[(patient_id, corresp['barcode'][index])]['barcode'].append(line[index])

            # Gestion des autres données, notement les clinical data
            elif index in corresp['other'] and line[index]:

                redcap_labels = corresp['other'][index]

                if not isinstance(redcap_labels, list):
                    redcap_labels = [redcap_labels]

                for redcap_label in redcap_labels:
                    try:
                        if metadata[redcap_label]['field_type'] in ['radio', 'dropdown', 'yesno']:
                            try:
                                other_data[patient_id][redcap_label] = choices_map[redcap_label][line[index]]
                            except KeyError as e:
                                if line[index] == 'FFPE block':
                                    other_data[patient_id][redcap_label] = choices_map[redcap_label]['FFPE']
                                    logger.info('La valeur \'FFPE block\' est convertie en \'FFPE\'')
                                else:
                                    raise e
                        else:
                            other_data[patient_id][redcap_label] = line[index]
                    except KeyError as e:
                        if redcap_label not in ['histotype_multisarc_other', 'histotype_acompli_other']:
                            raise e


        # Déduction du tumor_type
        # acompli -> 1 | Colon
        # multisarc -> 2 | Sarcoma
        if line[inv_corresp['histotype_multisarc']] and line[inv_corresp['histotype_acompli']]:
            raise ValueError('Can not be both acompli and multisarc')
        elif line[inv_corresp['histotype_multisarc']] or line[inv_corresp['histotype_multisarc_other']]:
            other_data[patient_id]['tumor_type'] = '2'
        elif line[inv_corresp['histotype_acompli']] or line[inv_corresp['histotype_acompli_other']]:
            other_data[patient_id]['tumor_type'] = '1'

    return {'couple_count': couple_count, 'other_data': other_data}


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


def handle_exception(exc_type, exc_value, exc_traceback):

    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def bring_crf_file(script_config):
    """ Copy crf file localy from FTP server

        :param script_config: Script's configuration dictionary.

        :return: path of downloaded crf file.

    """

    head_crf, tail_crf = os.path.split(config['remote_crf_file'])

    path_download = config['local_crf_file']

    with ftplib.FTP_TLS(config['crf_host']) as ftps:
        ftps.login(config['login_crf'], config['password_crf'])
        # Encrypt all data, not only login/password
        ftps.prot_p()
        # Déclare l'IP comme étant de la famille v6 pour être compatible avec ftplib (même si on reste en v4)
        # cf: stackoverflow.com/questions/35581425/python-ftps-hangs-on-directory-list-in-passive-mode
        ftps.af = socket.AF_INET6
        ftps.cwd(head_crf)

        with open(path_download, 'wb') as f:
            ftps.retrbinary('RETR {}'.format(tail_crf), lambda x: f.write(x.decode("ISO-8859-1").encode("utf-8")))

    return path_download


def args():
    """Parse options."""
    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('-c', '--config', default="config.yml", help='config file.')
    opt_parser.add_argument('-s', '--secret', default="secret_config.yml", help='secret config file.')
    opt_parser.add_argument('-l', '--log', default="logging.yml", help='logging configuration file.')
    opt_parser.add_argument('--dev', action='store_true', help='developpement mode. Does not look for crf file on FTP.')
    return opt_parser.parse_args()


if __name__ == '__main__':
    args = args()

    # On log les uncaught exceptions
    sys.excepthook = handle_exception

    with open(args.config, 'r') as ymlfile:
        config = yaml.load(ymlfile)
    with open(args.secret, 'r') as ymlfile:
        secret_config = yaml.load(ymlfile)
    config.update(secret_config)

    with open(args.log, 'r') as ymlfile:
        log_config = yaml.load(ymlfile)

    logger = set_root_logger(config['path_to_log'], log_config)

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

    if args.dev:
        # local_path_crf = os.path.join(config['path_to_data'], 'crf_extraction',
        # 'MULTIPLI_Sequencing_20180822_barcode.csv')
        local_path_crf = os.path.join('test', 'cron_crf_test', 'data', 'MULTIPLI_Sequencing_mock.tsv')
    else:
        # get crf file with ftps
        local_path_crf = bring_crf_file(config)

    with open(local_path_crf, 'r') as csvfile:
        crf_data = treat_crf(csvfile, config['corresp'], project.metadata)

    # Les couple patient_id, type_barcode des records non vide de redcap
    pack = treat_redcap_response(response, redcap_fields)
    redcap_couple, redcap_barcodes, redcap_records = pack

    pack = create_n_clone(crf_data['couple_count'], redcap_couple, redcap_barcodes, redcap_records,
        type_barcode_to_instrument)

    to_clone_barcode, clone_chain, to_create_barcode, create_chain = pack

    records_to_import = list(itertools.chain(to_clone_barcode, clone_chain, to_create_barcode,
        create_chain))

    for patient_id in crf_data['other_data']:
        record = {"patient_id": patient_id,
                  "redcap_repeat_instrument": "",
                  "redcap_repeat_instance": ""}

        for index in crf_data['other_data'][patient_id]:
            record[index] = crf_data['other_data'][patient_id][index]

        records_to_import.append(record)

    project.import_records(records_to_import)
