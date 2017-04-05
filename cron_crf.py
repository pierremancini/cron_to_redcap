#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Ce script est destiné à être appelé par un cron pour interagir avec le CRF.
"""

import os
import csv
import sys
import yaml
from redcap import Project


def barcode_info_crf(dict_reader):
    """ 
        Format data from CRF's .csv file.

        Format to a data structure compatible with clone_record. 
    """

    # Structure : {barcode: {'patient_id': ... , 'type_barcode': ... }}
    dict = {}

    for line in dict_reader:
        patient_id = line['patient_id']
        for index in line:
            if index in barcode_index and line[index]:
                dict.setdefault(line[index], {'patient_id': patient_id, 'type_barcode': index})
    return dict


def sort_records(response):
    """
        Sort record by barcode.

        Return a dictionnary with barcodes as key.
    """

    dict = {}

    for record in response:
        for index in record:
            if index == 'patient_id' and record[index]:
                patient_id = record[index]
            if index == 'redcap_repeat_instrument' and record[index]:
                redcap_repeat_instrument = record[index]

        try:
            dict.setdefault(patient_id, {}).setdefault(redcap_repeat_instrument, []).append(record)
        except UnboundLocalError:
            pass

    return dict


def get_to_clone(dict_crf, records_redcap, type_barcode_to_instrument):
    """
        Return list of record's barcode that have to be cloned in Redcap
        and list of record's that have to be created in Redcap.

        1. - Find couples: (patient_id, type_instrument)redcap & (patient_id, type_instrument)CRF
           - Couple that does match give the barcodes "to_create"
        2. Filter couples with same barcode,
           the remaining barcodes are return as to_clone_barcode

        :param dict_crf: output of barcode_info_crf()
        :param records_redcap: record from redcap sorted by barcode and intrument type
        :param type_barcode_to_instrument: give instrument type with barcode type
    """

    instrument_to_barcode = {'germline_dna_sequencing': 'germline_dna_cng_barcode',
         'tumor_dna_sequencing': 'tumor_dna_barcode',
         'rna_sequencing': 'rna_gcn_barcode'}

    t_redcap = tuple((patient_id, instrument) for patient_id in records_redcap for instrument in records_redcap[patient_id])

    flag_to_clone = True

    to_clone_barcode = []
    to_create_barcode = []

    for barcode in dict_crf:
        patient_id_crf = dict_crf[barcode]['patient_id']
        inst_crf = type_barcode_to_instrument[dict_crf[barcode]['type_barcode']]
        if (patient_id_crf, inst_crf) in t_redcap:
            flag_to_clone = True
            for record in records_redcap[patient_id_crf][inst_crf]:
                if barcode == record[instrument_to_barcode[inst_crf]]:
                    flag_to_clone = False
            if flag_to_clone:
                to_clone_barcode.append(barcode)
        else:
            to_create_barcode.append(barcode)

    return (to_clone_barcode, to_create_barcode)


def clone_record(barcode, dict_info, type_barcode_inst, records_redcap):
    """
        Create a clone of the record in the redcap that has the same barcode
        and increment the 'redcap_repeat_instance' number.
    """

    patient_id = dict_info['patient_id']
    instrument_type = type_barcode_inst[dict_info['type_barcode']]

    def get_max_instance_number(patient_id, instrument, records_redcap):
        """
            Return instance number.
        """

        max_instance_number = 0
        for record in records_redcap[patient_id][instrument]:
            if record['redcap_repeat_instance']:
                if record['redcap_repeat_instance'] > max_instance_number:
                    max_instance_number = record['redcap_repeat_instance']

        return max_instance_number

    # On determine l'instance number
    # et on incrémente
    instance_number = get_max_instance_number(patient_id, instrument_type, records_redcap) + 1

    new_record = {'redcap_repeat_instrument': instrument_type,
                  'patient_id': patient_id,
                  dict_info['type_barcode']: barcode,
                  'redcap_repeat_instance': instance_number
                  }

    return new_record


def create_record(barcode, dict_info):
    """
        Create new record for redcap.
    """

    new_record = {'redcap_repeat_instrument': dict_info['type_barcode'],
                  'patient_id': dict_info['patient_id'],
                  dict_info['type_barcode']: barcode}

    return new_record


with open('config_crf.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

response = project.export_records()

barcode_index = ['germline_dna_cng_barcode', 'tumor_dna_barcode', 'rna_gcn_barcode']

instrument_index = ['germline_dna_sequencing', 'tumor_dna_sequencing', 'rna_sequencing']

# Correspondance des champs barcode et redcap_repeated_instrument
# dans un résultat d'exportation de données via l'api redcap
type_barcode_to_instrument = {'germline_dna_cng_barcode': 'germline_dna_sequencing',
     'tumor_dna_barcode': 'tumor_dna_sequencing',
     'rna_gcn_barcode': 'rna_sequencing'}

barcode_redcap = [record[index] for record in response for index in record if index in barcode_index]
barcode_redcap = list(filter(None, barcode_redcap))

with open(os.path.join('data', 'CRF_mock.tsv'), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')
    dict_crf_barcode = barcode_info_crf(dict_reader)

# Structure :
# {patient_id: { instrument_instance: {record} } }
records_redcap = sort_records(response)

records_to_import = []

to_clone_barcode, to_create_barcode = get_to_clone(dict_crf_barcode, records_redcap, type_barcode_to_instrument)

for barcode in to_clone_barcode:
    try:
        records_to_import.append(clone_record(barcode,
            dict_crf_barcode[barcode],
            type_barcode_to_instrument,
            records_redcap))
    except KeyError as e:
        print('KeyError clone_record()')
        pass

for barcode in to_create_barcode:
    try:
        records_to_import.append(create_record(barcode, dict_crf_barcode[barcode]))
    except KeyError as e:
        raise e
        print('KeyError create_record()')
        pass

project.import_records(records_to_import)
