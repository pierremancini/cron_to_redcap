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

with open('config_crf.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)


# On regarde les info déjà présentes sur RedCap
# Est-ce qu'on fait une diff comme dans cron_cng ?
api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

response = project.export_records()

barcode_index = ['germline_dna_cng_barcode', 'tumor_dna_barcode', 'rna_gcn_barcode']

barcode_redcap = [record[index] for record in response for index in record if index in barcode_index]
barcode_redcap = list(filter(None, barcode_redcap))

## Choix patient_id
# patient_id_redcap = [record['patient_id'] for record in response]

with open(os.path.join('data', 'CRF_mock.tsv'), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')

    csv_content = [line for line in dict_reader]

barcode_crf = [line[index] for line in csv_content for index in line if index in barcode_index]

barcode_import = set(barcode_crf) - set(barcode_redcap)

# Choix patient_id
# patient_id_crf = [line['patient_id'] for line in csv_content]
# patient_to_import = set(patient_id_crf) - set(patient_id_redcap)

# Les records déjà présent dans redcap sont exclus



def get_instance_nb_max(records_same_id_barcode_type):
    """
        Return the biggest redcap_repeat_instance.
    """

    max = 0
    for record in records_same_id_barcode_type:
        if record['redcap_repeat_instance'] > max:
            max = record['redcap_repeat_instance']
    return max


def create_record(barcode, patient_id, response):
    """ Update record from a (new) barcode.

        barcode arguement is a dictionnary giving the type of barcode
        and its value.

        response argument is the list of all records in redcap.

        Provide a 'redcap_repeat_instance' number according to
        the records already in redcap.
    """

    to_count = []
    redcap_repeat_instrument = ''

    for record_redcap in response:
        try:
            if record_redcap['patient_id'] == patient_id:
                # print(record_redcap['redcap_repeat_instrument'] + ' vs ' + barcode['index'])
                for index in barcode_index:
                    # On regarde pour le type de barcode qui nous interesse si il y a une valeur
                    # dans le record redcap.
                    if index == barcode['index']:
                        if record_redcap[index]:
                            to_count.append(record_redcap)
                            redcap_repeat_instrument = record_redcap['redcap_repeat_instrument']
        except KeyError as e:
            raise e
            # Ce patient_id n'existe pas encore

    instance_nb = get_instance_nb_max(to_count) + 1
    new_record = {'redcap_repeat_instrument': redcap_repeat_instrument,
                  'redcap_repeat_instance': instance_nb,
                  'patient_id': patient_id,
                  barcode['index']: barcode['value']}

    return new_record


records_to_import = []
for record in csv_content:
    for i in record:
        if i in barcode_index:
            # Le barcode n'est pas encore dans redcap
            if record[i] in barcode_import:
                records_to_import.append(create_record({'index': i, 'value': record[i]}, record['patient_id'], response))

project.import_records(records_to_import)
