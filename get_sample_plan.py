#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests
import json
import argparse
import yaml
import csv
from redcap import Project

""" Export report from RedCap. """


def args():
    """Parse options."""
    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('-p', '--path', required=False, help='Path for output samples_plan')
    opt_parser.add_argument('--id', required=False, help='Report\'s id.')
    opt_parser.add_argument('-f', '--format', required=False, help='csv, xml or json')
    return opt_parser.parse_args()


args = args()

with open('secret_config.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

report_id = args.id
if not report_id:
    report_id = '5'  # Correspond à Full sample plan multipli
format = args.format
if not format:
    format = 'json'

data = {
    'token': config['api_key'],
    'content': 'report',
    'format': 'json',
    'report_id': report_id,
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'returnFormat': 'json'
}


api_url = 'http://ib101b/html/redcap/api/'

r = requests.post(api_url, data=data)

response = json.loads(r.text)  # Conversion json -> python

project = Project(api_url, config['api_key'])

# Strucure:
# {field_label: {instrument: field_name}}
redcap_fields = {}

# Définition dynamique (par rapport au champs créer dans RedCap) des types
for metadict in project.metadata:
    redcap_fields.setdefault(metadict['field_label'], {}).setdefault(metadict['form_name'],
        metadict['field_name'])


def make_samples_plan(record_data, redcap_fields):
    """ Make samples plan from RedCap report.

        Copie des fonctionnalités de la fonction transform_record_data du plugin ib101b.

        :return: Dictonnary containing data of script's main output
                 i.e. samples_plan .tsv file.
    """

    seq_instru = ['tumor_dna_sequencing', 'germline_dna_sequencing', 'rna_sequencing']

    rows_tsv = []

    for record in record_data:
        if record['redcap_repeat_instrument'] in seq_instru and record['redcap_repeat_instance']:
            instrument = record['redcap_repeat_instrument']

            if instrument == 'germline_dna_sequencing':
                analysis_type = 'CD'
            elif instrument == 'tumor_dna_sequencing':
                analysis_type = 'MD'
            elif instrument == 'rna_sequencing':
                analysis_type = 'MR'

            path_on_cng = record[redcap_fields['Path on cng'][instrument]]
            fastQ_file_cng = record[redcap_fields['FastQ filename CNG'][instrument]]
            fastQ_file_local = record[redcap_fields['FastQ filename Local'][instrument]]
            set = record[redcap_fields['Set'][instrument]]
            patient_id = record['patient_id']
            case = '{}-{}-{}'.format(patient_id, set, analysis_type)
            row = [case, path_on_cng, fastQ_file_cng, fastQ_file_local]
            rows_tsv.append(row)
            fastQ_file_cng_md5 = fastQ_file_cng + '.md5'
            fastQ_file_local_md5 = fastQ_file_local + '.md5'
            md5_row = [case, path_on_cng, fastQ_file_cng_md5, fastQ_file_local_md5]
            rows_tsv.append(md5_row)
    return rows_tsv


rows_tsv = make_samples_plan(response, redcap_fields)


if args.path:
    samples_plan_path = args.path
else:
    # Default path
    samples_plan_path = os.path.join('data', 'samples_plan_get.tsv')


with open(samples_plan_path, 'w') as csvfile:
    header = ['CASE', 'URL', 'REMOTEFILE', 'LOCALFILE']
    writer = csv.writer(csvfile, delimiter='\t')
    writer.writerow(header)
    for row in rows_tsv:
        writer.writerow(row)
