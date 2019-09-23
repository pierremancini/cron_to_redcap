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

""" Export report from RedCap and format it as secondary samples plan. """


def args():
    """Parse options."""
    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('-p', '--path', required=False, help='Path for output samples_plan')
    return opt_parser.parse_args()

args = args()

with open('secret_config.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

report_id = 15 # Correspond au report Secondary samples plan


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


def make_sec_samples_plan(record_data, redcap_fields):

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
        set = record[redcap_fields['Set'][instrument]]
        patient_id = record['patient_id']
        case = '{}-{}-{}'.format(patient_id, set, analysis_type)

        # 1 fichier <-> 1 barcode
        # Ordre des lignes CD > MD > MR

        rows_tsv.append(case)

    return rows_tsv


rows_tsv = make_sec_samples_plan(response, redcap_fields)

if args.path:
    samples_plan_path = args.path
else:
    # Default path
    samples_plan_path = os.path.join('data', 'secondary_samples_plan.tsv')


with open(samples_plan_path, 'w') as csvfile:
    header = ['CASE', 'URL', 'REMOTEFILE', 'LOCALFILE']
    writer = csv.writer(csvfile, delimiter='\t')
    writer.writerow(header)
    for row in rows_tsv:
        writer.writerow(row)
