#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Mise à jour des données cliniques de tout les patients (27/11/17) """

import update_redcap_record as redcap_record
from redcap import Project
import yaml
import argparse
import os
import csv
import time

import re

import sys
from pprint import pprint


def args():
    """Parse options."""
    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('-c', '--config', default="config.yml", help='config file.')
    opt_parser.add_argument('-s', '--secret', default="secret_config.yml", help='secret config file.')
    opt_parser.add_argument('-l', '--log', default="logging.yml", help='logging configuration file.')
    return opt_parser.parse_args()


args = args()

with open(args.config, 'r') as ymlfile:
    config = yaml.load(ymlfile)
with open(args.secret, 'r') as ymlfile:
    secret_config = yaml.load(ymlfile)
config.update(secret_config)

project = Project(config['redcap_api_url'], config['api_key'])

# {patient_id: {field: value, field: value}}
import_dict = {}

with open(os.path.join('data', 'AR_20171124_150711.txt'), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='#')
    next(dict_reader)
    for line in dict_reader:

        import_dict.setdefault(line['USUBJID'], {})

        # On récupère les colones correspondant aux champs des bioinformatic analysis

        # Date of FastQ files receipt at bioinformatic unit
        # RedCap: Date if receipt of all needed files

        line['ARRCDAT'] = line['ARRCDAT'].strip()
        line['ARAVADAT'] = line['ARAVADAT'].strip()

        if line['ARRCDAT'] and line['ARRCDAT'] != '':
            match = re.search(r'(\d{2})(\D{3})(\d{4})', line['ARRCDAT'])
            day, mounth, year = match.group(1, 2, 3)

            if mounth == 'AUG':
                mounth = '08'
            elif mounth == 'JUL':
                mounth = '07'
            else:
                raise ValueError

            date = '{}-{}-{}'.format(year, mounth, day)

            import_dict[line['USUBJID']]['date_receipt_files'] = date

        else:
            import_dict[line['USUBJID']]['date_receipt_files'] = line['ARRCDAT']



        # FastQ files quality control (Y/N)
        # RedCap: Quality control
        import_dict[line['USUBJID']]['quality_control'] = line['ARFQQC']

        # FastQ files availability in genVarXplorer (Y/N)
        # RedCap; Availability in genVarXplorer for interpretation
        import_dict[line['USUBJID']]['availab_genvarxplorer'] = line['ARGVX']

        # Date of FastQ files availability in genVarXplorer
        # RedCap: If yes data of availability
        if line['ARAVADAT'] and line['ARAVADAT'] != '':
            match = re.search(r'(\d{2})(\D{3})(\d{4})', line['ARAVADAT'])
            day, mounth, year = match.group(1, 2, 3)

            if mounth == 'AUG':
                mounth = '08'
            elif mounth == 'JUL':
                mounth = '07'
            elif mounth == 'SEP':
                mounth = '09'
            else:
                raise ValueError

            date = '{}-{}-{}'.format(year, mounth, day)
            import_dict[line['USUBJID']]['date_of_availability'] = date
        else:
            import_dict[line['USUBJID']]['date_of_availability'] = line['ARAVADAT']


for patient_id in import_dict:
    # Patient_id non présent sur redcap
    if patient_id not in ['T02-0005-RF', 'T02-0013-BM', 'T02-0007-BC']:
        print(patient_id)
        for field in import_dict[patient_id]:
            try:
                redcap_record.update(config['redcap_api_url'], config['api_key'], patient_id,
                        field, import_dict[patient_id][field], 'bioinformatic_analysis')
            except KeyError:
                print('KeyError sur {}'.format(patient_id))
                print('Field: {}, value: {}'.format(field, import_dict[patient_id][field]))
        


