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
import requests


# Script dependencies
with open('redcap.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)
api_url = 'http://ib101b/html/redcap/api/'


project = Project(api_url, config['api_key'])

opt_parser = argparse.ArgumentParser(description=__doc__)
opt_parser.add_argument('-d', '--display-fields', required=False, action='store_true',
    help='Display redcap fields that can be updated.')
opt_parser.add_argument('-id', '--patient-id', required=False,
    help='Display redcao fields that can be updated.')
opt_parser.add_argument('-f', '--field', required=False,
    help='Display redcao fields that can be updated.')
opt_parser.add_argument('-v', '--value', required=False,
    help='Display redcao fields that can be updated.')
args = opt_parser.parse_args()

to_display, metadata = {}, {}
for metadata_dict in project.metadata:
    to_display.setdefault(metadata_dict['form_name'], []).append(metadata_dict['field_name'])

    metadata[metadata_dict['field_name']] = {'field_type': metadata_dict['field_type'],
                                             'form_name': metadata_dict['form_name'],
                                             'select_choices_or_calculations': metadata_dict['select_choices_or_calculations']}

if args.display_fields:
    # {instrument: [fieldname]}
    print(to_display)
else:
    target_patient_id = args.patient_id
    target_field = args.field
    new_value = args.value

    api_url = 'http://ib101b/html/redcap/api/'

    ids, fields = [], []
    ids.append(target_patient_id)
    fields.append(target_field)

    # Si le champ visé est un champ 'yes'/'no' on blinde la nouvelle valeur pour
    # n'avoir que du '1'/'0'
    bool_switch = {'no': '0', 'yes': '1', 'false': '0', 'true': '1', '0': '0', '1': '1'}
    if metadata[target_field]['field_type'] in ['yesno', 'truefalse']:
        try:
            int(new_value)
        except ValueError:
            new_value = new_value.lower()
        new_value = bool_switch[new_value]

    if metadata[target_field]['field_type'] in ['radio', 'dropdown']:
        # On génère le radio_switch dynamiquement depuis les metadata
        raw = metadata[target_field]['select_choices_or_calculations']
        a = raw.split('|')
        radio_switch = {sub_a.split(', ')[1].strip(): sub_a.split(', ')[0].strip() for sub_a in a}
        try:
            new_value = int(new_value)
        except ValueError:
            new_value = radio_switch[new_value]

    data_export = project.export_records(records=ids, fields=fields)

    # Update one record
    data_export[0][target_field] = new_value
    print(data_export)
    response = project.import_records(data_export)
    print(response)
