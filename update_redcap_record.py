#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import yaml
import redcap
import os


class FieldNameError(redcap.RCAPIError):
    """ Error related to redcap field name."""
    def __init__(self, field_name, form_name):
        self.msg = 'The field name {}, does not match with the form {}'.format(field_name, form_name)

    def __str__(self):
        return self.msg

instrument_list = ['germline_dna_sequencing', 'tumor_dna_sequencing', 'rna_sequencing']

opt_parser = argparse.ArgumentParser(description=__doc__, prog='update_redcap.py')
# Affichage des champs modifiables
opt_parser.add_argument('-d', '--display-fields', required=False, action='store_true',
    help='Display redcap fields that can be updated.')
# Arguments pour utilisation classique
opt_parser.add_argument('-id', '--patient-id', required=False, help='Patient_id.')
opt_parser.add_argument('-c', '--config', default="config.yml", help='config file.')
opt_parser.add_argument('-s', '--secret', default="secret_config.yml", help='secret config file.')
opt_parser.add_argument('-f', '--field', required=False, help='RedCap field.')
opt_parser.add_argument('-v', '--value', required=False, help='New value.')
opt_parser.add_argument('-fastq', '--full-fastq', required=False,
    help='Content of FastQ filename Local. To use when updating a sequencing related field.')
opt_parser.add_argument('--seq-form', required=False, choices=instrument_list,
    help='Form name related to sequencing')
args = opt_parser.parse_args()

with open(args.config, 'r') as ymlfile:
    config = yaml.load(ymlfile)
with open(args.secret, 'r') as ymlfile:
    secret_config = yaml.load(ymlfile)
config.update(secret_config)

project = redcap.Project(config['redcap_api_url'], config['api_key'])

# Exploitation des métadata
to_display, metadata, redcap_fields = {}, {}, {}
for metadata_dict in project.metadata:
    to_display.setdefault(metadata_dict['form_name'], []).append(metadata_dict['field_name'])

    metadata[metadata_dict['field_name']] = {'field_type': metadata_dict['field_type'],
        'form_name': metadata_dict['form_name'],
        'select_choices_or_calculations': metadata_dict['select_choices_or_calculations']}
    redcap_fields.setdefault(metadata_dict['field_label'], {}).setdefault(metadata_dict['form_name'],
        metadata_dict['field_name'])

if args.display_fields:
    print(to_display)

target_patient_id = args.patient_id
target_field = args.field
new_value = args.value

ids, fields = [], []
ids.append(target_patient_id)
fields.append(target_field)

# On ne fait pas de blindage sur les champ complete
if 'complete' not in target_field:
    # Si le champ visé est un champ 'yes'/'no' on blinde la nouvelle valeur pour
    # n'avoir que du '1'/'0'
    if metadata[target_field]['field_type'] in ['yesno', 'truefalse']:
        bool_switch = {'no': '0', 'yes': '1', 'false': '0', 'true': '1', '0': '0', '1': '1'}
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

# Update de record répétable
if args.full_fastq:
    data_export = project.export_records(records=ids)

    instrument = args.seq_form

    for record in data_export:
        if record[redcap_fields['FastQ filename Local'][instrument]] == args.full_fastq:
            to_import = record
# Update de record dans un instrument non-répétable
else:
    data_export = project.export_records(records=ids, fields=fields)

    to_import = data_export[0]

# Update one record
to_import[target_field] = new_value


response = project.import_records(data_export)
print(response)
