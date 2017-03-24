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


patient_id_redcap = [record['patient_id'] for record in response]


with open(os.path.join('data', 'CRF_mock.tsv'), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')

    csv_content = [line for line in dict_reader]

    patient_id_crf = [line['patient_id'] for line in csv_content]

    patient_to_import = set(patient_id_crf) - set(patient_id_redcap)

    # Les records déjà présent dans redcap sont exclus
    record_to_import = [record
        for record in csv_content if record['patient_id'] in patient_to_import]


# Gérer incrémenter le champ repeated instance si le patient_id est le même
# (si même barecode aussi)

project.import_records(record_to_import)



# Les données du CRF seront surement dans un fichier au format csv
