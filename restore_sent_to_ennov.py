#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from redcap import Project
import sys
import yaml

from pprint import pprint

record = []


with open("config.yml", 'r') as ymlfile:
    config = yaml.load(ymlfile)
with open("secret_config.yml", 'r') as ymlfile:
    secret_config = yaml.load(ymlfile)
config.update(secret_config)


api_url = config['redcap_api_url']
project = Project(api_url, config['api_key'])

with open('data/sent_to_ennov_at_backup.csv', "r") as f:
    dict_reader = csv.DictReader(f, delimiter=',')


    for line in dict_reader:
        record.append({'patient_id': line['Patient ID'],
                       'sent_to_ennov_at': line['Sent to ennov at']
                     })


    project.import_records(record)