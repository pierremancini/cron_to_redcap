#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Met toutes les valeurs sent_at de RedCap à Null. """


import update_redcap_record as redcap_record
from redcap import Project
import yaml
import argparse

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

response = project.export_records(fields=['patient_id'])

for subrecord in response:
    redcap_record.update(config['redcap_api_url'], config['api_key'], subrecord['patient_id'],
    'sent_at', '', 'bioinformatic_analysis')
