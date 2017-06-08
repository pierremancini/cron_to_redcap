#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests
import json
import argparse


def get_options():
"""Parse options"""
opt_parser = argparse.ArgumentParser(description=__doc__)
opt_parser.add_argument('-i', '--infile', required=True, help='Input file.')
opt_parser.add_argument('-o', '--outfile', help='Output file.')
return opt_parser.parse_args()


with open('redcap.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

# TODO: 

data = {
    'token': config['api_key'],
    'content': 'report',
    'format': 'json',
    'report_id': '5',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'returnFormat': 'json'
}

r = requests.post('http://ib101b/html/redcap/api/', data = data)

print(r.text)

