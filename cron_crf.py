#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    Ce script est destiné à être appelé par un cron pour interagir avec le CRF.
"""

import csv
import requests
import yaml
from redcap import Project

with open('config_crf.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

