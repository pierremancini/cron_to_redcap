#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" Unit test with pytest framework. """

from cron_crf import treat_crf
import csv
import yaml
from data_test_treat_crf import *

import logging
import logging.config


def test_dict_content():

    with open('test/logging.yml', 'r') as ymlfile:
        log_config = yaml.load(ymlfile)

    with open("./config.yml", 'r') as ymlfile:
        config = yaml.load(ymlfile)

    with open("test/cron_crf_test/data/MULTIPLI_Sequencing_mock.tsv", "r") as crffile:
        crf_data = treat_crf(crffile, config['corresp'], mock_redcap_metadata)

    assert crf_data == mock_crf_data
