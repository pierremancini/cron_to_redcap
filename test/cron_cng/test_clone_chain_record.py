#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from cron_cng import clone_chain_record
from data_test_clone_chain_record import *


def test_returned_list_length():

    if isinstance(record_to_clone, (list, tuple)):
        record_to_clone_length = len(record_to_clone)
    else:
        record_to_clone_length = 1

    assert len(clone_chain_record(record_to_clone, redcap_fields,
        records_by_couple, num_of_clone)) == record_to_clone_length + num_of_clone
