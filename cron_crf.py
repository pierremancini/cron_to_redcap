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
import itertools


# TODO: Mettre logger les erreurs si le script est en production

def treat_crf(reader, barcode_index):
    """
        1. Format data from CRF's .csv file.
        2. Find all doublon in CRF file.

        TODO: Changer la doc pour les paramètres

        :param reader: Content of .tsv's CRF file
        :param barcode_index: different types of index corresponding to barcode
    """

    # Couple = patient_id & type_barcode,
    # est utilisé comme clé primaire dans le fichier CRF et le RedCap

    # Structure
    # {(patient_id, type_barcode): nb}
    couple_count = {}

    for line in reader:
        patient_id = line['patient_id']
        for index in line:
            if index in barcode_index and line[index]:
                couple_count.setdefault((patient_id, index), {'count': 0, 'barcode': []})
                couple_count[(patient_id, index)]['count'] += 1
                couple_count[(patient_id, index)]['barcode'].append(line[index])

    return couple_count


def create_clone_chains(couple_count, redcap_couple, redcap_barcodes, redcap_records):
    """
        TODO: changer la doc

        :param dict_crf: First output of treat_crf()
        :param doublons: ?
        :param redcap_couple: ...
        :param type_barcode_to_instrument: give instrument type with barcode type
    """

    # A comparer avec t_redcap pour avoir chained_barcode
    instrument_to_barcode = {'germline_dna_sequencing': 'germline_dna_cng_barcode',
         'tumor_dna_sequencing': 'tumor_dna_barcode',
         'rna_sequencing': 'rna_cng_barcode'}

    # Correspondance des champs barcode et redcap_repeated_instrument
    # dans un résultat d'exportation de données via l'api redcap
    type_barcode_to_instrument = {'germline_dna_cng_barcode': 'germline_dna_sequencing',
         'tumor_dna_barcode': 'tumor_dna_sequencing',
         'rna_cng_barcode': 'rna_sequencing'}

    def max_instance_number():
        """  """

        # On determine l'instance number
        # et on incrémente
        max_instance_number = 0
        for record in redcap_records[(patient_id, instrument)]:
            if int(record['redcap_repeat_instance']) > max_instance_number:
                max_instance_number = int(record['redcap_repeat_instance'])

        return max_instance_number

    def clone_record():
        """

        """
        if barcode not in redcap_barcodes:
            instance_number = max_instance_number() + 1
            new_record = {'redcap_repeat_instrument': instrument,
                          'patient_id': patient_id,
                          type_barcode: barcode[0],
                          'redcap_repeat_instance': instance_number
                          }

        return new_record

    def clone_chain_record(clone_chain):
        """
            :param clone_chain:
        """

        new_records = []

        instance_number = max_instance_number() + 1

        for barcode in couple_count[couple]['barcode']:
            if barcode not in redcap_barcodes:
                new_records.append({'redcap_repeat_instrument': instrument,
                                  'patient_id': patient_id,
                                  type_barcode: barcode,
                                  'redcap_repeat_instance': instance_number})
                instance_number += 1

        clone_chain += new_records

        return clone_chain

    def create_record():
        """
            Create new record for redcap.
        """

        new_record = {'redcap_repeat_instrument': instrument,
                      'patient_id': patient_id,
                      type_barcode: barcode[0]}

        return new_record

    def create_chain_record(create_chain):
        """
            :param create_chain:
        """

        new_records = []
        instance_number = 1

        # Ici la variable barcode est un
        for barcode in couple_count[couple]['barcode']:
            new_records.append({'redcap_repeat_instrument': instrument,
                               'patient_id': patient_id,
                               type_barcode: barcode,
                               'redcap_repeat_instance': instance_number})
            instance_number += 1

        create_chain += new_records

        return create_chain


    # Les record vide ne doivent pas apparaitrent

    to_clone_barcode = []
    clone_chain = []
    to_create_barcode = []
    create_chain = []

    for couple in couple_count:

        # Closure pour les fonctions clone/create/chain
        barcode = couple_count[couple]['barcode']
        patient_id = couple[0]
        type_barcode = couple[1]
        instrument = type_barcode_to_instrument[couple[1]]

        # Doublon dans le fichier CRF
        doublon = couple_count[couple]['count'] > 1

        # Existe-t-il déjà un record dans redcap avec le même patient_id et le même type de barcode:
        clone = (couple[0], couple[1]) in redcap_couple

        if (not doublon) and (not clone):
            to_create_barcode.append(create_record())

        if (not doublon) and clone:
            to_clone_barcode.append(clone_record())

        if doublon and (not clone):
            create_chain = create_chain_record(create_chain)

        if doublon and clone:
            clone_chain = clone_chain_record(clone_chain)

    return (to_clone_barcode, clone_chain, to_create_barcode, create_chain)


def treat_redcap_response(response, barcode_index):
    """ """

    # Strucuture:
    # (patient_id, type_barcode)
    redcap_couple = []

    redcap_barcodes = []

    # Structure:
    # {patient_id: {instruement: {record}}}
    redcap_records = {}

    for record in response:
        if record['redcap_repeat_instance'] and record['redcap_repeat_instrument']:
            patient_id = record['patient_id']
            # on regarde quel type_barcode est renseigné:
            for index in record:
                if index in barcode_index and record[index]:
                    redcap_couple.append((patient_id, index))
                    redcap_barcodes.append(record[index])
                    redcap_records.setdefault((patient_id, record['redcap_repeat_instrument']), []).append(record)

    return redcap_couple, redcap_barcodes, redcap_records


with open('config_crf.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

api_url = 'http://ib101b/html/redcap/api/'
project = Project(api_url, config['api_key'])

barcode_index = ['germline_dna_cng_barcode', 'tumor_dna_barcode', 'rna_cng_barcode']
instrument_index = ['germline_dna_sequencing', 'tumor_dna_sequencing', 'rna_sequencing']

response = project.export_records()

with open(os.path.join('data', 'CRF_mock.tsv'), 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')
    couple_count = treat_crf(dict_reader, barcode_index)

    # Les couple patient_id, type_barcode des records non vide de redcap
    pack = treat_redcap_response(response, barcode_index)
    redcap_couple, redcap_barcodes, redcap_records = pack

    pack = create_clone_chains(couple_count, redcap_couple, redcap_barcodes, redcap_records)
    to_clone_barcode, clone_chain, to_create_barcode, create_chain = pack

records_to_import = list(itertools.chain(to_clone_barcode, clone_chain,
                                    to_create_barcode, create_chain))

project.import_records(records_to_import)
