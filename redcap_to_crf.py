#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" Transfers redcap bioinfo data to CRF FTP server """

import os
import yaml
from redcap import Project
import logging
import logging.config
import ftplib
import socket
import csv
import argparse
import sys
import hashlib

from pprint import pprint


def get_ftp_md5(ftp, remote_path):
    m = hashlib.md5()
    ftp.retrbinary('RETR %s' % remote_path, m.update)
    return m.hexdigest()


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def upload_file(local_path, remote_path, connection, timeout=6, max_tries=1):

    """ Upload file on ftp server.

        :param remote_path: Head + tail file path
        :param server: Dictonnary {host: '', login: '', password: ''}
    """

    local_head, local_fname = os.path.split(local_path)
    remote_head, remote_fname = os.path.split(remote_path)

    for count in range(max_tries):
        try:
            try:
                with ftplib.FTP_TLS(connection['host'], timeout=timeout) as ftps:

                    ftps.set_debuglevel(2)

                    ftps.login(connection['login'], connection['password'])
                    # Encrypt all data, not only login/password
                    ftps.prot_p()
                    # Déclare l'IP comme étant de la famille v6 pour être compatible avec ftplib (même si on reste en v4)
                    # cf: stackoverflow.com/questions/35581425/python-ftps-hangs-on-directory-list-in-passive-mode
                    ftps.af = socket.AF_INET6
                    ftps.cwd(remote_head)
                    with open(os.path.join(local_path), 'rb') as file:
                        ftps.storbinary('STOR {}'.format(local_fname), file)

            # Si on a un timeout ça se passe comme prévu.
            except socket.timeout as e:
                print(e)

                # On vérifié l'intégrité du fichier transféré
                with ftplib.FTP_TLS(config['crf_host']) as ftps:
                    ftps.login(connection['login'], connection['password'])
                    ftps.prot_p()
                    ftps.af = socket.AF_INET6
                    ftp_md5 = get_ftp_md5(ftps, remote_path)

                if ftp_md5 == md5(local_fname):
                    print('md5 ok')
                    return True
                else:
                    print('Wrong md5.')
                    print('FTP upload: Attemp n°{} , failed to upload {}'.format(count + 1, local_fname))

        except ftplib.all_errors as e:
            print(e)
            print('FTP upload: Attemp n°{} , failed to upload {}'.format(count + 1, local_fname))

    return False


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

# Génération du fichier d'exportation vers CRF
project = Project(config['redcap_api_url'], config['api_key'])
response = project.export_records(forms='bioinformatic_analysis')

# Strucure:
# {field_label: {instrument: field_name}}
redcap_fields = {}

bio_analysis = {}

# Utilise les metadata pour retrouver les champs correspondant au formulaire bioinformatic_analysis
# ICI à changer
for metadict in project.metadata:
    if metadict['form_name'] == 'bioinformatic_analysis':
        bio_analysis.setdefault(metadict['field_label'], metadict['field_name'])

    redcap_fields.setdefault(metadict['field_label'], {}).setdefault(metadict['form_name'], metadict['field_name'])


# On veux les champs bioinformatic_analysis pour créer le header du fichier d'export
# Puis on remplis avec les valeurs elle même
header = ['Patient ID',
    'Date if receipt of all needed files',
    'Quality control',
    'Availability in genVarXplorer for interpretation',
    'If yes data of availability'] # ! Utiliser 'If yes, data of availability' avec ',' pour le label


# Records utilisés pour tester redcap
to_exclude = ['DEV1', 'DEV2', 'DEV3', 'SARC2', 'SARC3']

extraction_filename = 'bioanalysis_import.txt'
local_path = os.path.join('data', 'crf_extraction', extraction_filename)
path_crf_file = os.path.join('MULTIPLI', extraction_filename)

# Traitement de la réponse redcap

# On enregistre le fichier en local aussi pour avoir un backup
with open(local_path, 'w') as tsvfile:
    csvwriter = csv.writer(tsvfile, delimiter='\t')
    csvwriter.writerow(header)
    for record in response[1:]:
        if not record['redcap_repeat_instrument'] and not record['redcap_repeat_instance']:
            if record['patient_id'] not in to_exclude:
                row = [record['patient_id'],
                record['date_receipt_files'],
                record['quality_control'],
                record['availab_genvarxplorer'],
                record['data_of_availability']]
                csvwriter.writerow(row)

connection = {'host': config['crf_host'], 'login': config['login_crf'], 'password': config['password_crf']}
# Attention, le fichier précédent sera écrasé
upload_file(local_path, path_crf_file, connection)
