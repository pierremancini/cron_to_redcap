#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" Transfers redcap bioinfo data to CRF FTP server """

import os
import yaml
from redcap import Project
import logging
import ftplib
import socket
import csv
import argparse
import sys
import hashlib
import time
import update_redcap_record as redcap_record

from project_logging import set_root_logger

from pprint import pprint

from contextlib import redirect_stdout
import io


def get_ftp_md5(ftp, remote_path):
    m = hashlib.md5()
    ftp.retrbinary('RETR %s' % remote_path, m.update)
    return m.hexdigest()


def md5(fpath):
    hash_md5 = hashlib.md5()
    with open(fpath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def upload_file(local_path, remote_path, connection, timeout=5, max_tries=2):
    """ Upload file on ftp server.

        :param remote_path: Head + tail file path
        :param connection: Dictonnary {host: '', login: '', password: ''}
    """

    local_head, local_fname = os.path.split(local_path)
    remote_head, remote_fname = os.path.split(remote_path)

    for count in range(max_tries):
        # Capture du stdout de storbinary pour le logger
        alt_stream = io.StringIO()
        with redirect_stdout(alt_stream):
            try:
                try:
                    with ftplib.FTP_TLS(connection['host'], timeout=timeout) as ftps:

                        ftps.set_debuglevel(1)

                        ftps.login(connection['login'], connection['password'])
                        # Encrypt all data, not only login/password
                        ftps.prot_p()
                        # Déclare l'IP comme étant de la famille v6 pour être compatible avec ftplib (même si on reste en v4)
                        # cf: stackoverflow.com/questions/35581425/python-ftps-hangs-on-directory-list-in-passive-mode
                        ftps.af = socket.AF_INET6
                        ftps.cwd(remote_head)

                        # Copie sur le remote
                        with open(os.path.join(local_path), 'rb') as file:
                            ftps.storbinary('STOR {}'.format(local_fname), file)

                # Si on a un timeout ça se passe comme prévu.
                except socket.timeout as e:
                    logger.debug(e)
                    logger.debug('stdout of storbinary :\n' + alt_stream.getvalue())
                    # On vérifié l'intégrité du fichier transféré
                    with ftplib.FTP_TLS(config['crf_host']) as ftps:
                        ftps.login(connection['login'], connection['password'])
                        ftps.prot_p()
                        ftps.af = socket.AF_INET6
                        ftp_md5 = get_ftp_md5(ftps, remote_path)

                    if ftp_md5 == md5(local_path):
                        logger.info('md5 ok')
                        return True
                    else:
                        logger.warning('{} Wrong md5.'.format(local_path))
                        logger.debug('FTP upload: Attemp n°{} , failed to upload {}'.format(count + 1, local_fname))

            # FileNotFoundError
            except FileNotFoundError as e:
                # On log l'erreur pour le débug sans bloquer
                logger.debug(e)
                raise
            except ftplib.all_errors as e:
                logger.error(e)
                logger.debug('FTP upload: Attemp n°{} , failed to upload {}'.format(count + 1, local_fname))

    return False


def handle_exception(exc_type, exc_value, exc_traceback):

    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def args():
    """Parse options."""
    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('-c', '--config', default="config.yml", help='config file.')
    opt_parser.add_argument('-s', '--secret', default="secret_config.yml", help='secret config file.')
    opt_parser.add_argument('-l', '--log', default="logging.yml", help='logging configuration file.')
    return opt_parser.parse_args()


if __name__ == '__main__':

    args = args()

    # On log les uncaught exceptions
    sys.excepthook = handle_exception

    with open(args.config, 'r') as ymlfile:
        config = yaml.load(ymlfile)
    with open(args.secret, 'r') as ymlfile:
        secret_config = yaml.load(ymlfile)
    config.update(secret_config)

    logger = set_root_logger(config['path_to_log'], os.path.basename(__file__))

    # Génération du fichier d'exportation vers CRF
    project = Project(config['redcap_api_url'], config['api_key'])
    response = project.export_records(forms='bioinformatic_analysis', raw_or_label='label')

    # Strucure:
    # {field_label: {instrument: field_name}}
    redcap_fields = {}

    bio_analysis = {}

    # Utilise les metadata pour retrouver les champs correspondant au formulaire bioinformatic_analysis
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

    local_path = config['local_bioanalysis_file']
    remote_bioanalysis_file = config['remote_bioanalysis_file']

    # Traitement de la réponse redcap
    # Ecriture du fichier d'import en local
    id_list = []
    with open(local_path, 'w') as tsvfile:
        csvwriter = csv.writer(tsvfile, delimiter=',', quoting=csv.QUOTE_NONE)
        csvwriter.writerow(header)
        for record in response:
            if not record['redcap_repeat_instrument'] and not record['redcap_repeat_instance']:
                if record['patient_id'] not in to_exclude and not record['sent_to_ennov_at']:
                    # En cas de valeur nulle les dates doivent avoir un espaces pour que l'importation
                    # dans le eCRF marche. (voir mail Delphine)
                    if not record['date_of_availability']:
                        record['date_of_availability'] = ' '
                    if not record['date_receipt_files']:
                        record['date_receipt_files'] = ' '

                    id_list.append(record['patient_id'])
                    row = [record['patient_id'],
                    record['date_receipt_files'],
                    record['quality_control'],
                    record['availab_genvarxplorer'],
                    record['date_of_availability']]
                    csvwriter.writerow(row)

    connection = {'host': config['crf_host'], 'login': config['login_crf'], 'password': config['password_crf']}
    # upload du fichier d'import
    # Attention, le fichier précédent sera écrasé
    # Vérifie que le transfert à bien eu lieu avec un code retour qui dépend
    # de la vérification md5 de la fonction
    if upload_file(local_path, remote_bioanalysis_file, connection):
        # Set le champ sent_to_ennov_at des records du fichier envoyé
        for patient_id in id_list:
            redcap_record.update(config['redcap_api_url'], config['api_key'], patient_id, 'sent_to_ennov_at',
                time.strftime('%Y-%m-%d'), 'bioinformatic_analysis')
