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


path_crf_file = config['path_bioanalysis']
head_crf, tail_crf = os.path.split(path_crf_file)

try:
    with ftplib.FTP_TLS(config['crf_host'], timeout=1) as ftps:
    
        ftps.set_debuglevel(2)
    
        ftps.login(config['login_crf'], config['password_crf'])
        # Encrypt all data, not only login/password
        ftps.prot_p()
        # Déclare l'IP comme étant de la famille v6 pour être compatible avec ftplib (même si on reste en v4)
        # cf: stackoverflow.com/questions/35581425/python-ftps-hangs-on-directory-list-in-passive-mode
        ftps.af = socket.AF_INET6
        ftps.cwd(head_crf)
    
        ftps.retrlines('LIST')
    
        with open('data/test.txt', 'rb') as file:
            ftps.storbinary('STOR {}'.format('test.txt'), file)

except socket.timeout:
    with ftplib.FTP_TLS(config['crf_host']) as ftps:
        ftps.login(config['login_crf'], config['password_crf'])
        # Encrypt all data, not only login/password
        ftps.prot_p()
        # Déclare l'IP comme étant de la famille v6 pour être compatible avec ftplib (même si on reste en v4)
        # cf: stackoverflow.com/que
        ftps.af = socket.AF_INET6
        ftp_md5 = get_ftp_md5(ftps, os.path.join(head_crf, 'test.txt'))
        print(ftp_md5)

print(md5('data/test.txt'))