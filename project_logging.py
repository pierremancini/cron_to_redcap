#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import logging.config

import inspect


def set_root_logger(path_to_log, config_dict):
    """
        1. Check et création des dossier de log.

        Le nom du fichier _.log est défini en fonction du nom de fichier appelant la fonction.

        2. Instancie un logger "root".
    """

    # Le nom du dossier dans lequel set_logger est défini <=> nom du projet
    project_folder = os.path.relpath(inspect.stack()[0].filename, '..').split('/')[0]

    # Le nom du fichier qui à appeler la fonction set_logger de ce module
    name = os.path.splitext(inspect.stack()[1].filename)[0]

    path = '{}/{}/{}/{}'.format(path_to_log, project_folder, name, name + '.log')
    config_dict['handlers']['file_handler']['filename'] = path

    try:
        logging.config.dictConfig(config_dict)
    except ValueError:
        if not os.path.exists('{}/{}/{}'.format(path_to_log, project_folder, name)):
            os.makedirs('{}/{}/{}'.format(path_to_log, project_folder, name))
        # Il faut créé les dossiers de log
        logging.config.dictConfig(config_dict)

    logger = logging.getLogger()

    return logger
