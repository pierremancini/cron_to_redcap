#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import requests
import os
import sys
import bs4 as BeautifulSoup
import requests
import yaml
import re
import csv
import argparse

from pprint import pprint

""" Get samples plan from eCRF export file and CNG webpage without using RedCAP instance.

    Input file format:

    The file is a tsv.

    The header have to be:

    patient_id  SQBCTUM SQBCGER SQBCRNA info    set


    Using example:

    python3 no_redcap_samples_plan.py --input-file data/example_input_no_redcap.tsv
     -p data/samples_plan_no_redcap.tsv


    The script uses the configurations files: config.yml and secret_config.yml.

"""


def args():
    """ Parse options."""

    opt_parser = argparse.ArgumentParser(description=__doc__)
    opt_parser.add_argument('--input-file', required=False, help='Path to input file')
    opt_parser.add_argument('-p', '--path', required=False, help='Path for output samples_plan')
    opt_parser.add_argument('-c', '--config', default="config.yml", help='config file.')
    opt_parser.add_argument('-s', '--secret', default="secret_config.yml", help='secret config file.')

    return opt_parser.parse_args()

args = args()

with open(args.config, 'r') as ymlfile:
    config = yaml.load(ymlfile)
with open(args.secret, 'r') as ymlfile:
    secret_config = yaml.load(ymlfile)
config.update(secret_config)


# 1) Read, parse input file

# Structure:
# {barcode: [{patient_id, barcode_type, set, info}]}
file_data = {}

type_mapping = {'SQBCGER': 'CD',
                'SQBCRNA': 'MR',
                'SQBCTUM': 'MD'}

# List of aimed sets on CNG
sets = set()

if args.path:
    input_file_path = args.input_file
else:
    # Default path
    input_file_path = os.path.join(config['path_to_data'], 'example_input_no_redcap.tsv')

with open(input_file_path, 'r') as csvfile:
    dict_reader = csv.DictReader(csvfile, delimiter='\t')
    for line in dict_reader:
        patient_id = line['patient_id']
        sets.add(line['set'])
        for index in line:
            if index in type_mapping.keys() and line[index]:
                file_data.setdefault(line[index], []).append({'patient_id': patient_id,
                                                             'type': index,
                                                             'set': line['set'],
                                                             'info': line['info']})

# 2) Scrap CNG's web page
def get_md5(fastq_path, mock=False):
    """ Get md5 value with path to fastq file name.

        :param mock: Tell if the data taken from md5_by_path gobal variable.
    """
    if mock:
        return md5_by_path[fastq_path]
    else:

        md5_path = fastq_path + '.md5'

        response = requests.get(md5_path, auth=(config['login_cng'], config['password_cng'])).content
        md5 = response.decode().split(' ')[0]

        return md5

def info_from_set(set_to_complete):
    """ Get and transform data from url's set on CNG."""

    # Strucure
    # {barcode:
    #    [{project, kit_code, barcode, lane, read, end_of_file, flowcell, tag}},
    #        ... ]
    dicts_fastq_info = {}

    for set in set_to_complete:
        set_url = config['url_cng'] + set
        page = requests.get(set_url, auth=(config['login_cng'], config['password_cng']))
        soup = BeautifulSoup.BeautifulSoup(page.content, 'lxml')

        fastq_gen = (file.string for file in soup.find_all('a') if re.search(r'fastq\.((gz)|(bz)|(zip)|(bz2)|(tgz)|(tbz2))$',
        file.string))
        for fastq in fastq_gen:
            project, kit_code, barcode, lane, read, end_of_file = fastq.split('_')
            flowcell, tag = end_of_file.split('.')[:-2]
            # Passe la variable mock pour lire les md5 depuis le dump json et pas le CNG
            # pour avoir un debug/developpement plus rapide
            md5 = get_md5(set_url + '/' + fastq)
            local_filename = '{}_{}_{}_{}.{}_{}_{}.fastq.gz'.format(
                project, kit_code, barcode, flowcell, tag, lane, read)
            dict_fastq_info = {'Set': set,
                               'FastQ filename CNG': fastq,
                               'FastQ filename Local': local_filename,
                               'Path on cng': set_url,
                               'md5 value': md5,
                               'Project': project,
                               'Kit code': kit_code,
                               'Barcode': barcode,
                               'Lane': lane,
                               'Read': read,
                               'Flowcell': flowcell,
                               'Tag': tag}
            dicts_fastq_info.setdefault(barcode, []).append(dict_fastq_info)

    return dicts_fastq_info

# dicts_fastq_info = info_from_set(sets)

dicts_fastq_info = {'B00JC7M': [{'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_1_1_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_1_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '1',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': 'd7264057fa022be9fb48158ad48b87d5'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_1_2_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_1_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '1',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': '3b8c25c26bdec8d498609b221f692260'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_2_1_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_2_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '2',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': 'f9911b9477e24923327260cf3e5851ba'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_2_2_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_2_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '2',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': '0ab875ef3fab908202a62983c2a8d33d'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_3_1_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_3_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '3',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': '660c59da640bf8697ff1b591abc4f70c'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_3_2_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_3_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '3',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': 'c29a77613399f965c0be106b2fce3f6a'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_4_1_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_4_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '4',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': 'ae7f2ae9c161c6f5dd34d716b508d221'},
             {'Barcode': 'B00JC7M',
              'FastQ filename CNG': 'M589_CAC_B00JC7M_4_2_HN2TGBGX3.IND150.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7M_HN2TGBGX3.IND150_4_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '4',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND150',
              'md5 value': 'c7e663784c0ee6192734811188174a14'}],
 'B00JC7O': [{'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_1_1_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_1_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '1',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': '7e89602fe549dc3a1ce0200b1b0a8352'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_1_2_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_1_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '1',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': 'd24ccf63576a7a98d2716db3529dcf78'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_2_1_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_2_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '2',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': '0c0bdcce6378b0359443414b44adae13'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_2_2_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_2_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '2',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': 'df5e6e9b0483764ec948e823d2eaa76c'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_3_1_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_3_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '3',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': '8d2800e94366b574c8b40bcb6029c733'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_3_2_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_3_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '3',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': '2d0a4b9819b23291e013400d933ba12e'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_4_1_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_4_1.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '4',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '1',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': 'e4851dca87111cd39528ae5c18ba7263'},
             {'Barcode': 'B00JC7O',
              'FastQ filename CNG': 'M589_CAC_B00JC7O_4_2_HN2TGBGX3.IND153.fastq.gz',
              'FastQ filename Local': 'M589_CAC_B00JC7O_HN2TGBGX3.IND153_4_2.fastq.gz',
              'Flowcell': 'HN2TGBGX3',
              'Kit code': 'CAC',
              'Lane': '4',
              'Path on cng': 'https://www.cng.fr/data/MULTIPLI/fastq/set20',
              'Project': 'M589',
              'Read': '2',
              'Set': 'set20',
              'Tag': 'IND153',
              'md5 value': '2272704c82105df298cb716514a6c7c7'}]}

# 3) Generate sample plan
rows_tsv = []

for barcode in file_data:

    for list_item in file_data[barcode]:

        analysis_type = type_mapping[list_item['type']]
        patient_id = list_item['patient_id']
        for cng_item in dicts_fastq_info[barcode]:

            set = cng_item['Set']
            path_on_cng = config['url_cng'] + set
            fastQ_file_cng = cng_item['FastQ filename CNG']
            fastQ_file_local = cng_item['FastQ filename Local']
            info = list_item['info']

            case = '{}-{}{}-{}'.format(patient_id, set, info, analysis_type)
            row = [case, path_on_cng, fastQ_file_cng, fastQ_file_local]
            rows_tsv.append(row)
            fastQ_file_cng_md5 = fastQ_file_cng + '.md5'
            fastQ_file_local_md5 = fastQ_file_local + '.md5'
            md5_row = [case, path_on_cng, fastQ_file_cng_md5, fastQ_file_local_md5]
            rows_tsv.append(md5_row)


if args.path:
    samples_plan_path = args.path
else:
    # Default path
    samples_plan_path = os.path.join('data', 'samples_plan_no_redcap.tsv')

with open(samples_plan_path, 'w') as csvfile:
    header = ['CASE', 'URL', 'REMOTEFILE', 'LOCALFILE']
    writer = csv.writer(csvfile, delimiter='\t')
    writer.writerow(header)
    for row in rows_tsv:
        writer.writerow(row)
