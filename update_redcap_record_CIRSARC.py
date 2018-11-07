#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import ast
import argparse
import yaml
import redcap
import os
from redcap import RedcapError
# from redcap import Project

opt_parser = argparse.ArgumentParser(description=__doc__, prog='update_redcap.py')

opt_parser.add_argument('-y', '--secret', default="secret_config.yml", help='secret config file.')
opt_parser.add_argument('-d', '--analysis-date', required=True, help='Analysis_date.')
opt_parser.add_argument('-c', '--cinsarc-status', required=True, help='CINSARC Status.')
opt_parser.add_argument('-s', '--sample-name', required=True, help='Sample_Name.')

args = opt_parser.parse_args()


with open(args.secret, 'r') as ymlfile:
        secret_config = yaml.load(ymlfile)

analysis_date = args.analysis_date
print(analysis_date)
cinsarc_status = args.cinsarc_status
print(cinsarc_status)
sample_name = args.sample_name
print(sample_name)



api_url = secret_config['api_url_cirsarc']
api_key = secret_config['api_key_cirsarc']

project = redcap.Project(api_url, api_key)
print("yo")
print(project)


subset = project.export_records(events=["analysis_arm_1"])
for s in subset:
	if s["sample_platform_number"] == sample_name:
		print(s)
		patient_id = s["patient_id"]

		if not s["analysis_cinsarc_signature"] and not s["analysis_date"]:
			to_import = [{'patient_id':patient_id,'redcap_event_name':'analysis_arm_1','redcap_repeat_instance':'1',
						'analysis_cinsarc_signature':cinsarc_status,'analysis_date':analysis_date}]
			response = project.import_records(to_import)
			print(response)
			print("patient : ",s["patient_id"]," UPDATED")
		else:
			print("can't update patient :",s["patient_id"]," analysis repeat instance : ",s["redcap_repeat_instance"]," some data already here...")



