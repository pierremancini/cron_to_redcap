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


opt_parser.add_argument('-id', '--patient-id', required=True, help='Patient_id.')
opt_parser.add_argument('-d', '--analysis-date', required=True, help='Analysis_date.')
opt_parser.add_argument('-c', '--cinsarc-status', required=True, help='CINSARC Status.')

args = opt_parser.parse_args()

try:
	os.remove("Update_redcap.log")
except OSError:
	pass
logFile = open("Update_redcap.log","a")

patient_id = args.patient_id
# patient_id = patient_id[1:-1]
message = "patient id list : %s\n" % (patient_id)
logFile.write(message)
analysis_date = args.analysis_date
# print(analysis_date)
message = "analysis date : %s\n" % (analysis_date)
logFile.write(message)
cinsarc_status = args.cinsarc_status
# print(cinsarc_status)

# patient_id = ast.literal_eval(patient_id)
message = "patient id type : %s\n" % (type(patient_id))
logFile.write(message)
logFile.close()

api_url = 'https://redcap-exterieur.bordeaux.unicancer.fr/bioinfo/api/'
# api_url = 'https://129.10.20.249/bioinfo/api/'
api_key = 'A6AA00754139284F6BEFB5D9C23BED94'

project = redcap.Project(api_url, api_key)
print("yo")
print(project)


s1 = project.export_records(records=patient_id,events=["analysis_arm_1"], fields=["cinsarc_signature","analysis_date"])
s1=s1[0]
if not s1["cinsarc_signature"] and not s1["analysis_date"]:
	to_import = [{'patient_id':patient_id,'redcap_event_name':'analysis_arm_1','redcap_repeat_instance':'1',
				'cinsarc_signature':cinsarc_status,'analysis_date':analysis_date}]
	response = project.import_records(to_import)
	print(response)
else:
	print("can't update patient :",s1["patient_id"]," analysis repeat instance : ",s1["redcap_repeat_instance"]," some data already here...")


# for p in patient_id:
# 	print(p)
# 	s1 = project.export_records(records=p,events=["analysis_arm_1"], fields=["cinsarc_signature","analysis_date"])
# 	s1=s1[0]
# 	if not s1["cinsarc_signature"] and not s1["analysis_date"]:
# 		to_import = [{'patient_id':p,'redcap_event_name':'analysis_arm_1','redcap_repeat_instance':'1',
# 					'cinsarc_signature':cinsarc_status,'analysis_date':analysis_date}]
# 		response = project.import_records(to_import)
# 		print(response)
# 	else:
# 		print("can't update patient :",s1["patient_id"]," analysis repeat instance : ",s1["redcap_repeat_instance"]," some data already here...")
# ids_of_interest = ['1','2']
#subset = project.export_records(records=ids_of_interest)
# subset = project.export_records(records=ids_of_interest, events=["analysis_arm_1"])
#subset = project.export_records(events=["analysis_arm_1"])
# subset = project.export_records(records="2")
# print(len(subset))
# print(subset)
# subset = project.export_records(records="3", events=["analysis_arm_1"])
# print(len(subset))
# print(subset)

# Only want the first two forms
# forms = project.forms[:1]
# subset = project.export_records(forms=forms)
# Known fields of interest
# fields_of_interest = ['age', 'test_score1', 'test_score2']
# fields_of_interest = 'cinsarc_signature'
# subset = project.export_records(fields=fields_of_interest)
