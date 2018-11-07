#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import yaml
import redcap
import os
from redcap import RedcapError
# from redcap import Project

opt_parser = argparse.ArgumentParser(description=__doc__, prog='update_redcap.py')


opt_parser.add_argument('-d', '--analysis-date', required=True, help='Analysis_date.')
opt_parser.add_argument('-c', '--cinsarc-status', required=True, help='CINSARC Status.')
opt_parser.add_argument('-s', '--sample-name', required=True, help='Sample_Name.')

args = opt_parser.parse_args()

try:
	os.remove("Update_redcap.log")
except OSError:
	pass
logFile = open("Update_redcap.log","a")

analysis_date = args.analysis_date
# print(analysis_date)
message = "analysis date : %s\n" % (analysis_date)
logFile.write(message)
cinsarc_status = args.cinsarc_status
# print(cinsarc_status)
print(cinsarc_status)

sample_name = args.sample_name
print(sample_name)


logFile.write(message)

api_url = 'https://redcap-exterieur.bordeaux.unicancer.fr/bioinfo/api/'
# api_url = 'https://129.10.20.249/bioinfo/api/'
api_key = 'A6AA00754139284F6BEFB5D9C23BED94'

project = redcap.Project(api_url, api_key)

subset = project.export_records(events=["analysis_arm_1"])
# print(s)
for s in subset:
	if s["sample_platform_number"] == sample_name:
		print(s)
		patient_id = s["patient_id"]
		print(patient_id)
		# s1 = project.export_records(records=patient_id,events=["analysis_arm_1"], fields=["analysis_cinsarc_signature","analysis_date"])
		if not s["analysis_cinsarc_signature"] and not s["analysis_date"]:
				to_import = [{'patient_id':patient_id,'redcap_event_name':'analysis_arm_1','redcap_repeat_instance':'1',
				'analysis_cinsarc_signature':cinsarc_status,'analysis_date':analysis_date}]
				response = project.import_records(to_import)
				print(response)
	# logFile.write(response)
				logFile.write("UPDATED\n")
				print("Patient : ",s["patient_id"]," UPDATED")

	else:	
		print("can't update patient :",s["patient_id"]," analysis repeat instance : ",s["redcap_repeat_instance"]," some data already here...")
		# message = "can't update patient :%s",s1["patient_id"]," analysis repeat instance : %s",s1["redcap_repeat_instance"]," some data already here..."
		logFile.write("CANT UPDATE\n")


# print(s1)
# s1=s1[0]
# print(s1)



logFile.close()

