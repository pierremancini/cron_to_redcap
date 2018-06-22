#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import yaml
import redcap
import os
from redcap import RedcapError



api_url = 'https://redcap-exterieur.bordeaux.unicancer.fr/bioinfo/api/'
api_key = 'A6AA00754139284F6BEFB5D9C23BED94'
# print("yo")
project = redcap.Project(api_url, api_key)
FLAG_NEW=False

subset = project.export_records(events=["analysis_arm_1"])
# print(len(subset))
# print(subset)
patient_id = []
try:
	os.remove("logfile.log")
except OSError:
	pass
logFile = open("logfile.log","a")

for s in subset:
	# print("Patient id : ",s["patient_id"],"repeat instance : ", s["redcap_repeat_instance"], s["cel_file"], s["analysis_cinsarc_signature"])
	logFile.write("\n")
	message = "Patient id : %s, repeat instance : %s %s %s"  % (s["patient_id"], s["redcap_repeat_instance"],s["cel_file"], s["analysis_cinsarc_signature"])
	logFile.write(message)
	logFile.write("\n")

	if s["cel_file"] == "[document]":
		if not s["analysis_cinsarc_signature"] and not s["analysis_date"]:

			# print("Patient ",s["patient_id"],"  ",s["redcap_repeat_instance"]," has file for you, download it !")
			message = "Patient : %s, analysis repeat instance : %s has file for you, download it !"  % (s["patient_id"], s["redcap_repeat_instance"])
			logFile.write(message)
			logFile.write("\n")
			
			try:
				file_content, headers = project.export_file(record=s["patient_id"], field='cel_file', event='analysis_arm_1')
			except RedcapError:
				# print("Can't get the file...\n")
				message = "Can't get the file...\n"
				logFile.write(message)
				pass
			else:
				with open(headers["name"], "wb") as f:
					f.write(file_content)
				# print("file ",headers["name"]," downloaded ! \n")
				message = "file %s downloaded ! \n" % (headers["name"])
				logFile.write(message)
				FLAG_NEW=True
				patient_id.append(s["patient_id"])
				# open(headers["name"],'w').write(file_content)
		else:
			# print("Data already registred in Bioinfo analysis, this is not a new case ...\n")
			message = "Data already registred in Bioinfo analysis, this is not a new case ...\n"
			logFile.write(message)
	else:
		# print("No file available for patient ", s["patient_id"], "analysis repeat instance : ",s["redcap_repeat_instance"],"\n")
		message = "No file available for patient %s, analysis repeat instance : %s \n"  % (s["patient_id"], s["redcap_repeat_instance"])
		logFile.write(message)


logFile.close()
exit(str(patient_id))