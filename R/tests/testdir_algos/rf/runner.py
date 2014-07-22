#!/usr/bin/python

import os, subprocess

genTestDirectory = 'generated/'
count = 0
for file in os.listdir(genTestDirectory):
	if file.endswith(".R") and count < 5:
		logFile = "log."+ file
		cmd = "R -f {0}{1} > log/{2}".format(genTestDirectory, file, logFile)
		print cmd
		subprocess.call(cmd, shell=True, executable="/usr/local/bin/zsh")
		count += 1