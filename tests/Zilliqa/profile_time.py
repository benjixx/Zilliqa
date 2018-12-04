#!/usr/bin/env python
# Copyright (c) 2018 Zilliqa
# This source code is being disclosed to you solely for the purpose of your
# participation in testing Zilliqa. You may view, compile and run the code for
# that purpose and pursuant to the protocols and algorithms that are programmed
# into, and intended by, the code. You may not do anything else with the code
# without express permission from Zilliqa Research Pte. Ltd., including
# modifying or publishing the code (or any part of it), and developing or
# forming another public or private blockchain network. This source code is
# provided 'as is' and no warranties are given as to title or non-infringement,
# merchantability or fitness for purpose and, to the extent permitted by law,
# all liability for your use of the code is disclaimed. Some programs in this
# code are governed by the GNU General Public License v3.0 (available at
# https://www.gnu.org/licenses/gpl-3.0.en.html) ('GPLv3'). The programs that
# are governed by GPLv3.0 are those programs that are located in the folders
# src/depends and tests/depends and which include a reference to GPLv3 in their
# program files.

import os
import sys
import time
import fnmatch
import re

try:
	from sortedcontainers import SortedDict
except ImportError:
	sys.exit("""You need sortedcontainers!
              install it by pip install sortedcontainers""")

import xml.etree.cElementTree as ET

NODE_LISTEN_PORT = 5001
LOCAL_RUN_FOLDER = './local_run/'

STATE_LOG_FILE = 'state-00001-log.txt'
KEYWORD_DSCON = '[DSCON]'
KEYWORD_BEGIN = 'BGIN'
KEYWORD_DONE = 'DONE'

DSConsensusStartTime = SortedDict()
DSConsensusEndTime = SortedDict()
MBConsensusTime = SortedDict()
FBConsensusTime = SortedDict()

def print_usage():
	print ("Profile consensus and communication time from state logs\n"
		"=============================================================\n"
	"Usage:\n\tpython " + sys.argv[0] + " [Log Parent Path] [Output File Path]\n")

def find_files(directory, pattern):
	for root, dirs, files in os.walk(directory):
		for basename in files:
			if fnmatch.fnmatch(basename, pattern):
				filename = os.path.join(root, basename)
				yield filename

def get_time(line):
	m = re.search(r'[\d]+:[\d]+:[\d]+:[ ]*[\d]+', line)
	if m != None:
		return m.group(0)
	else:
		return ""

def get_block_number(line):
	m = re.search(r'[[\d]+]', line)
	blockNumber = m.group(0)
	blockNumber = blockNumber[1:len(blockNumber)-1]
	return int(blockNumber)

def scan_file(fileName):
	file = open(fileName, "r+")
	for line in file:
		if line.find(KEYWORD_DSCON) != -1:
			blockNumber = get_block_number(line)
			if line.find(KEYWORD_BEGIN) != -1:
				DSConsensusStartTime[blockNumber] = get_time(line)
			elif line.find(KEYWORD_DONE) != -1:
				DSConsensusEndTime[blockNumber] = get_time(line)

def convert_time_string(strTime):
	a,b,c,d = strTime.split(':')
	return int(a)*3600000 + int(b) * 60000 + int(c) * 1000 + int(d)

def printResult():	
	DSConsensusStartTimeKeys = DSConsensusStartTime.keys()
	DSConsensusStartTimeValues = DSConsensusStartTime.values()
	DSConsensusEndTimeValues = DSConsensusEndTime.values()
	totalDSBockNumber = min(len(DSConsensusStartTimeValues), len(DSConsensusEndTimeValues))
	index = 0
	while index < totalDSBockNumber:
		timeSpan = convert_time_string(DSConsensusEndTimeValues[index]) - convert_time_string(DSConsensusStartTimeValues[index])
		print("DS Block\t" + str(DSConsensusStartTimeKeys[index]) + "\t" + DSConsensusStartTimeValues[index] + "\t" + DSConsensusEndTimeValues[index] + "\t" + str(timeSpan))
		index += 1

def main():
	numargs = len(sys.argv)
	if (numargs < 3):
		print_usage()
	else:
		stateLogPath = sys.argv[1]
		if os.path.exists(stateLogPath) != True:
			print ("Path " + stateLogPath + " not exist!")
			print_usage()
			return

		outputFileName = sys.argv[2]
		outputFile = open(outputFileName, "w+")
		if outputFile.closed:
			print ("Failed to open file " + outputFileName)
			print_usage()
			return

		#os.chdir(stateLogPath)
		#for file in glob.glob(".txt"):
		#	print(file)
		#for file in os.listdir(stateLogPath):
		#	print(file)
		#	if file == STATE_LOG_FILE:
		#		print(os.path.join(stateLogPath, file))
		#for root, dirnames, filenames in os.walk(stateLogPath):
		#	for filename in fnmatch.filter(filenames, STATE_LOG_FILE):
		#		print(filename)
		fileNames = find_files(stateLogPath, STATE_LOG_FILE)
		for fileName in fileNames:
			print("Checking file: " + fileName)
			scan_file(fileName)

		printResult()

if __name__ == "__main__":
	main()
