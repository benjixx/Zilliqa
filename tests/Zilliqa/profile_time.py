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
	sys.exit("""You need sortedcontainers! Install it by run 'sudo python -m pip install sortedcontainers'""")

STATE_LOG_FILE = 'state-00001-log.txt'
KEYWORD_DSCON = '[DSCON]'
KEYWORD_MBCON = '[MICON]'
KEYWORD_FBCON = '[FBCON]'
KEYWORD_BEGIN = 'BGIN'
KEYWORD_DONE = 'DONE'

class Consensus:
	blockNumber = 0
	index = 0
	name = ''
	startTime = ''
	endTime = ''
	timeSpan = 0

MBConsensusDict = SortedDict()
FBConsensusDict = SortedDict()

DSConsensusStartTime = SortedDict()
DSConsensusEndTime = SortedDict()

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
	mbConsensusStartTime = ''
	fbConsensusStartTime = ''
	for line in file:
		if line.find(KEYWORD_DSCON) != -1:
			blockNumber = get_block_number(line)
			if line.find(KEYWORD_BEGIN) != -1:
				DSConsensusStartTime[blockNumber] = get_time(line)				
			elif line.find(KEYWORD_DONE) != -1:
				DSConsensusEndTime[blockNumber] = get_time(line)
		elif line.find(KEYWORD_MBCON) != -1:
			blockNumber = get_block_number(line)
			if line.find(KEYWORD_BEGIN) != -1:
				mbConsensusStartTime = get_time(line)
			elif line.find(KEYWORD_DONE) != -1:
				mbConsensus = Consensus()
				mbConsensus.blockNumber = blockNumber
				mbConsensus.startTime = mbConsensusStartTime
				mbConsensus.endTime = get_time(line)
				MBConsensusDict.setdefault(blockNumber, []).append(mbConsensus)
		elif line.find(KEYWORD_FBCON) != -1:
			blockNumber = get_block_number(line)
			if line.find(KEYWORD_BEGIN) != -1:
				fbConsensusStartTime = get_time(line)
			elif line.find(KEYWORD_DONE) != -1:
				fbConsensus = Consensus()
				fbConsensus.blockNumber = blockNumber
				fbConsensus.startTime = fbConsensusStartTime
				fbConsensus.endTime = get_time(line)
				FBConsensusDict.setdefault(blockNumber, []).append(fbConsensus)
	file.close()

def convert_time_string(strTime):
	a,b,c,d = strTime.split(':')
	return int(a)*3600000 + int(b) * 60000 + int(c) * 1000 + int(d)

def printResult(outputFile):	
	DSConsensusStartTimeKeys = DSConsensusStartTime.keys()
	DSConsensusStartTimeValues = DSConsensusStartTime.values()
	DSConsensusEndTimeValues = DSConsensusEndTime.values()
	print("Length of DSConsensusStartTimeKeys " + str(len(DSConsensusStartTimeKeys)))

	MBConsensusDictKeys = MBConsensusDict.keys()
	MBConsensusDictValues = MBConsensusDict.values()
	print("Length of MBConsensusDictKeys " + str(len(MBConsensusDictKeys)))

	FBConsensusDictKeys = FBConsensusDict.keys()
	FBConsensusDictValues = FBConsensusDict.values()
	print("Length of FBConsensusDictKeys " + str(len(FBConsensusDictKeys)))

	totalFBBlockNumber = len(FBConsensusDictKeys)
	totalDSBlockNumber = len(DSConsensusEndTimeValues)
	dsIndex = 0
	fbIndex = 0
	mbIndex = 0
	while fbIndex < totalFBBlockNumber:
		if DSConsensusStartTimeKeys[dsIndex] == FBConsensusDictKeys[fbIndex]:
			dsTimeSpan = convert_time_string(DSConsensusEndTimeValues[dsIndex]) - convert_time_string(DSConsensusStartTimeValues[dsIndex])
			outputFile.write("DS Block\t" + str(DSConsensusStartTimeKeys[dsIndex]) + "\t" + DSConsensusStartTimeValues[dsIndex] + "\t" + DSConsensusEndTimeValues[dsIndex] + "\t" + str(dsTimeSpan) + "\n")

		if mbIndex < len(MBConsensusDictKeys):
			if (MBConsensusDictKeys[mbIndex] == FBConsensusDictKeys[fbIndex]):
				for mbConsensus in MBConsensusDictValues[mbIndex]:
					mbTimeSpan = convert_time_string(mbConsensus.endTime) - convert_time_string(mbConsensus.startTime)
					outputFile.write("MB Block\t" + str(mbConsensus.blockNumber) + "\t" + mbConsensus.startTime + "\t" + mbConsensus.endTime + "\t" + str(mbTimeSpan) + "\n")
				mbIndex += 1
			else:
				print("Warning: no complete micro block consensus found for block " + str(FBConsensusDictKeys[fbIndex]))

		for fbConsensus in FBConsensusDictValues[fbIndex]:
			fbTimeSpan = convert_time_string(fbConsensus.endTime) - convert_time_string(fbConsensus.startTime)
			outputFile.write("FB Block\t" + str(fbConsensus.blockNumber) + "\t" + fbConsensus.startTime + "\t" + fbConsensus.endTime + "\t" + str(fbTimeSpan) + "\n")

		fbIndex += 1

		if fbIndex >= totalFBBlockNumber:
			break

		if (dsIndex < len(DSConsensusStartTimeKeys) - 1 and DSConsensusStartTimeKeys[dsIndex + 1] == FBConsensusDictKeys[fbIndex]):
			dsIndex += 1

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

		fileNames = find_files(stateLogPath, STATE_LOG_FILE)
		for fileName in fileNames:
			print("Checking file: " + fileName)
			scan_file(fileName)

		printResult(outputFile)

		outputFile.close()

if __name__ == "__main__":
	main()
