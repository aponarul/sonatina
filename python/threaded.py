import os
import sys
import shutil
import zipfile
import pdb
import time
import multiprocessing
from multiprocessing import Process, Lock, Pool
import glob
import functools


#prodConfigs
input_dir="/ubidata/JourneyData/sys.argv[2]"
Temp_folder="/d2/data/apps/ubi/ingestion/octo/temp"
journey_extension_folder="/d2/data/apps/ubi/ingestion/octo/journeyext"
journey_folder="/d2/data/apps/ubi/ingestion/octo/journey"
badFiles="/d2/data/apps/ubi/ingestion/octo/badFiles"

journeyLock = multiprocessing.Lock()
journeyExtLock = multiprocessing.Lock()
index = sys.argv[1].split("/")[-1].split(".")[0].split("_")[1]
journeyFile = "Journey_%d.csv" % int(index)



def process_filename(journeyfilename):
	'''AAA_NA_1000_2016-03-11-21-58-53_354235058581665_3FADP4AJ5FM115166_0_0.zip'''
	arr = journeyfilename.strip().split(".")
	components = arr[0].split("_")
	metadata = {'project_id': components[0],
				'tsp_sequential_id': components[1],
				'device_sequential_id': components[2],
				'start_date': components[3],
				'imei': components[4],
				'vin': components[5],
				'device_type': components[6],
				'vin_type': components[7],
				'journey_date': components[3][0:10],
				'filename': arr[0]}

	return metadata




def metaData_Create(metadata):
	a = []
	a.append(metadata['project_id'])
	a.append(metadata['tsp_sequential_id'])
	a.append(metadata['device_sequential_id'])
	a.append(metadata['start_date'])
	a.append(metadata['imei'])
	a.append(metadata['vin'])
	a.append(metadata['device_type'])
	a.append(metadata['vin_type'])
	a.append(metadata['filename'])
	a.append(metadata['journey_date'])
	additional_str = ",".join(a)
	return additional_str

		

def process(metadata, journeyfile):
	additional_str = metaData_Create(metadata)
	zf = zipfile.ZipFile(journeyfile, "r")
	journey = zf.open("JOURNEY.csv")
	#newJRNY = "JOURNEY%d.csv" % int(sys.argv[2])
	abstarget = os.path.join(journey_folder, journeyFile)
	try:
		journeyLock.acquire()
		with open(abstarget, "a") as fos:
			for line in journey:
				mainInfo = line.strip()
				str = "%s,%s\n" % (mainInfo, additional_str)
				fos.write(str)
	finally:
		journeyLock.release()

	journey_ext = zf.open("JOURNEY_EXTENSION.csv")
	newJRNYEXT = "JOURNEY_EXT.csv"
	abstarget = os.path.join(journey_extension_folder, newJRNYEXT)
	try:
		journeyExtLock.acquire()
		with open(abstarget, "a") as flos:
			for line in journey_ext:
				mainInfo = line.strip()
				str = "%s,%s\n" % (mainInfo, additional_str)
				flos.write(str)
	finally:
		journeyExtLock.release()
	
	return None




def csvCreate(input_files):
		filecount = 0
		for file in (input_files):
				fileName = file.strip()
				abspath = os.path.join(input_dir, fileName)
				targetpath = os.path.join(Temp_folder, fileName)
				shutil.copy(abspath, targetpath)
				metadata = process_filename(fileName)
				try:
						process(metadata, targetpath)
						filecount += 1
				except zipfile.BadZipfile:
						shutil.copy(abspath, badFiles)
						print("Bad file is %s" % fileName)
				os.remove(targetpath)

		print(filecount)

#the list of files is broken into 9 splits which are processes in parallel and written to one file. 
if __name__ == '__main__':
#				os.chdir(input_dir)
#				files = glob.glob('*zip')
				global recordCount
				text = open(sys.argv[1])
				files = text.readlines()
#				files = open(sys.argv[1]).read().split("\n")
				dirLength = len(files)
				chunk = dirLength/10
				dir1 = files[0:chunk]
				dir2 = files[chunk:chunk*2]
				dir3 = files[chunk*2:chunk*3]
				dir4 = files[chunk*3:chunk*4]
				dir5 = files[chunk*4:chunk*5]
				dir6 = files[chunk*5:chunk*6]
				dir7 = files[chunk*6:chunk*7]
				dir8 = files[chunk*7:chunk*8]
				dir9 = files[chunk*8:chunk*9]
				dir10 = files[chunk*9:]
				p = Pool(10)
				print(p.map(csvCreate, [dir1, dir2, dir3, dir4, dir5, dir6, dir7, dir8, dir9, dir10]))
				hdfsFile = os.path.join(journey_folder,journeyFile)
				fileInHdfs = "/data/ubi/ingestion/journey/%s" % journeyFile
				command = "hdfs dfs -copyFromLocal %s /data/ubi/ingestion/journey/." % hdfsFile
				
				os.system(command)
				sparkCommand = "nohup time spark-submit --verbose --master yarn-client --conf spark.driver.memory=15G --conf spark.logLineage=true --conf spark.network.timeout=800 --class com.csaa.ati.services.octoJourneyIngest --executor-cores 5 --queue adhoc --num-executors 1 --executor-memory 15G /d2/data/apps/ubi/ingestion/octo/scripts/ubi-journeyData-validation.jar \"/data/ubi/ingestion/config/ruleEngine.conf\" \"%s\" >> /d2/data/apps/ubi/ingestion/octo/logs/JOURNEY%s.log &" % (fileInHdfs, index)
				
				os.system(sparkCommand)
				
				





