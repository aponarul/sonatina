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
import datetime
from datetime import date, timedelta

yesterday = datetime.date.today() - timedelta(1)

listDirs="/d2/data/apps/ubi/ingestion/octo/fileLists"
#listDirs="/d2/ubi/octoJourney/threaded/fileLists"




def process(file):
	#command = "python /d2/ubi/octoJourney/threaded/threaded.py %s" % file
	command = "python /d2/data/apps/ubi/ingestion/octo/scripts/threaded.py %s %s" % (file,yesterday)
	os.system(command)
	

if __name__ == '__main__':
	files = os.listdir(listDirs)
	lst = []
	for i in files:
		lst.append(os.path.join(listDirs,i))
	p = Pool(len(files))
	print(p.map(process,lst))
