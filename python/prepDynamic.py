import os
import sys

dir="/d2/data/apps/ubi/ingestion/octo/fileLists"
#prodSource
source="/ubidata/JourneyData/sys.argv[1]"
#source="/d2/ubi/octoJourney/threaded/source/sys.argv[1]"
#source="/d2/ubi/octoJourney/threaded/source/2017-11-16"
#source1="/ubidata_np/test"

	
def writeFile(files, index):
	os.chdir(dir)
	fileName = "Journeyfiles_%d.txt" % index
	with open(fileName, "a") as fos:
		for i in files:
			if i.endswith("zip"):
				str = "%s\n" %(i)
				fos.write(str)
							
if __name__ == '__main__':
	files = os.listdir(source)
	chunk = 32768
	chunks = len(files)/chunk
	lst = []
	for i in range(0, chunks):
		lst.append(files[i*chunk:(i+1)*chunk])

	lst.append(files[chunk*chunks:])
	for i in range(0,chunks+1):
		writeFile(lst[i],i)
