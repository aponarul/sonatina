import datetime
from datetime import date, timedelta
import os
yesterday = datetime.date.today() - timedelta(1)
command = "python /d2/data/apps/ubi/ingestion/octo/scripts/prepDynamic.py %s" % yesterday
#print(command)
os.system(command)

