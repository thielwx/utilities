import datetime as DT
import sys, os
import boto3
import subprocess as sp
import pandas as pd
from botocore.client import Config
from botocore import UNSIGNED
import threading

import warnings

warnings.filterwarnings("ignore")
#===========================================
#Function Land
#===========================================

      
#A function that takes in the datetime and returns the string elements:
#Year, Month, Day, Hour, and Day of Year
def datetimestring(ctime):
    YYYY = ctime.strftime("%Y")
    mm = ctime.strftime("%m")
    dd = ctime.strftime("%d")
    HH = ctime.strftime("%H")
    MM = ctime.strftime("%M")
    doy = ctime.strftime("%j")
    filestring = YYYY+doy+HH+MM
    return YYYY, mm, dd, HH, doy, filestring
    

#A function that takes in the output from urlcreator and runs the wget function
def glmfunction(inputpath, savepath, filestring):
    cmd = 'python /localdata/GLMtools/glmtools/examples/grid/make_GLM_grids.py -o '+savepath+' --fixed_grid --split_events --goes_position east --goes_sector conus --dx=2.0 --dy=2.0 '+inputpath+filestring+'*.nc'
    #print (cmd)
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return 0


def driver()



#===========================================
# Use like this: python runglmtools.py 20190602 201906021200 201906021400 
#===========================================

if __name__ == '__main__':
    main()

def main():
    advancetime = DT.timedelta(seconds=60) #Can also set this to days or months (days=1)
    
    args = sys.argv

    case = args[1]
    startdate = args[2] #YYYYMMDDHHMM
    enddate = args[3] #YYYYMMDDHHMM
     
    #Creating the savepath path
    savepath = '/localdata/cases/'+case+'/GLM_grids/'
    inputpath = '/localdata/cases/'+case+'/GLM/OR_GLM-L2-LCFA_G16_s'

    #Creating the path if one doesn't already exist for the files
    if not os.path.exists(savepath):
        os.makedirs(savepath)

    #Creating the starting and ending times
    dtstart = DT.datetime.strptime(startdate, "%Y%m%d%H%M") 
    dtend = DT.datetime.strptime(enddate, "%Y%m%d%H%M")

    #List of minutes to pull from
    minute_list = pd.date_range(start=dtstart,end=dtend,freq='M').to_pydatetime().tolist()
    
    #Making sure the start time is before the end time
    if dtstart > dtend:
        print ('ERROR: Start time is after the end time')
        sys.exit(0)

for i in minutelist[::5]:
    



def driver():

    #While loop for processing the data on a per-mintute basis
    ctime = dtstart
    while ctime < dtend:
        print(ctime.strftime("%Y%m%d-%H%M")) #Checkpoint
        YYYY, mm, dd, HH, doy, filestring = datetimestring(ctime)
        glmfunction(inputpath, savepath, filestring)
        ctime = ctime+advancetime

