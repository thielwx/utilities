import datetime as DT
import sys, os
import boto3
import subprocess as sp
import pandas as pd
from botocore.client import Config
from botocore import UNSIGNED
import threading
import numpy as np
import time

import warnings

warnings.filterwarnings("ignore")


sat_number = '17'
#Dictionary that allows us to switch between 16 and 17 data with some grace
glm_vars = {'16':['east','/GLM_grids/',''],
            '17':['west','/GLM17_grids/','17']}

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
    cmd = 'python /localdata/GLMtools/glmtools/examples/grid/make_GLM_grids.py -o '+savepath+' --fixed_grid --split_events --goes_position '+glm_vars[sat_number][0]+' --goes_sector conus --dx=2.0 --dy=2.0 '+inputpath+filestring+'*.nc'
    print (cmd)
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return 0




#===========================================
# Use like this: python runglmtools.py 20190602 201906021200 201906021400
#===========================================

def main():
    advancetime = DT.timedelta(seconds=60) #Can also set this to days or months (days=1)
    
    args = sys.argv

    case = args[1]
    startdate = args[2] #YYYYMMDDHHMM
    enddate = args[3] #YYYYMMDDHHMM
    
     
    #Creating the savepath path
    savepath = '/localdata/cases/'+case+glm_vars[sat_number][1]
    inputpath = '/localdata/cases/'+case+'/GLM'+glm_vars[sat_number][2]+'/OR_GLM-L2-LCFA_G'+sat_number+'_s'

    #Creating the path if one doesn't already exist for the files
    if not os.path.exists(savepath):
        os.makedirs(savepath)

    #Creating the starting and ending times
    dtstart = DT.datetime.strptime(startdate, "%Y%m%d%H%M") 
    dtend = DT.datetime.strptime(enddate, "%Y%m%d%H%M")

    #List of minutes to pull from
    minute_list = pd.date_range(start=dtstart,end=dtend,freq='min').to_pydatetime().tolist()
    
    #Making sure the start time is before the end time
    if dtstart > dtend:
        print ('ERROR: Start time is after the end time')
        sys.exit(0)

    maxthreads = 7
    index = np.arange(0,len(minute_list),1)


    for i in index[:-1:5]: #Outer loop for each 5 minutes
        minutes = [minute_list[i],
                minute_list[i+1],
                minute_list[i+2],
                minute_list[i+3],
                minute_list[i+4]]
        for j in minutes:
            print (j)
            if threading.active_count() <= maxthreads:
                YYYY, mm, dd, HH, doy, filestring = datetimestring(j)
                t = threading.Thread(target=glmfunction,name=threading.active_count(),args=(inputpath, savepath, filestring))
                t.start()
                time.sleep(2)
            else:
                while threading.active_count() > maxthreads:
                    time.sleep(30)
                    #print ('**************************DING*******************************')
                    YYYY, mm, dd, HH, doy, filestring = datetimestring(j)
                    t = threading.Thread(target=glmfunction,name=threading.active_count(),args=(inputpath, savepath, filestring))
                    t.start()
                    time.sleep(2)

if __name__ == '__main__':
    main()
