#===========================================
# Use like this:
#
#       python runglmtools.py INPUT_PATH OUTPUT_PATH START_TIME END_TIME POSITION SCENE CENTER_LAT CENTER_LON WIDTH HEIGHT
#
#
# Formatting:
#
#       START_TIME/END_TIME   = YYYYmmddHHMM
#       POSITION              = east OR west
#       SCENE                 = conus OR meso OR custom
#       CENTER_LAT/CENTER_LON = [deg]
#       WIDTH/HEIGHT          = [km]
#
#
# Examples:
#
#       python runglmtools.py /localdata/cases/20190524/GLM16/ /localdata/cases/20190524/GLM16_grids/ 201905242000 201905242220 east conus 0 0 0 0
#       python runglmtools.py /localdata/cases/20190524/GLM16/ /localdata/cases/20190524/GLM16_grids/ 201905242000 201905242220 east meso 37.5 -97 0 0
#       python runglmtools.py /localdata/cases/20190524/GLM16/ /localdata/cases/20190524/GLM16_grids/ 201905242000 201905242220 east custom 37.5 -97 100 100 
#
#===========================================



#=======================================
#  This program is designed to run glmtools using thredding to speed up processing
#  and be flexible enough to be used for GOES-East/West along with the conus, meso, and custom scenes
#  
#  Author: Kevin Thiel
#  Creation date: 3/31/2021
# 
#  Special Notes:
#               This program processes the data down to the minute
#
#======================================



from datetime import datetime,timedelta
import sys, os
import subprocess as sp
import pandas as pd
from botocore.client import Config
from botocore import UNSIGNED
import threading
import numpy as np
import time

import warnings

warnings.filterwarnings("ignore")


#===========================================
#Function Land
#===========================================
    

#A function that generates the command to make the glm grids
def glmfunction(input_path, output_path, position, scene, center_lat, center_lon, length, width, file_string, curr_time):

    if scene == 'conus':
        cmd = 'python /localdata/GLMtools/glmtools/examples/grid/make_GLM_grids.py -o '+output_path+' --fixed_grid --split_events --dx=2.0 --dx=2.0 --goes_position='+position+' --goes_sector='+scene+' '+input_path+file_string+curr_time+'*.nc'
    elif scene == 'meso':
        cmd = 'python /localdata/GLMtools/glmtools/examples/grid/make_GLM_grids.py -o '+output_path+' --fixed_grid --split_events --dx=2.0 --dx=2.0 --goes_position='+position+' --goes_sector='+scene+' --ctr_lat='+center_lat+' --ctr_lon='+center_lon+' '+input_path+file_string+curr_time+'*.nc'
    elif scene == 'custom':
        cmd = 'python /localdata/GLMtools/glmtools/examples/grid/make_GLM_grids.py -o '+output_path+' --fixed_grid --split_events --dx=2.0 --dx=2.0 --goes_position='+position+' --goes_sector='+scene+' --ctr_lat='+center_lat+' --ctr_lon='+center_lon+' --width='+width+' --height='+height+' '+input_path+file_string+curr_time+'*.nc'
    print (cmd)

    p = sp.Popen(cmd,shell=True)
    p.wait()
    return 0


#============================================================================
#The main part of the program
#============================================================================


#Universal varaibles
dt = timedelta(seconds=60) #Can also set this to days or months (days=1)
maxthreads = 7

#Reading in the function arguments
args = sys.argv

input_path   = args[1]
output_path    = args[2]

start_date   = args[3]
end_date     = args[4]

position    = args[5].lower()
scene       = args[6].lower()

center_lat  = args[7]
center_lon  = args[8]

width       = args[9]
height      = args[10]


#Creating the starting and ending times
dtstart = datetime.strptime(start_date, "%Y%m%d%H%M") 
dtend = datetime.strptime(end_date, "%Y%m%d%H%M")


#=============================================================================

##Making sure the start time is before the end time
if dtstart > dtend:
    print ('ERROR: Start time is after the end time')
    sys.exit(0)
##Making sure the position has been defined correctly
if (position != 'east') | (position != 'west'):
    print ('ERROR: Position not defined as east or west')
    sys.exit(0)
##Making sure the scene has been defined correctly
if (scene != 'conus') | (scene != 'meso') | (scene != 'custom'):
    print ('ERROR: Scene not defined as conus, meso, or custom')
    sys.exit(0)

#Creating the path if one doesn't already exist for the files
if not os.path.exists(save_path):
    os.makedirs(save_path)

#Getting the sat number and defining the file string that we will use later
if position == 'east':
    sat_number = '16'
if position == 'west':
    sat_number = '17'

file_string = 'OR_GLM-L2-LCFA_G'+sat_number+'_s'


#List of minutes to pull from
minute_list = pd.date_range(start=dtstart,end=dtend,freq='min').to_pydatetime().tolist()
index = np.arange(0,len(minute_list),1)



#Loop that goes through minute by minute to process each varaible on a thread
for minute in minute_list[:-1]:
    print (minute)
    curr_time = minute.strftime('%Y%j%H%M')
    if threading.active_count() <= maxthreads:
        t = threading.Thread(target=glmfunction,name=threading.active_count(),args=(input_path, output_path, position, scene, center_lat, center_lon, length, width, file_string, curr_time))
        t.start()
        time.sleep(2)
        
    else:
        while threading.active_count() > maxthreads:
            time.sleep(30)
            #print ('**************************DING*******************************')
            t = threading.Thread(target=glmfunction,name=threading.active_count(),args=(input_path, output_path, position, scene, center_lat, center_lon, length, width, file_string, curr_time))
            t.start()
            time.sleep(2)

