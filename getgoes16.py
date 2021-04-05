import datetime as DT
import sys, os
import boto3
import subprocess as sp
from botocore.client import Config
from botocore import UNSIGNED


#===========================================
#Function Land
#===========================================


#A function that creates the initial url as a function of date
def starturl(dtstart):
    if dtstart < DT.datetime(2019, 5, 8):
        url = 'http://goes16.metr.ou.edu/'
    elif (dtstart >= DT.datetime(2019, 5, 8)) & (dtstart < DT.datetime(2020,3,1)):
        url = 'http://goes16.metr.ou.edu/new_data/'
    else:
        url = 'http://goes16.metr.ou.edu/files_after_2019_05_08/'
    return url


         
#A function that takes in the datetime and returns the string elements:
#Year, Month, Day, Hour, and Day of Year
def datetimestring(ctime):
    YYYY = ctime.strftime("%Y")
    mm = ctime.strftime("%m")
    dd = ctime.strftime("%d")
    HH = ctime.strftime("%H")
    doy = ctime.strftime("%j")
    directorystring = YYYY+'_'+mm+'_'+dd+'_'+doy
    return YYYY, mm, dd, HH, doy, directorystring
    


#A function that takes in a bunch of stuff and creates the download/save paths 
#and beginning of the file names used in the wget function
def urlcreator(case, platform, product, url, directorystring, YYYY, doy, HH):
    if platform == 'GLM':
        dpath = str(url)+directorystring+'/glm/L2/LCFA/'
        dfile = 'OR_GLM-L2-LCFA_G16_s'+YYYY+doy+HH
        savepath = '/localdata/cases/'+case+'/'+platform+'/'
    elif platform == 'ABI': #The string slicing here is so that you can do the individual ABI L1b bands
        if (product[:-2] == 'RadC') | (product[:-2] == 'RadF') | (product[:-2] == 'RadM1') | (product[:-2] == 'RadM2'):
            dpath = url + directorystring+'/abi/L1b/'+product[:-2]+'/'
            dfile = 'OR_ABI-L1b-'+product[:4]+'-M6C'+product[-2:]+'_G16_s'+YYYY+doy+HH
            savepath = '/localdata/cases/'+case+'/'+platform+'/'+product+'/'+str(product[-2:])
        elif (product[:-2] == 'CMIPC') | (product[:-2] == 'CMIPF') | (product[:-2] == 'CMIPM1') | (product[:-2] == 'CMIPM2'):
            dpath = url+directorystring+'/abi/L2/PDA/'+product[:-2]+'/'
            dfile = 'OR_ABI-L2-'+product[:5]+'-M6C'+product[-2:]+'_G16_s'+YYYY+doy+HH
            savepath = '/localdata/cases/'+case+'/'+platform+'/'+product+'/'
        elif (product == 'DMWC'):
            dpath = url+directorystring+'/abi/L2/PDA/'+product+'/'
            dfile = 'OR_ABI-L2-'+product+'-M6C02_G16_s'+YYYY+doy+HH
            savepath = '/localdata/cases/'+case+'/'+platform+'/'+product+'/'
        else:
            dpath = url+directorystring+'/abi/L2/PDA/'+product+'/'
            dfile = 'OR_ABI-L2-'+product+'-M6_G16_s'+YYYY+doy+HH
            savepath = '/localdata/cases/'+case+'/'+platform+'/'+product+'/'
    return dpath, dfile, savepath


#A function that takes in the output from urlcreator and runs the wget function
def wgetfunction(dpath, dfile, savepath):
    cmd = 'wget -r -l1 -q -nH --cut-dirs=10 -np "'+dpath+'" -A "'+dfile+'*.nc" -P '+savepath
    #print (cmd)
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return 0


#===========================================
# Use like this: python getgoes16.py ABI CODC 20190602 2019060212 2019060214 
# NOTE: If using RadC or CMIPC from ABI, must include channel number like so 'RadC10'
#===========================================

if __name__ == '__main__':

    advancehour = DT.timedelta(hours=1) #Can also set this to days or months (days=1)
    
    args = sys.argv

    
    platform = args[1]
    product = args[2]
    case = args[3]
    startdate = args[4] #YYYYMMDDHH
    enddate = args[5] #YYYYMMDDHH



    #Creating the starting and ending times
    dtstart = DT.datetime.strptime(startdate, "%Y%m%d%H") 
    dtend = DT.datetime.strptime(enddate, "%Y%m%d%H")

    
    #Making sure the start time is before the end time
    if dtstart > dtend:
        print ('ERROR: Start time is after the end time')
        sys.exit(0)


    #Creating the starting URL
    url = starturl(dtstart)    

    #While loop for downloading the data on an hourly basis
    ctime = dtstart
    while ctime < dtend:
        print(ctime.strftime("%Y%m%d-%H")) #Checkpoint
        YYYY, mm, dd, HH, doy, directorystring = datetimestring(ctime)
        dpath, dfile, savepath = urlcreator(case, platform, product, url, directorystring, YYYY, doy, HH)
        
        if not os.path.exists(savepath):
            os.makedirs(savepath)   

        wgetfunction(dpath, dfile, savepath)
        ctime = ctime+advancehour

