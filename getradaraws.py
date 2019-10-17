import datetime as DT
import sys, os
import boto3
from botocore.client import Config
from botocore import UNSIGNED


def getradarlist(radar,ctime,noaas3,path):
    
    radarlist = []
    prefix = "%s/%s/" % (ctime.strftime("%Y/%m/%d"), radar)
    radardic = noaas3.list_objects_v2(Bucket='noaa-nexrad-level2', Delimiter='/', Prefix= prefix)
    print(prefix)
    if "Contents" in radardic:
        for i in radardic['Contents'][:]:
            if ((i['Key'][-2::] == 'gz' and i['Key'][-13:-11] == ctime.strftime("%H")) or (i['Key'][-3::] == 'V06' and i['Key'][-10:-8] == ctime.strftime("%H"))):              
                #print "MATCHES"
                fin = open(path+'LIST.txt','a')
                fin.write(i['Key']+'\n')
                fin.close()
                radarlist.append(i['Key'])
            else:
                continue

    return(radarlist,prefix)

def downloadradars(radarlist,ctime,noaas3,prefix,radar,outloc):
    if not os.path.exists(outloc):
        os.makedirs(outloc)
    for i in radarlist:
        if ".gz" in i:
            noaas3.download_file('noaa-nexrad-level2',i,outloc+'/'+i[-26::])
        else: 
            noaas3.download_file('noaa-nexrad-level2',i,outloc+'/'+i[-23::])
    return 0
'''
#not used:
def downloadsingleradar(radarfile,ctime,noaas3,prefix):

    if not os.path.exists('/data2/Radar/'+prefix):
        os.makedirs('/data2/Radar/'+prefix)
    print(radarfile)
    noaas3.download_file('noaa-nexrad-level2',radarfile,'/data2/Radar/'+radarfile)
    print('Downloaded '+radarfile)
    return 0

#not used:
def getradarsindate(ctime,noaas3):
    

    radarsday = noaas3.list_objects_v2(Bucket='noaa-nexrad-level2',Delimiter='/',Prefix=ctime.strftime("%Y/%m/%d/"))
    for i in radarsday['CommonPrefixes']:
        currentradardate = i['Prefix']
        if(currentradardate[-5::].startswith('K')):

            radarsdaydic = noaas3.list_objects_v2(Bucket='noaa-nexrad-level2',Delimiter='/',Prefix=i['Prefix'])
    
            print(currentradardate[-5::])
            for i in radarsdaydic['Contents']:
                if( i['Key'].endswith('gz')):
                    radarfile = i['Key'][:]
                    downloadsingleradar(radarfile,ctime,noaas3,currentradardate)
    
    return 0
#not used
def copyNSEdata(ctime,path):
    nsePath = path + 'env/NSE'
    print "Copying NSE for %s"%ctime.strftime("%Y%m%d")
    if not os.path.exists(nsePath):
        os.makedirs(nsePath)
    syscmd = 'rsync -aP /data6/NSE/%s/NSE/ %s'%(ctime.strftime("%Y%m%d"),nsePath)  
    os.system(syscmd)

#not used:
def configCase(path,uleft,lright):
#write domain_info.txt file
    fin = open(path+'domain_info.txt','w')
    fin.write('%s\n%s\nComposite LayerAverage HDA VIL'%(uleft,lright))
    fin.close()

#Create symbolic link for terrain file
    terrainPath = path + '../../../base_files/terrain'
    if not (os.path.exists(terrainPath) and os.path.exists(path+'terrain')):
        syscmd = 'ln -s %s %s/terrain'%(terrainPath,path)
        os.system(syscmd)
    else:
        print 'No existing terrain file exists\nYou need to copy in a terrain file to %s'%path
#Create symbolic link for the grib2conv.xml file
    xmlPath = path + '../../../base_files/grib2conv.xml'
    if not (os.path.exists(xmlPath) and os.path.exists(path+'grib2conv.xml')):
        syscmd = 'ln -s %s %s/grib2conv.xml'%(xmlPath,path)
        os.system(syscmd)
    else:
        print 'No existing grib2conv.xml file exists\nYou need to copy in a grib2conv.xml file to %s'%path
#Copy the processSingleData.py file into case
    procPath = path + '../../../base_files/processSingleData.py'
    if not (os.path.exists(procPath) and os.path.exists(path+'processSingleData.py')):
        syscmd = 'rsync -aP %s %s/processSingleData.py'%(procPath,path)
        os.system(syscmd)
    else:
        print 'No existing processSingleData.py file exists\nYou need to copy in a processSingleData.py file to %s'%path
#Create symbolic link for the processMultiData.py
    procPath = path + '../../../base_files/processMultiData.py'
    if not (os.path.exists(procPath) and os.path.exists(path+'processMultiData.py')):
        syscmd = 'rsync -aP %s %s/processMultiData.py'%(procPath,path)
        os.system(syscmd)
    else:
        print 'No existing processMultiData.py file exists\nYou need to copy in a processMultiData.py file to %s'%path
'''





if __name__ == '__main__':

    advancehour = DT.timedelta(hours=1) #Can also set this to days or months (days=1)
    
    noaas3 = boto3.client('s3',region_name='us-east-1',config=Config(signature_version=UNSIGNED))
############################################
# Use like this: python getradaraws.py KILN 20190603 2019060312 2019060314
############################################
    args = sys.argv

    
    radar = args[1]
    case= args[2]
    startdate = args[3] #YYYYMMDDHH
    enddate = args[4] #YYYYMMDDHH
    path = '/localdata/cases/'+case+'/raw_radar/'+radar+'/'

    #Creating the starting and ending times
    dtstart = DT.datetime.strptime(startdate, "%Y%m%d%H") 
    dtend = DT.datetime.strptime(enddate, "%Y%m%d%H")
    
    #Making sure the start time is before the end time
    if dtstart > dtend:
        print ('ERROR: Start time is after the end time')
        sys.exit(0)

    #Creating the path if one doesn't already exist for the radar files
    if not os.path.exists(path):
        os.makedirs(path)

    prvTime=""
    
    print (radar) #Checkpoint
    outloc=path
    ctime = dtstart
    while ctime < dtend:
        print(ctime.strftime("%Y%m%d-%H")) #Checkpoint
        ##getradarlist()
        radarlist, prefix = getradarlist(radar,ctime,noaas3,path)
        ##downloadradars()
        out = downloadradars(radarlist,ctime,noaas3,prefix,radar,outloc)

        prvTime=ctime.strftime("%Y%m%d")
        ctime = ctime+advancehour

