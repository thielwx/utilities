import sys
import subprocess as sp
import os
import time
import glob

#=====================================
#Function Land
#=====================================


def run_makeindex(outloc):

    '''Runs makeindex input: output locations. Returns process code '''

    cmd = 'makeIndex.pl '+ outloc +' '+ 'code_index.xml'
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return p.returncode

def run_replaceindex(outloc,codefiles,startdate,fakefile):

    '''Runs makeindex input: output locations. Returns process code '''
    xmlfiles = ' '.join(codefiles)
    cmd = 'replaceIndex -i "/localdata/cases/'+startdate+'/NSE/code_index.xml '+ xmlfiles +' '+ fakefile +'" -o '+ outloc+'/biginput.xml'
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return p.returncode




def run_w2mergerrefl(outloc,startdate,nw_lat,nw_lon,se_lat,se_lon):
    logfile = open('logs/w2mergerrefl'+startdate+'.log','w')
    cmd = 'w2merger -i '+outloc+'/biginput.xml -o '+outloc+'/merged -C 7 -e 60 -t "'+nw_lat+' '+nw_lon+' 22" -b "'+se_lat+' '+se_lon+' 0.25" -s "0.01 0.01 NMQWD" -I ReflectivityQC -a "Composite HDA LayerAverage VIL" -g 8 -R 300 -p 0.5 -T 12'   
    
    p = sp.Popen(cmd,shell=True, stdout=logfile,stderr=sp.STDOUT)
    p.wait()
    logfile.flush()

    logfile.close()

    return p.returncode


#=============================================
#Run like this: python runmerger.py 20180519
#=============================================

def main():
	
    args = sys.argv
    startdate = str(args[1])

    radars = os.listdir('/localdata/cases/'+startdate+'/processed_radar/') #Listing the radars	

    nw_lat = '37.77'
    nw_lon = '-100.05'
    se_lat = '34.76'
    se_lon = '-96.69'

    codefiles = ['/localdata/cases/'+startdate+'/processed_radar/'+i+'/code_index.xml' for i in radars]
    fakefile = '/localdata/cases/'+startdate+'/FAKE/code_index.xml'

    outloc = '/localdata/cases/'+startdate+'/merged_radar'
    largecodereturn = run_replaceindex(outloc, codefiles, startdate, fakefile)


    w2mergercode = run_w2mergerrefl(outloc,startdate,nw_lat,nw_lon,se_lat,se_lon)
    if not(w2mergercode == 0 or w2mergercode == 255):
        print('ERROR   '+str(w2mergercode))
        sys.exit('1 merger REF failed')
    accfinish =time.time()


    updatecode = run_makeindex(outloc)

    sys.exit(0)
    return 0



if __name__ == '__main__':
    main()

