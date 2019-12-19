import sys
import subprocess as sp
import os
import time
import shutil
import threading


def run_ldm2netcdf(radar, startdate, inloc, outloc, prefix):

    '''Runs ldm2netcdf input: radar site, input and output locations. Returns process code and writes out log file called ldm2netcdf.log'''

    logfile = open('logs/ldm2netcdf'+startdate+'.'+radar+'.log','w')

    cmd = 'ldm2netcdf  -s ' + radar + ' -i '+ inloc + radar +' -o ' + outloc + '  -a -1 -L -p  '+ prefix #+' --verbose unanticipated'

    p = sp.Popen(cmd,shell=True,stdout=logfile,stderr=sp.STDOUT)
    p.wait()
    logfile.flush()

    logfile.close()

    return p.returncode


def run_makeindex(outloc):

    '''Runs makeindex input: output locations. Returns process code '''

    cmd = 'makeIndex.pl '+ outloc +' code_index.xml'
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return p.returncode

def run_replaceindex(outloc,soundingloc):

    '''Runs replaceindex to get NSE and radar on same xml.'''

    cmd = 'replaceIndex -o '+outloc +'/big_index.xml -i "'+outloc+'/code_index.xml '+soundingloc+'/code_index.xml"'
    p = sp.Popen(cmd,shell=True)
    p.wait()
    return p.returncode


def run_dealiasVel(radar, startdate, outloc, soundingloc, prefix):

    '''Runs dealias2d input: radar site, output location, and sounding file location. Returns process code and writes out log file called dealias2d.log'''

    logfile = open('logs/dealias2d'+startdate+'.'+radar+'.log','w')

    cmd = 'dealiasVel -R '+ radar + ' -i ' + outloc+'/big_index.xml ' +' -o '+ outloc +' -S SoundingTable -Z ReflectivityQC'

    p = sp.Popen(cmd,shell=True,stdout=logfile,stderr=sp.STDOUT)
    p.wait()
    logfile.flush()

    logfile.close()


    return p.returncode


def run_w2qcnn(radar, startdate, outloc):
    
    '''Runs qcnn input: radar site, outlocation. Returns process code and writes out log file named qcnn.log'''

    logfile = open('logs/qcnn'+startdate+'.'+radar+'.log','w')

    cmd = 'w2qcnndp -i ' + outloc +'/code_index.xml -o '+ outloc +' -s '+ radar + ' -R 0.25x0.5x460 --verbose=debug'

    p = sp.Popen(cmd,shell=True, stdout=logfile,stderr=sp.STDOUT)
    p.wait()
    logfile.flush()

    logfile.close()


    return p.returncode


def run_w2circ(radar, startdate, outloc):

    ''' Runs w2circ input: radar site and outlocation. Returnes process code and writes out log file named w2circ.log. '''

    logfile = open('logs/w2circ'+startdate+'.'+radar+'.log','w')

    #cmd = ['w2circ', '-i', outloc+'code_index.xml', '-o', outloc, '-a', '-w', '-c']
    #cmd = 'w2circ -i '+ outloc +'/code_index.xml -o '+ outloc + ' -az -w -C'
    #cmd = 'w2circ -i '+ outloc +'/code_index.xml -o '+ outloc + ' -sr -z "ReflectivityQC" -S -D -t -az -C -w -vmax -L "0:2:0:7.5:AGL 2:5:0:90:AGL" -g "'+radar+' /localdata/terrain/'+radar+'.nc" -b 5 --verbose'
    cmd = 'w2circ -i '+ outloc +'/code_index.xml -o '+ outloc + ' -sr -z "ReflectivityQC" -D -t -az -w -vmax -L "0:2:0:7.5:AGL 2:5:0:90:AGL" -g "'+radar+' /localdata/terrain/'+radar+'.nc" -b 5 --verbose'


    p = sp.Popen(cmd, shell=True, stdout=logfile, stderr=sp.STDOUT)
    p.wait()
    logfile.flush()

    logfile.close()


    return p.returncode



def runwdssii(radar,startdate,inloc,outloc,soundingloc,prefix):
    """MAIN driver function"""

    ldmstart = time.time()
    ldm2netcdfCode = run_ldm2netcdf(radar, startdate, inloc, outloc, prefix)
    if ldm2netcdfCode:
        print('ERROR')
        sys.exit('1 ldm2netcdf failed')
    ldmfinish = time.time()

    print("ldm time: " +str(ldmfinish-ldmstart)+" "+str(radar) +" \n")
 
    updatecode = run_makeindex(outloc)
    
    accstart =time.time()
    
    qcnnCode = run_w2qcnn(radar, startdate, outloc)
    if not(qcnnCode == 0 or qcnnCode == 255):
        print('ERROR   '+str(qcnnCode))
        sys.exit('1 qcnn failed')

    accfinish =time.time()
  
    print("qcnn time: "+ str(accfinish-accstart) +" "+str(radar)+"\n")

    updatecode = run_makeindex(outloc)
    updatecode = run_replaceindex(outloc,soundingloc)

    dealiasstart = time.time()

    dealias2dCode = run_dealiasVel(radar, startdate, outloc, soundingloc, prefix)
    if dealias2dCode:
        print( 'ERROR')
        sys.exit('1 dealias failed')
    dealiasfinish = time.time()

    print("dealias time: "+ str(dealiasfinish-dealiasstart)+" "+str(radar)+"\n")
    updatecode = run_makeindex(outloc)

    return 0
def main():
    """run like this python runwdssii.py 20180501 /localdata/cases/20180501/raw_radar/ /localdata/cases/20180501/processed_radar/ /localdata/cases/20180501/NSE/ K

    also make logs file where you run this script so logs get made.
    """
    args = sys.argv

    radars = ['KLBB','KAMA','KFDR','KTLX'] #20180501

    startdate = args[1] #YYYMMDD
    inloc = args[2]     #inital level2 location
    outloc = args[3]    #Location of output
    soundingloc = args[4]#location to NSE path
    prefix = args[5]     #prefix of files

    maxthreads = 5      #change for you computer...cpus

    for radar in radars:
        if threading.active_count() <= maxthreads:

            print(radar) 
            outloc1 = outloc+'/'+radar
            t = threading.Thread(target=runwdssii,name=threading.active_count(),args=(radar,startdate,inloc,outloc1,soundingloc,prefix))
            t.start()
            time.sleep(2)
        else:
            while threading.active_count() > maxthreads:
                time.sleep(300)

    sys.exit(0)
    return 0



if __name__ == '__main__':
    main()



