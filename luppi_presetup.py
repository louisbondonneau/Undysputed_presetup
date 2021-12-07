import numpy as np
import os, sys, re, socket
import argparse
import time
import ATNF
import os.path
from os.path import splitext
from os.path import split
from os.path import basename
import subprocess
from subprocess import check_output

import astropy.coordinates as coord
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from datetime import datetime
from datetime import timedelta

# 2020/11/25 modification for the multicast version of dump_udp_ow_12
# 2020/12/04 sync ephem pass from scp to rsync
# 2020/19/05 it is no possible to use diff projid in lanes with param --projid=ES12
# 2020/19/05 option to force undysputedbk3

parser = argparse.ArgumentParser()
parser.add_argument('-bk1', dest='bk1', action='store_true',
        help="Force bk1")
parser.add_argument('-bk2', dest='bk2', action='store_true',
        help="Force bk2")
parser.add_argument('-bk3', dest='bk3', action='store_true',
        help="Force bk3")
parser.add_argument('-test', dest='testmode', action='store_true',
        help="testmode")
parser.add_argument('INPUT_ARCHIVE', nargs='+', help="Parsetfile of the current observation")
args = parser.parse_args()


HOSTNAME=socket.gethostname()
if(args.bk1):HOSTNAME='undysputedbk1'
if(args.bk2):HOSTNAME='undysputedbk2'
if(args.bk3):HOSTNAME='undysputedbk3'
if(args.testmode):
    test = 1
else:
    test = 0

PORT = [1242, 5586, 1242, 5586]  #PORT = [1490, 5586, 1491, 5587]
RCV_IP = ["192.168.5.100", "192.168.5.100",
          "192.168.5.101", "192.168.5.101"]
DST_IP = ["192.168.5.100", "192.168.5.100", 
          "192.168.5.101", "192.168.5.101"]
multicast = True
if(multicast):
    DST_IP = ["224.2.3.1", "224.2.3.2",
              "224.2.3.3", "224.2.3.4"]

#Liste des groupes multicast:
# CEP = {0: ("CEP 0", "224.2.3.1", 0x07d0),   # configure sender as: MAC/IP/port = 01:00:5e:02:03:01 / 224.2.3.1 / 0x07d0 (2000)
#        1: ("CEP 1", "224.2.3.2", 0x07d1),   # configure sender as: MAC/IP/port = 01:00:5e:02:03:02 / 224.2.3.2 / 0x07d1 (2001)
#        2: ("CEP 2", "224.2.3.3", 0x07d2),   # configure sender as: MAC/IP/port = 01:00:5e:02:03:03 / 224.2.3.3 / 0x07d2 (2002)
#        3: ("CEP 3", "224.2.3.4", 0x07d3),   # configure sender as: MAC/IP/port = 01:00:5e:02:03:04 / 224.2.3.4 / 0x07d3 (2003)
default_topic = 'ES03'

def stop_function( stopTime ):
    STOPchar = 'echo "python /home/louis/luppi_test_smart/python/luppi_setup.py -X &> /obs/PSR_STOP.log"  > /obs/JOB'
    STOPtoAT='sudo at '+TIME_TO_hhmm_MMDDYY(stopTime, offset=-2)+' -f /obs/JOB &> /obs/JOB.log'
    completed = subprocess.run(STOPchar, shell=True)
    print('returncode:', completed)
    completed = subprocess.run(STOPtoAT, shell=True)
    print('returncode:', completed)
    print('STOP is set for '+TIME_TO_hhmm_MMDDYY(stopTime, offset=-2)+'\n'+STOPtoAT)

DATE = args.INPUT_ARCHIVE[0].split('-')[-1].split('.')[0].rstrip()
SHELLfile=' >> /data2/SHELL--at-'+str(DATE)+'-BEAM'

mjdsnow=86400
if not (test):
    # ephem sync with bk2
    if(HOSTNAME=='undysputedbk1'):
        completed = subprocess.run("rsync -av -e \"ssh \" /ephem/*.par root@undys2:/ephem/", shell=True)
        completed = subprocess.run("rsync -av -e \"ssh \" /ephem/*.par nfrplsobs@databf2dt:/databf2/nenufar-pulsar/ES03/ephem/", shell=True)
        completed = subprocess.run("rsync -av -e \"ssh \" /ephem/old_parfiles/*.par nfrplsobs@databf2dt:/databf2/nenufar-pulsar/ES03/ephem/old_parfiles/", shell=True)
        completed = subprocess.run("rsync -av -e \"ssh \" /ephem/old_parfiles/*.par root@undys2:/ephem/old_parfiles/", shell=True)
# MJDs NOW
#mjdsnow = check_output('/home/louis/LUPPI_presetup/getmjdtime | cut -d = -f3 | head -1 | awk \'{print$1}\'', shell=True)
#print(mjdsnow)
#mjdsnow = (int(str(mjdsnow).split('\'')[1].split('\\')[0])) % (24*3600)
#print(mjdsnow)
time.sleep(1)

Parsetfile = open(args.INPUT_ARCHIVE[0], "r")
if not (test):completed = subprocess.run('scp '+args.INPUT_ARCHIVE[0]+'  lbondonneau@nancep3dt:/home/lbondonneau/OBS/', shell=True)
print('scp '+args.INPUT_ARCHIVE[0]+'  lbondonneau@nancep3dt:/home/lbondonneau/OBS/')
STOPnow = 'python /home/louis/luppi_test_smart/python/luppi_setup.py -X'
STOPchar = 'echo "python /home/louis/luppi_test_smart/python/luppi_setup.py -X &> /obs/PSR_STOP.log"  > /obs/JOB'
LUPPIsetup = 'python /home/louis/luppi_test_smart/python/luppi_setup.py '
luppi_daq_dedisp = '/home/louis/luppi_test_smart/luppi_daq_dedisp_GPU1 '

write_raw = 'sudo -E /home/louis/luppi_test_smart/luppi_write_raw '

write_raw_olaf = 'sudo /home/louis/olaf_script/dump_udp_ow_12_multicast'

TFsetup = 'sudo -E /home/cognard/bin/tf '


#Observation.stopTime=2018-05-06T20:52:20Z
def TIME_TO_hhmm_MMDDYY(TIME, offset=0):
    TIME = TIME.split('Z')[0].split('T')
    stopTime = TIME[0]+' '+TIME[1]
    mjd = JULIEN_TO_MJD(stopTime) + (float(offset)/86400)
    julian =  MJD_TO_JULIEN(mjd)
    Y = julian.split('-')[0].strip()
    Y = Y[2]+Y[3]
    M = julian.split('-')[1].strip()
    D = julian.split('-')[2].split(' ')[0].strip()
    H = julian.split(' ')[1].split(':')[0].strip()
    Mi = julian.split(':')[1].strip()
    string = '%s:%s %s/%s/%s' %(H,Mi,M,D,Y)
    return string   #'00:56 04/14/20'

def JULIEN_TO_MJD(JULIEN):
    t = Time(JULIEN, format='iso', scale='utc')
    return t.mjd

def MJD_TO_JULIEN(MJD):
    t = Time(MJD, format='mjd', scale='utc')
    return t.iso

def TIME_TO_MJDS(TIME, offset=0):
    H = TIME.split('T')[1].split(':')[0].strip()
    Mi = TIME.split(':')[1].strip()
    S = TIME.split(':')[2].split('Z')[0].strip()
    SMJHD = int(H)*3600+int(Mi)*60+int(S) + offset
    SMJHD = SMJHD  % 86400
    return SMJHD

def TIME_TO_YYYYMMDD(TIME):
    Y = TIME.split('-')[0].strip()
    Y = Y[2]+Y[3]
    M = TIME.split('-')[1].strip()
    D = TIME.split('-')[2].split('T')[0].strip()
    H = TIME.split('T')[1].split(':')[0].strip()
    Mi = TIME.split(':')[1].strip()
    S = TIME.split(':')[2].split('Z')[0].strip()
    string = '20%s-%s-%sT%s:%s:%s' %(Y, M, D, H, Mi, S)
    return string

def TIME_TO_DYYYYMMDDTHHMM(TIME):
    Y = TIME.split('-')[0].strip()
    Y = Y[2]+Y[3]
    M = TIME.split('-')[1].strip()
    D = TIME.split('-')[2].split('T')[0].strip()
    H = TIME.split('T')[1].split(':')[0].strip()
    Mi = TIME.split(':')[1].strip()
    #TIME_TO_YYYYMMDDS = TIME.split(':')[2].split('Z')[0].strip()
    string = 'D20%s%s%sT%s%s' %(Y, M, D, H, Mi)
    return string

def search_parfile(src_name):
    print(src_name)
    if( src_name == '' ):
        print("ERROR: src_name is empty")
        exit(0)
    PSRJ, PSRB = ATNF.search(src_name,'PSRB')
    print(src_name, PSRJ)
    if os.path.isfile('/ephem/'+PSRJ[0]+'.par'):
        new_src_name = PSRJ[0]
    elif os.path.isfile('/ephem/'+PSRB[0]+'.par'):
        new_src_name = PSRB[0]
    else:
        print("ERROR: cant find parfile /ephem/%s.par or /ephem/%s.par" % (PSRJ[0], PSRB[0]))
        exit(0)
    return new_src_name

#initialisation des list
nlane = 0
Allcommande=[]
AllstartTime=[]
AllstopTime=[]
AlltargetList=[]
AllparametersList=[]
AlltransferList=[]
AllmodeList=[]
AllRAdList=[]
AllDECdList=[]
AllRAjList=[]
AllDECjList=[]
AlllowchanList=[]
AllhighchanList=[]
#recuperation des infos dans le parset
for line in Parsetfile:
    if not re.search("AnaBeam", line):
        if re.search("Observation.topic", line):
            topic = line.split('=')[1].strip().split(' ')[0]
            if(topic[0:2] != 'ES'): topic = default_topic # default topic is default_topic
        if re.search("Observation.name", line):
            observation_name = line.split('=')[1].strip()
        if re.search("Observation.startTime", line):
            observation_start = line.split('=')[1].strip()
            observation_start = datetime.strptime(observation_start, "%Y-%m-%dT%H:%M:%SZ")
        if re.search("Observation.stopTime", line):
            observation_stop = line.split('=')[1].strip()
            observation_stop = datetime.strptime(observation_stop, "%Y-%m-%dT%H:%M:%SZ")
        #if re.search("Observation.stopTime", line):
        #    stopTime = line.split('=')[1].strip()
        ##if re.search("Observation.startTime", line):
        #if re.search("Beam\[0\].startTime", line):
        #    STARTmjds = line.split('=')[1].strip()
        if re.search("Observation.nrBeams", line):
            nBEAM = int(line.split('=')[1].strip())
        if re.search("Output.hd_lane", line):
            nlane += 1

laneperbeam = np.zeros(nBEAM)
beam_lane = np.zeros([nBEAM,nlane])
lanenumber = np.nan*np.zeros([nBEAM,nlane])
beamnumber = np.nan*np.zeros([nBEAM,nlane])
nlane = 0

for BEAM in range(nBEAM):
    AlltargetList = AlltargetList+['NONE']
    AllstartTime = AllstartTime+['NONE']
    AllstopTime = AllstopTime+['NONE']
    AllparametersList = AllparametersList+['NONE']
    AlltransferList = AlltransferList+['']
    AllmodeList = AllmodeList+['NONE']
    AllRAdList = AllRAdList+['NONE']
    AllDECdList = AllDECdList+['NONE']
    AlllowchanList = AlllowchanList+['NONE']
    AllhighchanList = AllhighchanList+['NONE']
    Parsetfile = open(args.INPUT_ARCHIVE[0], "r")
    for line in Parsetfile:
        if re.search('Beam\['+str(BEAM)+'\]', line) and not re.search("AnaBeam", line):
            if re.search("startTime=", line):
                AllstartTime[BEAM] = [line.split('=')[1].strip()]
            if re.search("duration=", line):
                AllstopTime[BEAM] = line.split('=')[1].strip()
                AllstopTime[BEAM] = datetime.strptime(AllstartTime[BEAM][0], "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=int(AllstopTime[BEAM]))
                AllstopTime[BEAM] = [AllstopTime[BEAM].strftime("%Y-%m-%dT%H:%M:%SZ")]

                if( datetime.strptime(AllstartTime[BEAM][0], "%Y-%m-%dT%H:%M:%SZ") < datetime.now() ):
                    print("Old startTime in the parset for BEAM%d %s start now at %s" % (BEAM, AllstartTime[BEAM][0], (datetime.now()+timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")))
                    AllstartTime[BEAM] = [(datetime.now()+timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")]
                else:
                    print("startTime for BEAM%d is set to %s" % (BEAM, AllstartTime[BEAM][0]))
                if( datetime.strptime(AllstopTime[BEAM][0], "%Y-%m-%dT%H:%M:%SZ") < datetime.now() ):
                    print("Old stopTime in the parset for BEAM%d %s stop now at %s" % (BEAM, AllstopTime[BEAM][0], (datetime.now()+timedelta(seconds=660)).strftime("%Y-%m-%dT%H:%M:%SZ")))
                    AllstopTime[BEAM] = [(datetime.now()+timedelta(seconds=660)).strftime("%Y-%m-%dT%H:%M:%SZ")]
                else:
                    print("stopTime for BEAM%d is set to %s" % (BEAM, AllstopTime[BEAM][0]))

            if re.search("target=", line):
                AlltargetList[BEAM] = [line.split('"')[1].split('_')[0].strip()]
            if re.search("parameters=", line):
                if not (line[len(line.strip())-1] == '='): # if parameters is not empti
                    print(line.strip('"'))
                    AllmodeList[BEAM] = [line.split('=')[1].split(':')[0].strip('"').strip()] # ajout d'un strip le 310120
                    AllparametersList[BEAM] = [line.strip('"').split(':')[1].strip().strip('"').lower()]
                    print(AllmodeList[BEAM][0], AllparametersList[BEAM])
                    if re.search("--notransfer", AllparametersList[BEAM][0]) :
                        if (AllmodeList[BEAM][0] == 'FOLD') or (AllmodeList[BEAM][0] == 'SINGLE') or (AllmodeList[BEAM][0] == 'WAVE'):
                            AlltransferList[BEAM] = [' -t']
                            print(AllparametersList[BEAM][0])
                            transfer_search = re.search("--notransfer", AllparametersList[BEAM][0])
                            AllparametersList[BEAM][0] = AllparametersList[BEAM][0][:int(transfer_search.start())]+ AllparametersList[BEAM][0][int(transfer_search.end()):]
                    elif re.search("--fasttransfer", AllparametersList[BEAM][0]) :
                        if (AllmodeList[BEAM][0] == 'FOLD') or (AllmodeList[BEAM][0] == 'SINGLE') or (AllmodeList[BEAM][0] == 'WAVE'):
                            AlltransferList[BEAM] = [' -f']
                            print(AllparametersList[BEAM][0])
                            transfer_search = re.search("--fasttransfer", AllparametersList[BEAM][0])
                            AllparametersList[BEAM][0] = AllparametersList[BEAM][0][:int(transfer_search.start())]+ AllparametersList[BEAM][0][int(transfer_search.end()):]
                    else:
                        AlltransferList[BEAM] = [' ']
                else:
                    AllparametersList[BEAM] = [' ']
                    AlltransferList[BEAM] = [' ']
                    AllmodeList[BEAM] = ['FOLD']
            if re.search("angle1=", line):
                AllRAdList[BEAM] = [line.split('=')[1].strip()]
            if re.search("angle2=", line):
                AllDECdList[BEAM] = [line.split('=')[1].strip()]
            #if re.search("subbandList=", line):
            #    AlllowchanList[BEAM] = [line.split('=')[1].split('[')[1].split('.')[0].strip()]
            #if re.search("subbandList=", line):
            #    AllhighchanList[BEAM] = [line.split('=')[1].split('.')[-1].split(']')[0].strip()]
            if re.search("lane.=", line):
                nlane = int(line.split('=')[0].split('.lane')[1])
                print(int(BEAM), int(nlane))
                beam_lane[int(BEAM), int(nlane)] = 1
                lanenumber[int(BEAM), int(nlane)] = nlane
                beamnumber[int(BEAM), int(nlane)] = BEAM
                laneperbeam[BEAM] += 1
                AlllowchanList[BEAM] = [line.split('=')[1].split('[')[1].split('.')[0].strip()]
                print(AlllowchanList[BEAM])
            if re.search("lane.=", line):
                AllhighchanList[BEAM] = [line.split('=')[1].split('.')[-1].split(']')[0].strip()]

print(beam_lane)
print(lanenumber)
print(beamnumber)
#mise en forme des position RA DEC pour luppi
first_tf = 1
first_psr = 1
for lane in range(nlane+1):
    print("\n-----------------------lane %d beam %d----------" % (lane, BEAM))
    BEAM = np.argmax(beam_lane[:,lane])

    #if(BEAM%2==1):
    #    AllRAjList  = AllRAjList  + ['NONE']
    #    AllDECjList = AllDECjList + ['NONE']
    #    Allcommande = Allcommande + ['NONE']
    #    continue
    #commiBEAM = int(BEAM/2)  # remenber to replace commiBEAM per BEAM for LANEWBAv2
    while(len(AllRAjList) <= BEAM):
        AllRAjList  = AllRAjList  + ['NONE']
        AllDECjList = AllDECjList + ['NONE']
        Allcommande = Allcommande + ['NONE']

    try:
        ra = np.asfarray(AllRAdList,float)[BEAM][0]
        dec = np.asfarray(AllDECdList,float)[BEAM][0]
    except:
        print("WARNING: no ra et dec in the parset_user set to 0.0 and 0.0 (it's only used the header in pulsar obs)")
        ra = 0.0
        dec = 0.0

    c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)

    if(abs(c.ra.hms.s) < 10):
        AllRAjList[BEAM] = ["%02d:%02d:0%.6f" % (int(c.ra.hms.h),int(c.ra.hms.m),c.ra.hms.s)]
    else: 
        AllRAjList[BEAM] = ["%02d:%02d:%.6f" % (int(c.ra.hms.h),int(c.ra.hms.m),c.ra.hms.s)]
    if(c.dec.degree >= 0):
        if(abs(c.dec.dms.s) < 10):
            AllDECjList[BEAM] = ["+%02d:%02d:0%.6f" % (int(c.dec.dms.d),int(c.dec.dms.m),c.dec.dms.s)]
        else: 
            AllDECjList[BEAM] = ["+%02d:%02d:%.6f" % (int(c.dec.dms.d),int(c.dec.dms.m),c.dec.dms.s)]
    else:
        if(abs(c.dec.dms.s) < 10):
            AllDECjList[BEAM] = ["-%02d:%02d:0%.6f" % (abs(int(c.dec.dms.d)),abs(int(c.dec.dms.m)),abs(c.dec.dms.s))]
        else: 
            AllDECjList[BEAM] = ["-%02d:%02d:%.6f" % (abs(int(c.dec.dms.d)),abs(int(c.dec.dms.m)),abs(c.dec.dms.s))]
    
    if(HOSTNAME=='undysputedbk2'):
        if (lane < 2):
            continue
    #elif (BEAM > 1):
    elif (lane >= 2): # should be bk1 or a test machine
            continue
    
    
    
    if (first_psr == 1):
        first_psr = 0
        if not (test):
            completed = subprocess.run(STOPnow, shell=True) # will stop old obs if running
            print('returncode:', completed)

    #-------------------------------projid-parametrisation-------------------------------------------

    #topic
    #--projid
    #AllparametersList[BEAM][0]
    print(AllparametersList[BEAM][0])
    if re.search("--projid=", AllparametersList[BEAM][0]):
        for iparam in range(np.size(AllparametersList[BEAM][0].split())):
            if re.search("--projid=", AllparametersList[BEAM][0].split()[iparam]):
                param_old = AllparametersList[BEAM][0].split()[iparam]
                topic_tmp = AllparametersList[BEAM][0].split()[iparam].split('=')[-1].upper()
                proj_index = iparam
                print('force topic to %s' % (topic_tmp))
        AllparametersList[BEAM][0] = AllparametersList[BEAM][0].replace(param_old,"")
    else:
        topic_tmp = topic
    dirname_databf2 = observation_start.strftime("%Y%m%d_%H%M%S")+observation_stop.strftime("%Y%m%d_%H%M%S")+'_'+observation_name
    path_databf2 = '/databf2/nenufar-pulsar/'+topic_tmp+'/'+observation_start.strftime("%Y/%m")+'/'+dirname_databf2+'/'
    #-------------------------------TF---MODE------------------------------------------
    
    if(AllmodeList[BEAM][0]=='TF'):
         if (first_tf == 1):
             first_tf = 0
             print('-----------------------TF----MODE--BEAM-'+str(BEAM)+'--LANE--'+str(lane)+'----------')

             Allcommande[BEAM]=[TFsetup+" parset=%s" % (
                                args.INPUT_ARCHIVE[0]) ]
             if not (test):
                 completed = subprocess.run(Allcommande[BEAM][0]+' &', shell=True)
             print('subprocess:',Allcommande[BEAM][0] )
    
    #-------------------------------SINGLE---MODE----------------------------------------
    elif(AllmodeList[BEAM][0]=='SINGLE'):
        print('-----------------------SINGLE--PULSE--MODE--BEAM-'+str(BEAM)+'-----LANE--'+str(lane)+'-------')
        Allcommande[BEAM] = [LUPPIsetup+"--ra=%s --dec=%s --lowchan=%s -i --highchan=%s --datadir /data2/ --dataport=%d --rcv_ip=%s --dst_ip=%s --projid=%s --beam=%d --smjdstart=%d --search --jday %s %s" % (
            AllRAjList[BEAM][0], AllDECjList[BEAM][0],
            AlllowchanList[BEAM][0], AllhighchanList[BEAM][0],
            PORT[BEAM], RCV_IP[BEAM], DST_IP[BEAM],
            topic_tmp, BEAM, TIME_TO_MJDS(AllstartTime[BEAM][0], offset=60), TIME_TO_DYYYYMMDDTHHMM(AllstartTime[BEAM][0]), AllparametersList[BEAM][0]) ]
        if not re.search("--src=", AllparametersList[BEAM][0]):
            src_name = search_parfile(AlltargetList[BEAM][0])
            Allcommande[BEAM][0] = [Allcommande[BEAM][0]+" --src=%s" % (src_name)]
        
        print('STOP is set for '+TIME_TO_hhmm_MMDDYY(AllstopTime[BEAM][0], offset=-60))
        if not (test):
            stop_function( AllstopTime[BEAM][0] )
            completed = subprocess.run(Allcommande[BEAM][0], shell=True)
            print('subprocess:', Allcommande[BEAM][0])
            completed = subprocess.run(luppi_daq_dedisp+AlltransferList[BEAM][0]+' -g '+str(int(BEAM))+SHELLfile+str(int(BEAM))+'.log'+' &', shell=True)
            print('returncode:', completed)
        print('subprocess:', Allcommande[BEAM][0])
        print('subprocess:', luppi_daq_dedisp+AlltransferList[BEAM][0]+' -g '+str(int(BEAM))+SHELLfile+str(int(BEAM))+'.log'+' &')


    #-------------------------------WAVEFORM---MODE----------------------------------------
    elif(AllmodeList[BEAM][0]=='WAVE'):
        print('-----------------------WAVEFORM---MODE---BEAM-'+str(BEAM)+'-----LANE--'+str(lane)+'-------')
        if (AllparametersList[BEAM] == 'NONE'): AllparametersList[BEAM] = ['']
        Allcommande[BEAM]=[LUPPIsetup+" --small_blocksize --ra=%s --dec=%s --lowchan=%s -i --highchan=%s --datadir /data2/ --dataport=%d --rcv_ip=%s --dst_ip=%s --projid=%s --beam=%d --smjdstart=%d --jday %s %s" % (AllRAjList[BEAM][0],AllDECjList[BEAM][0],
            AlllowchanList[BEAM][0],AllhighchanList[BEAM][0],
            PORT[BEAM], RCV_IP[BEAM], DST_IP[BEAM],
            topic_tmp, BEAM, TIME_TO_MJDS(AllstartTime[BEAM][0], offset=60), TIME_TO_DYYYYMMDDTHHMM(AllstartTime[BEAM][0]), AllparametersList[BEAM][0]) ]
        if not re.search("--src=", AllparametersList[BEAM][0]):
            src_name = search_parfile(AlltargetList[BEAM][0])
            Allcommande[BEAM][0] = [Allcommande[BEAM][0]+" --src=%s" % (src_name)]
        #completed = subprocess.run(LUPPIsetup+FLAG, shell=True)
        #print('subprocess:', LUPPIsetup+FLAG)7
        #upload des STOP et mise en plase des AT sur bk1 et bk2
        print('STOP is set for '+TIME_TO_hhmm_MMDDYY(AllstopTime[BEAM][0], offset=-60))
        if not (test):
            stop_function( AllstopTime[BEAM][0] )
            completed = subprocess.run(Allcommande[BEAM][0], shell=True)
            print('returncode:', completed)
            completed = subprocess.run(write_raw+AlltransferList[BEAM][0]+' -d -g '+str(int(BEAM))+SHELLfile+str(int(BEAM))+'.log'+' &', shell=True)
        print('subprocess:', Allcommande[BEAM][0])
        print('subprocess:', write_raw+AlltransferList[BEAM][0]+' -d -g '+str(int(BEAM))+SHELLfile+str(int(BEAM))+'.log'+' &')


    #-------------------------------WAVEFORM--OLAF--MODE----------------------------------------
    elif(AllmodeList[BEAM][0]=='WAVEOLAF'):
        print('-----------------------WAVEFORM--OLAF--MODE---BEAM-'+str(BEAM)+'-----LANE--'+str(lane)+'-------')
        
        if (AllparametersList[BEAM] == 'NONE'): AllparametersList[BEAM] = ['']
        
        try:
            src_name = list(filter(re.compile("--src.*").search, AllparametersList[BEAM][0].split(' ')))[0].split('=')[1].upper()
        except IndexError:
            src_name = 'UNKNOWN'

        print(AllparametersList[BEAM][0])
        if re.search("--src=", AllparametersList[BEAM][0]):
            for iparam in range(np.size(AllparametersList[BEAM][0].split())):
                if re.search("--src=", AllparametersList[BEAM][0].split()[iparam]):
                    param_old = AllparametersList[BEAM][0].split()[iparam]
                    src_name = AllparametersList[BEAM][0].split()[iparam].split('=')[-1].upper()
                    print('source name is %s' % (src_name))
            AllparametersList[BEAM][0] = AllparametersList[BEAM][0].replace(param_old,"")
        else:
            src_name = 'UNKNOWN'

        if (topic_tmp != 'ES03'):
            AllparametersList[BEAM][0] = AllparametersList[BEAM][0]+' --databf2path '+path_databf2

        #topic_tmp in /databf2path
        Allcommande[BEAM]=[write_raw_olaf+" -p %d -o /data2/%s --interaddr %s --multiaddr %s --compress --Start %s --End %s %s" % (PORT[BEAM], src_name, RCV_IP[BEAM], DST_IP[BEAM], TIME_TO_YYYYMMDD(AllstartTime[BEAM][0]), TIME_TO_YYYYMMDD(AllstopTime[BEAM][0]), AllparametersList[BEAM][0])]
        
        if not (test):
            completed = subprocess.run(Allcommande[BEAM][0]+SHELLfile+str(int(BEAM))+'.log'+' &', shell=True)
        print(Allcommande[BEAM][0]+SHELLfile+str(int(BEAM))+'.log'+' &')

    #-------------------------------FOLD---MODE----------------------------------------
    elif(AllmodeList[BEAM][0]=='FOLD'):
        print('-----------------------FOLD--MODE--BEAM-'+str(BEAM)+'-------------------')
        if (AllparametersList[BEAM] == 'NONE'): AllparametersList[BEAM] = ['']
        Allcommande[BEAM]=[LUPPIsetup+"--ra=%s --dec=%s --lowchan=%s -i --highchan=%s --datadir /data2/ --dataport=%d --rcv_ip=%s --dst_ip=%s --projid=%s --beam=%d --smjdstart=%d --jday %s %s" % (AllRAjList[BEAM][0],AllDECjList[BEAM][0],
            AlllowchanList[BEAM][0],AllhighchanList[BEAM][0],
            PORT[BEAM], RCV_IP[BEAM], DST_IP[BEAM],
            topic_tmp, BEAM, TIME_TO_MJDS(AllstartTime[BEAM][0], offset=60), TIME_TO_DYYYYMMDDTHHMM(AllstartTime[BEAM][0]), AllparametersList[BEAM][0]) ]
        if not re.search("--src=", AllparametersList[BEAM][0]):
            src_name = search_parfile(AlltargetList[BEAM][0])
            Allcommande[BEAM][0] = [Allcommande[BEAM][0]+" --src=%s" % (src_name)]
        #completed = subprocess.run(LUPPIsetup+FLAG, shell=True)
        #print('subprocess:', LUPPIsetup+FLAG)7
        #upload des STOP et mise en plase des AT sur bk1 et bk2
        print('STOP is set for '+TIME_TO_hhmm_MMDDYY(AllstopTime[BEAM][0], offset=-60))
        if not (test):
            stop_function( AllstopTime[BEAM][0] )
            completed = subprocess.run(Allcommande[BEAM][0], shell=True)
            print('returncode:', completed)
            completed = subprocess.run(luppi_daq_dedisp+AlltransferList[BEAM][0]+' -g '+str(int(BEAM))+SHELLfile+str(int(BEAM))+'.log'+' &', shell=True)
        print('subprocess:', Allcommande[BEAM][0])
        print(AlltransferList[BEAM])
        print('subprocess:', luppi_daq_dedisp+AlltransferList[BEAM][0]+' -g '+str(int(BEAM))+SHELLfile+str(int(BEAM))+'.log'+' &')
    else:
        print('WARNING: mode \''+AllmodeList[BEAM][0]+'\' in BEAM '+str(BEAM)+' is not taken into consideration by undysputed')
