import numpy as np
import sys
import traceback
import os
import re
import socket
import argparse
import time
import os.path
import subprocess
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time, TimeDelta
from datetime import datetime
from datetime import timedelta

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib

# 2020/11/25 modification for the multicast version of dump_udp_ow_12
# 2020/12/04 sync ephem pass from scp to rsync
# 2020/19/05 it is no possible to use diff projid in lanes with param --projid=ES12
# 2020/19/05 option to force undysputedbk3
# now commit messages are on github...

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


HOSTNAME = socket.gethostname()
if(args.bk1):
    HOSTNAME = 'undysputedbk1'
if(args.bk2):
    HOSTNAME = 'undysputedbk2'
if(args.bk3):
    HOSTNAME = 'undysputedbk3'

if(args.testmode):
    test = 1
else:
    test = 0

PORT = [1242, 5586, 1242, 5586]  # PORT = [1490, 5586, 1491, 5587]
RCV_IP = ["192.168.5.100", "192.168.5.100",
          "192.168.5.101", "192.168.5.101"]
DST_IP = ["192.168.5.100", "192.168.5.100",
          "192.168.5.101", "192.168.5.101"]
multicast = True
if(multicast):
    DST_IP = ["224.2.3.1", "224.2.3.2",
              "224.2.3.3", "224.2.3.4"]

default_topic = 'ES03'

mail_error_to = "louis.bondonneau@obs-nancay.fr"


def attach_file(msg, nom_fichier):
    if os.path.isfile(nom_fichier):
        piece = open(nom_fichier, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((piece).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "piece; filename= %s" % os.path.basename(nom_fichier))
        msg.attach(part)


def sendMail(subject, text, files=[]):
    msg = MIMEMultipart()
    msg['From'] = socket.gethostname() + '@obs-nancay.fr'
    msg['To'] = mail_error_to
    msg['Subject'] = subject
    msg.attach(MIMEText(text))
    if (len(files) > 0):
        for ifile in range(len(files)):
            attach_file(msg, files[ifile])
            # print(files[ifile])
    mailserver = smtplib.SMTP('localhost')
    # mailserver.set_debuglevel(1)
    mailserver.sendmail(msg['From'], msg['To'].split(','), msg.as_string())
    mailserver.quit()
    print('Send a mail: \"%s\"" to %s' % (subject, mail_error_to))


def traceback_tomail():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_print = traceback.format_exception(exc_type, exc_value, exc_traceback)
    sendMail(subject="Error while running luppi_presetup on %s" % (HOSTNAME),
             text="\n".join(traceback_print))
    print("\n".join(traceback_print))


def stop_function(stopTime):
    STOPchar = 'echo "python /home/louis/luppi_test_smart/python/luppi_setup.py -X &> /obs/PSR_STOP.log"  > /obs/JOB'
    STOPtoAT = 'sudo at ' + TIME_TO_hhmm_MMDDYY(stopTime, offset=-2) + ' -f /obs/JOB &> /obs/JOB.log'
    completed = subprocess.run(STOPchar, shell=True)
    print('returncode:', completed)
    completed = subprocess.run(STOPtoAT, shell=True)
    print('returncode:', completed)
    print('STOP is set for ' + TIME_TO_hhmm_MMDDYY(stopTime, offset=-2) + '\n' + STOPtoAT)


try:
    DATE = args.INPUT_ARCHIVE[0].split('-')[-1].split('.')[0].rstrip()
    SHELLfile = ' >> /data2/SHELL--at-' + str(DATE) + '-BEAM'

    mjdsnow = 86400
    if not (test):
        # ephem sync with bk1 & bk2
        try:
            # git clone -b main https://forge-osuc.cnrs-orleans.fr/git/pulsar_ephem ephem_test -c http.sslVerify=false
            print("Try to update ephem directory on " + HOSTNAME)
            command = "sudo git pull --rebase"
            completed = subprocess.run(command, cwd="/ephem", shell=True)
            if completed.returncode != 0:
                sendMail(subject="Erreur lors de l'exécution du git pull sur %s" % (HOSTNAME),
                         text="La commande '%s' a échoué avec le code de retour: %d\n" % (command, completed.returncode))
        except Exception as err:
            print(err)
        # ephem sync on databf
        if(HOSTNAME == 'undysputedbk1'):
            print("Try to update ephem directory on /databf/nenufar-pulsar/ES03/ephem")
            command = "ssh nfrplsobs@databfnfrdt \"cd /data/nenufar-pulsar/ES03/ephem && git pull --rebase\""
            completed = subprocess.run(command, shell=True)
            if completed.returncode != 0:
                sendMail(subject="Erreur lors de l'exécution du git pull sur databf",
                         text="La commande '%s' a échoué avec le code de retour: %d\n" % (command, completed.returncode))

    # MJDs NOW
    # mjdsnow = check_output('/home/louis/LUPPI_presetup/getmjdtime | cut -d = -f3 | head -1 | awk \'{print$1}\'', shell=True)
    # print(mjdsnow)
    # mjdsnow = (int(str(mjdsnow).split('\'')[1].split('\\')[0])) % (24*3600)
    # print(mjdsnow)
    time.sleep(1)

    Parsetfile = open(args.INPUT_ARCHIVE[0], "r")
    if not (test):
        completed = subprocess.run('scp ' + args.INPUT_ARCHIVE[0] + '  lbondonneau@nancep3dt:/home/lbondonneau/OBS/', shell=True)
    print('scp ' + args.INPUT_ARCHIVE[0] + '  lbondonneau@nancep3dt:/home/lbondonneau/OBS/')
    STOPnow = 'python /home/louis/luppi_test_smart/python/luppi_setup.py -X'
    STOPchar = 'echo "python /home/louis/luppi_test_smart/python/luppi_setup.py -X &> /obs/PSR_STOP.log"  > /obs/JOB'
    LUPPIsetup = 'python /home/louis/luppi_test_smart/python/luppi_setup.py '
    luppi_daq_dedisp = '/home/louis/luppi_test_smart/luppi_daq_dedisp_GPU1 '

    write_raw = 'sudo -E /home/louis/luppi_test_smart/luppi_write_raw '

    write_raw_olaf = 'sudo /home/louis/olaf_script/dump_udp_ow_12_multicast'

    TFsetup = 'sudo -E /home/cognard/bin/tf '

    def TIME_TO_hhmm_MMDDYY(time_obj, offset=0):
        # Convertir en objet datetime si c'est un Time
        if isinstance(time_obj, Time):
            time_obj = time_obj.datetime

        # Offset is given in seconds and is added to the time
        if (offset != 0):
            time_obj += timedelta(seconds=offset)

        # Use strftime to generate the desired format
        formatted_time = time_obj.strftime('%H:%M %m/%d/%y')
        return formatted_time

    def TIME_TO_MJDS(time_obj, offset=0):
        # Convertir en objet Time si c'est un datetime
        if isinstance(time_obj, datetime):
            time_obj = Time(time_obj)

        # Offset is given in seconds and is added to the time
        if (offset != 0):
            time_obj += TimeDelta(offset, format='sec')

        day_frac = time_obj.mjd % 1  # Fraction of the day
        seconds_in_day = day_frac * 86400.0  # Convert fractional day to seconds
        return int(np.round(seconds_in_day))

    def TIME_TO_YYYYMMDD(time_obj):
        # Convertir en objet datetime si c'est un Time
        if isinstance(time_obj, Time):
            time_obj = time_obj.datetime

        return time_obj.strftime('20%y-%m-%dT%H:%M:%S')

    def TIME_TO_DYYYYMMDDTHHMM(time_obj):
        # Convertir en objet datetime si c'est un Time
        if isinstance(time_obj, Time):
            time_obj = time_obj.datetime

        return "D" + time_obj.strftime('20%y%m%dT%H%M')

    # parset existence check

    def parset_exist(src_name):
        dir_path = '/ephem/'
        target_filename = src_name.upper() + '.PAR'

        # Parcourir les fichiers dans le dir
        for filename in os.listdir(dir_path):
            if filename.upper() == target_filename:
                return os.path.splitext(filename)[0]

        print(os.path.join(dir_path, target_filename))
        return False

    # initialisation des list
    nlane = 0
    Allcommande = []
    AllstartTime = []
    AllstopTime = []
    AlltargetList = []
    AllparametersList = []
    AlltransferList = []
    AllmodeList = []
    AllRAdList = []
    AllDECdList = []
    AllRAjList = []
    AllDECjList = []
    AlllowchanList = []
    AllhighchanList = []
    # recuperation des infos dans le parset
    for line in Parsetfile:
        if not re.search("AnaBeam", line):
            if re.search("Observation.topic", line):
                topic = line.split('=')[1].strip().split(' ')[0]
                # if(topic[0:2] != 'ES'): topic = default_topic # default topic is default_topic
            if re.search("Observation.name", line):
                observation_name = line.split('=')[1].strip().strip("\"")
            if re.search("Observation.startTime", line):
                observation_start = line.split('=')[1].strip()
                observation_start = datetime.strptime(observation_start, "%Y-%m-%dT%H:%M:%SZ")
            if re.search("Observation.stopTime", line):
                observation_stop = line.split('=')[1].strip()
                observation_stop = datetime.strptime(observation_stop, "%Y-%m-%dT%H:%M:%SZ")
            # if re.search("Observation.stopTime", line):
            #    stopTime = line.split('=')[1].strip()
            # if re.search("Observation.startTime", line):
            # if re.search("Beam\[0\].startTime", line):
            #    STARTmjds = line.split('=')[1].strip()
            if re.search("Observation.nrBeams", line):
                nBEAM = int(line.split('=')[1].strip())
            if re.search("Output.hd_lane", line):
                nlane += 1

    laneperbeam = np.zeros(nBEAM)
    beam_lane = np.zeros([nBEAM, nlane])
    lanenumber = np.nan * np.zeros([nBEAM, nlane])
    beamnumber = np.nan * np.zeros([nBEAM, nlane])
    nlane = 0

    for BEAM in range(nBEAM):
        AlltargetList = AlltargetList + ['NONE']
        AllstartTime = AllstartTime + ['NONE']
        AllstopTime = AllstopTime + ['NONE']
        AllparametersList = AllparametersList + ['NONE']
        AlltransferList = AlltransferList + ['']
        AllmodeList = AllmodeList + ['NONE']
        AllRAdList = AllRAdList + ['NONE']
        AllDECdList = AllDECdList + ['NONE']
        AlllowchanList = AlllowchanList + ['NONE']
        AllhighchanList = AllhighchanList + ['NONE']
        Parsetfile.seek(0)
        for line in Parsetfile:
            if re.search('Beam\[' + str(BEAM) + '\]', line) and not re.search("AnaBeam", line):
                if re.search("startTime=", line):
                    AllstartTime[BEAM] = [datetime.strptime(line.split('=')[1].strip(), "%Y-%m-%dT%H:%M:%SZ")]
                if re.search("duration=", line):
                    AllstopTime[BEAM] = line.split('=')[1].strip()
                    AllstopTime[BEAM] = [AllstartTime[BEAM][0] + timedelta(seconds=int(AllstopTime[BEAM]))]

                    if(AllstartTime[BEAM][0] < datetime.now()):
                        print("Old startTime in the parset for BEAM%d %s start now at %s" %
                              (BEAM, AllstartTime[BEAM][0].strftime("%Y-%m-%dT%H:%M:%SZ"), (datetime.now() + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")))
                        AllstartTime[BEAM] = [datetime.now() + timedelta(seconds=60)]
                    else:
                        print("startTime for BEAM%d is set to %s" % (BEAM, AllstartTime[BEAM][0].strftime("%Y-%m-%dT%H:%M:%SZ")))
                    if(AllstopTime[BEAM][0] < datetime.now()):
                        print("Old stopTime in the parset for BEAM%d %s stop now at %s" %
                              (BEAM, AllstopTime[BEAM][0].strftime("%Y-%m-%dT%H:%M:%SZ"), (datetime.now() + timedelta(seconds=660)).strftime("%Y-%m-%dT%H:%M:%SZ")))
                        AllstopTime[BEAM] = [(datetime.now() + timedelta(seconds=660))]
                    else:
                        print("stopTime for BEAM%d is set to %s" % (BEAM, AllstopTime[BEAM][0].strftime("%Y-%m-%dT%H:%M:%SZ")))

                if re.search("target=", line):
                    AlltargetList[BEAM] = [line.split('"')[1].split('_')[0].strip()]
                if re.search("parameters=", line):
                    if not (line[len(line.strip()) - 1] == '='):  # if parameters is not empti
                        print(line.strip('"'))
                        AllmodeList[BEAM] = [line.split('=')[1].split(':')[0].strip('"').strip()]  # ajout d'un strip le 310120
                        AllparametersList[BEAM] = [line.strip('"').split(':')[1].strip().strip('"').lower()]
                        print(AllmodeList[BEAM][0], AllparametersList[BEAM])
                        if re.search("--notransfer", AllparametersList[BEAM][0]):
                            if (AllmodeList[BEAM][0] == 'FOLD') or (AllmodeList[BEAM][0] == 'SINGLE') or (AllmodeList[BEAM][0] == 'WAVE'):
                                AlltransferList[BEAM] = [' -t']
                                print(AllparametersList[BEAM][0])
                                transfer_search = re.search("--notransfer", AllparametersList[BEAM][0])
                                AllparametersList[BEAM][0] = AllparametersList[BEAM][0][:int(
                                    transfer_search.start())] + AllparametersList[BEAM][0][int(transfer_search.end()):]
                        elif re.search("--fasttransfer", AllparametersList[BEAM][0]):
                            if (AllmodeList[BEAM][0] == 'FOLD') or (AllmodeList[BEAM][0] == 'SINGLE') or (AllmodeList[BEAM][0] == 'WAVE'):
                                AlltransferList[BEAM] = [' -f']
                                print(AllparametersList[BEAM][0])
                                transfer_search = re.search("--fasttransfer", AllparametersList[BEAM][0])
                                AllparametersList[BEAM][0] = AllparametersList[BEAM][0][:int(
                                    transfer_search.start())] + AllparametersList[BEAM][0][int(transfer_search.end()):]
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
                # if re.search("subbandList=", line):
                #    AlllowchanList[BEAM] = [line.split('=')[1].split('[')[1].split('.')[0].strip()]
                # if re.search("subbandList=", line):
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

    Parsetfile.close()
    print(beam_lane)
    print(lanenumber)
    print(beamnumber)
    # mise en forme des position RA DEC pour luppi
    first_tf = 1
    first_psr = 1
    for lane in range(nlane + 1):
        print("\n-----------------------lane %d beam %d----------" % (lane, BEAM))
        BEAM = np.argmax(beam_lane[:, lane])

        # if(BEAM%2==1):
        #    AllRAjList  = AllRAjList  + ['NONE']
        #    AllDECjList = AllDECjList + ['NONE']
        #    Allcommande = Allcommande + ['NONE']
        #    continue
        # commiBEAM = int(BEAM/2)  # remenber to replace commiBEAM per BEAM for LANEWBAv2
        while(len(AllRAjList) <= BEAM):
            AllRAjList = AllRAjList + ['NONE']
            AllDECjList = AllDECjList + ['NONE']
            Allcommande = Allcommande + ['NONE']

        try:
            ra = np.asfarray(AllRAdList, float)[BEAM][0]
            dec = np.asfarray(AllDECdList, float)[BEAM][0]
        except:
            print("WARNING: no ra et dec in the parset_user set to 0.0 and 0.0 (it's only used the header in pulsar obs)")
            ra = 0.0
            dec = 0.0

        c = SkyCoord(ra=ra * u.degree, dec=dec * u.degree)

        if(abs(c.ra.hms.s) < 10):
            AllRAjList[BEAM] = ["%02d:%02d:0%.6f" % (int(c.ra.hms.h), int(c.ra.hms.m), c.ra.hms.s)]
        else:
            AllRAjList[BEAM] = ["%02d:%02d:%.6f" % (int(c.ra.hms.h), int(c.ra.hms.m), c.ra.hms.s)]
        if(c.dec.degree >= 0):
            if(abs(c.dec.dms.s) < 10):
                AllDECjList[BEAM] = ["+%02d:%02d:0%.6f" % (int(c.dec.dms.d), int(c.dec.dms.m), c.dec.dms.s)]
            else:
                AllDECjList[BEAM] = ["+%02d:%02d:%.6f" % (int(c.dec.dms.d), int(c.dec.dms.m), c.dec.dms.s)]
        else:
            if(abs(c.dec.dms.s) < 10):
                AllDECjList[BEAM] = ["-%02d:%02d:0%.6f" % (abs(int(c.dec.dms.d)), abs(int(c.dec.dms.m)), abs(c.dec.dms.s))]
            else:
                AllDECjList[BEAM] = ["-%02d:%02d:%.6f" % (abs(int(c.dec.dms.d)), abs(int(c.dec.dms.m)), abs(c.dec.dms.s))]

        if(HOSTNAME == 'undysputedbk2'):
            if (lane < 2):
                continue
        # elif (BEAM > 1):
        elif (lane >= 2):  # should be bk1 or a test machine
            continue

        if (first_psr == 1):
            first_psr = 0
            if not (test):
                completed = subprocess.run(STOPnow, shell=True)  # will stop old obs if running
                print('returncode:', completed)

        # -------------------------------projid-parametrisation-------------------------------------------

        # topic
        # --projid
        # AllparametersList[BEAM][0]
        print(AllparametersList[BEAM][0])
        if re.search("--projid=", AllparametersList[BEAM][0]):
            for iparam in range(np.size(AllparametersList[BEAM][0].split())):
                if re.search("--projid=", AllparametersList[BEAM][0].split()[iparam]):
                    param_old = AllparametersList[BEAM][0].split()[iparam]
                    topic_tmp = AllparametersList[BEAM][0].split()[iparam].split('=')[-1].upper()
                    proj_index = iparam
                    print('force topic to %s' % (topic_tmp))
            AllparametersList[BEAM][0] = AllparametersList[BEAM][0].replace(param_old, "")
        else:
            topic_tmp = topic
        dirname_databf2 = observation_start.strftime("%Y%m%d_%H%M%S") + observation_stop.strftime("%Y%m%d_%H%M%S") + '_' + observation_name
        path_databf2 = '/data/nenufar-pulsar/' + topic_tmp + '/' + observation_start.strftime("%Y/%m") + '/' + dirname_databf2 + '/L0/'

        # -------------------------------TF---MODE------------------------------------------
        if(AllmodeList[BEAM][0] == 'TF'):
            if (first_tf == 1):
                first_tf = 0
                print('-----------------------TF----MODE--BEAM-' + str(BEAM) + '--LANE--' + str(lane) + '----------')

                Allcommande[BEAM] = [TFsetup + " parset=%s" % (
                    args.INPUT_ARCHIVE[0])]
                if not (test):
                    completed = subprocess.run(Allcommande[BEAM][0] + ' &', shell=True)
                print('subprocess:', Allcommande[BEAM][0])

        # -------------------------------SINGLE---MODE----------------------------------------
        elif(AllmodeList[BEAM][0] == 'SINGLE'):
            print('-----------------------SINGLE--PULSE--MODE--BEAM-' + str(BEAM) + '-----LANE--' + str(lane) + '-------')
            Allcommande[BEAM] = [LUPPIsetup + "--ra=%s --dec=%s --lowchan=%s -i --highchan=%s --datadir /data2/ --dataport=%d --rcv_ip=%s --dst_ip=%s --projid=%s --beam=%d --smjdstart=%d --search --jday %s %s" % (
                AllRAjList[BEAM][0], AllDECjList[BEAM][0],
                AlllowchanList[BEAM][0], AllhighchanList[BEAM][0],
                PORT[lane], RCV_IP[lane], DST_IP[lane],
                topic_tmp, BEAM, TIME_TO_MJDS(AllstartTime[BEAM][0], offset=60), TIME_TO_DYYYYMMDDTHHMM(AllstartTime[BEAM][0]), AllparametersList[BEAM][0])]
            if not re.search("--src=", AllparametersList[BEAM][0]):
                src_name = parset_exist(AlltargetList[BEAM][0])
                if (src_name == False):
                    src_name = AlltargetList[BEAM][0]
                    # TODO: send an error mail
                    sendMail(subject="No parfile for %s durring SINGLE %s in beam %d for %s" % (src_name, observation_name, BEAM, topic),
                             text="No parfile for %s durring SINGLE %s in beam %d for %s\n" % (src_name, observation_name, BEAM, topic),
                             files=[args.INPUT_ARCHIVE[0]])
                Allcommande[BEAM][0] = [Allcommande[BEAM][0] + " --src=%s" % (src_name)]
            else:
                src_name = parset_exist(re.search(r'--src=([^ ]+)', AllparametersList[BEAM][0]).group(1))
                if (src_name == False):
                    src_name = re.search(r'--src=([^ ]+)', AllparametersList[BEAM][0]).group(1)
                    # TODO: send an error mail
                    sendMail(subject="parfile do not exist for %s durring SINGLE %s in beam %d for %s" % (src_name, observation_name, BEAM, topic),
                             text="parfile do not exist for %s durring SINGLE %s in beam %d for %s\n" % (src_name, observation_name, BEAM, topic),
                             files=[args.INPUT_ARCHIVE[0]])

            print('STOP is set for ' + TIME_TO_hhmm_MMDDYY(AllstopTime[BEAM][0], offset=-60))
            if not (test):
                stop_function(AllstopTime[BEAM][0])
                completed = subprocess.run(Allcommande[BEAM][0], shell=True)
                print('subprocess:', Allcommande[BEAM][0])
                completed = subprocess.run(luppi_daq_dedisp + AlltransferList[BEAM][0] + " --databfdirname " + dirname_databf2 + ' -g ' +
                                           str(int(BEAM)) + SHELLfile + str(int(BEAM)) + '.log' + ' &', shell=True)
                print('returncode:', completed)
            print('subprocess:', Allcommande[BEAM][0])
            print('subprocess:', luppi_daq_dedisp + AlltransferList[BEAM][0] + " --databfdirname " + dirname_databf2 + ' -g ' + str(int(BEAM)) + SHELLfile + str(int(BEAM)) + '.log' + ' &')

        # -------------------------------WAVEFORM---MODE----------------------------------------
        elif(AllmodeList[BEAM][0] == 'WAVE'):
            print('-----------------------WAVEFORM---MODE---BEAM-' + str(BEAM) + '-----LANE--' + str(lane) + '-------')
            if (AllparametersList[BEAM] == 'NONE'):
                AllparametersList[BEAM] = ['']
            Allcommande[BEAM] = [LUPPIsetup + " --small_blocksize --ra=%s --dec=%s --lowchan=%s -i --highchan=%s --datadir /data2/ --dataport=%d --rcv_ip=%s --dst_ip=%s --projid=%s --beam=%d --smjdstart=%d --jday %s %s" % (
                AllRAjList[BEAM][0], AllDECjList[BEAM][0],
                AlllowchanList[BEAM][0], AllhighchanList[BEAM][0],
                PORT[lane], RCV_IP[lane], DST_IP[lane],
                topic_tmp, BEAM, TIME_TO_MJDS(AllstartTime[BEAM][0], offset=60), TIME_TO_DYYYYMMDDTHHMM(AllstartTime[BEAM][0]), AllparametersList[BEAM][0])]
            if not re.search("--src=", AllparametersList[BEAM][0]):
                src_name = parset_exist(AlltargetList[BEAM][0])
                if (src_name == False):
                    src_name = AlltargetList[BEAM][0]
                    sendMail(subject="No parfile for %s durring WAVE %s in beam %d for %s" % (src_name, observation_name, BEAM, topic),
                             text="No parfile for %s durring WAVE %s in beam %d for %s\n" % (src_name, observation_name, BEAM, topic),
                             files=[args.INPUT_ARCHIVE[0]])
                Allcommande[BEAM][0] = [Allcommande[BEAM][0] + " --src=%s" % (src_name)]
            else:
                src_name = parset_exist(re.search(r'--src=([^ ]+)', AllparametersList[BEAM][0]).group(1))
                if (src_name == False):
                    src_name = re.search(r'--src=([^ ]+)', AllparametersList[BEAM][0]).group(1)
                    sendMail(subject="parfile do not exist for %s durring WAVE %s in beam %d for %s" % (src_name, observation_name, BEAM, topic),
                             text="parfile do not exist for %s durring WAVE %s in beam %d for %s\n" % (src_name, observation_name, BEAM, topic),
                             files=[args.INPUT_ARCHIVE[0]])
            #completed = subprocess.run(LUPPIsetup+FLAG, shell=True)
            # print('subprocess:', LUPPIsetup+FLAG)7
            # upload des STOP et mise en plase des AT sur bk1 et bk2
            print('STOP is set for ' + TIME_TO_hhmm_MMDDYY(AllstopTime[BEAM][0], offset=-60))
            if not (test):
                stop_function(AllstopTime[BEAM][0])
                completed = subprocess.run(Allcommande[BEAM][0], shell=True)
                print('returncode:', completed)
                completed = subprocess.run(write_raw + AlltransferList[BEAM][0] + " --databfdirname " + dirname_databf2 + ' -d -g ' +
                                           str(int(BEAM)) + SHELLfile + str(int(BEAM)) + '.log' + ' &', shell=True)
            print('subprocess:', Allcommande[BEAM][0])
            print('subprocess:', write_raw + AlltransferList[BEAM][0] + " --databfdirname " + dirname_databf2 + ' -d -g ' + str(int(BEAM)) + SHELLfile + str(int(BEAM)) + '.log' + ' &')

        # -------------------------------WAVEFORM--OLAF--MODE----------------------------------------
        elif(AllmodeList[BEAM][0] == 'WAVEOLAF'):
            print('-----------------------WAVEFORM--OLAF--MODE---BEAM-' + str(BEAM) + '-----LANE--' + str(lane) + '-------')

            if (AllparametersList[BEAM] == 'NONE'):
                AllparametersList[BEAM] = ['']

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
                AllparametersList[BEAM][0] = AllparametersList[BEAM][0].replace(param_old, "")
            else:
                src_name = 'UNKNOWN'

            AllparametersList[BEAM][0] = AllparametersList[BEAM][0] + ' --databf2path ' + path_databf2

            # topic_tmp in /databf2path
            Allcommande[BEAM] = [write_raw_olaf + " -p %d -o /data2/%s --interaddr %s --multiaddr %s --compress --Start %s --End %s %s" %
                                 (PORT[lane], src_name, RCV_IP[lane], DST_IP[lane], TIME_TO_YYYYMMDD(AllstartTime[BEAM][0]), TIME_TO_YYYYMMDD(AllstopTime[BEAM][0]), AllparametersList[BEAM][0])]

            if not (test):
                completed = subprocess.run(Allcommande[BEAM][0] + SHELLfile + str(int(BEAM)) + '.log' + ' &', shell=True)
            print(Allcommande[BEAM][0] + SHELLfile + str(int(BEAM)) + '.log' + ' &')

        # -------------------------------FOLD---MODE----------------------------------------
        elif(AllmodeList[BEAM][0] == 'FOLD'):
            print('-----------------------FOLD--MODE--BEAM-' + str(BEAM) + '-------------------')
            if (AllparametersList[BEAM] == 'NONE'):
                AllparametersList[BEAM] = ['']
            Allcommande[BEAM] = [LUPPIsetup + "--ra=%s --dec=%s --lowchan=%s -i --highchan=%s --datadir /data2/ --dataport=%d --rcv_ip=%s --dst_ip=%s --projid=%s --beam=%d --smjdstart=%d --jday %s %s" % (AllRAjList[BEAM][0], AllDECjList[BEAM][0],
                                                                                                                                                                                                            AlllowchanList[BEAM][
                                                                                                                                                                                                                0], AllhighchanList[BEAM][0],
                                                                                                                                                                                                            PORT[lane], RCV_IP[
                                                                                                                                                                                                                lane], DST_IP[lane],
                                                                                                                                                                                                            topic_tmp, BEAM, TIME_TO_MJDS(AllstartTime[BEAM][0], offset=60), TIME_TO_DYYYYMMDDTHHMM(AllstartTime[BEAM][0]), AllparametersList[BEAM][0])]
            if not re.search("--src=", AllparametersList[BEAM][0]):
                src_name = parset_exist(AlltargetList[BEAM][0])
                if (src_name == False):
                    src_name = AlltargetList[BEAM][0]
                    sendMail(subject="No parfile for %s durring FOLD %s in beam %d for %s" % (src_name, observation_name, BEAM, topic),
                             text="No parfile for %s durring FOLD %s in beam %d for %s\n" % (src_name, observation_name, BEAM, topic),
                             files=[args.INPUT_ARCHIVE[0]])
                Allcommande[BEAM][0] = [Allcommande[BEAM][0] + " --src=%s" % (src_name)]
            else:
                src_name = parset_exist(re.search(r'--src=([^ ]+)', AllparametersList[BEAM][0]).group(1))
                if (src_name == False):
                    src_name = re.search(r'--src=([^ ]+)', AllparametersList[BEAM][0]).group(1)
                    sendMail(subject="parfile do not exist for %s durring FOLD %s in beam %d for %s" % (src_name, observation_name, BEAM, topic),
                             text="parfile do not exist for %s durring FOLD %s in beam %d for %s\n" % (src_name, observation_name, BEAM, topic),
                             files=[args.INPUT_ARCHIVE[0]])
            # completed = subprocess.run(LUPPIsetup+FLAG, shell=True)
            # print('subprocess:', LUPPIsetup+FLAG)7
            # upload des STOP et mise en plase des AT sur bk1 et bk2
            print('STOP is set for ' + TIME_TO_hhmm_MMDDYY(AllstopTime[BEAM][0], offset=-60))
            if not (test):
                stop_function(AllstopTime[BEAM][0])
                completed = subprocess.run(Allcommande[BEAM][0], shell=True)
                print('returncode:', completed)
                completed = subprocess.run(luppi_daq_dedisp + AlltransferList[BEAM][0] + " --databfdirname " + dirname_databf2 + ' -g ' +
                                           str(int(BEAM)) + SHELLfile + str(int(BEAM)) + '.log' + ' &', shell=True)
            print('subprocess:', Allcommande[BEAM][0])
            print(AlltransferList[BEAM])
            print('subprocess:', luppi_daq_dedisp + AlltransferList[BEAM][0] + " --databfdirname " + dirname_databf2 + ' -g ' + str(int(BEAM)) + SHELLfile + str(int(BEAM)) + '.log' + ' &')
        else:
            print('WARNING: mode \'' + AllmodeList[BEAM][0] + '\' in BEAM ' + str(BEAM) + ' is not taken into consideration by undysputed')
except:
    traceback_tomail()
    raise RuntimeError("Une erreur inconnue s'est produite")
