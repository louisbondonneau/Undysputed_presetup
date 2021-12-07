import numpy as np

import astropy.coordinates as coord
from astropy import units as u
from astropy.coordinates import SkyCoord, AltAz
from astropy.time import Time
import argparse
import os, re
import string as trans



def searchPSR(NAME):
    NAME = NAME.replace("+","\+")
    NAME = NAME.replace("-","\-")
    NAME = NAME.replace(":","\:")
    with open("/home/louis/LUPPI_presetup/psrcat.db","r") as database:
        lines = database.readlines()
        ALLPSR = []
        for i in range(0, len(lines)-1):
            line = lines[i]
            if re.search('@', line) or re.search('#', line):
                line = lines[i+1]
                if not re.search('#', line):
                    PSR = []
                    while not re.search('@', line):
                        line = line.replace("\n","")
                        PSR.append([line[0:8].strip(" "), line[9:9+24].strip(" ").replace("E","e").replace("D","e"), line[9+25:9+25+5].strip(" ")])
                        line = lines[i+1]
                        i += 1
                    if re.search(NAME, ''.join(str(e[1]) for e in PSR[:])): ALLPSR.append(PSR)
    return(ALLPSR)

def search(NAME, ARG):
    result = searchPSR(NAME)
    shape = np.shape(result)
    JNAME = []
    VAL = []
    ERR = []
    for i in range(shape[0]):
        one = 1
        VAL.append('')
        ERR.append('')
        JNAME.append('')
        for j in (result[i])[:]:
            if( j[0] == "PSRJ"):
                JNAME[i] = j[1]
            if( j[0] == ARG):
                VAL[i] = j[1]
                ERR[i] = j[2]
    return JNAME, VAL
