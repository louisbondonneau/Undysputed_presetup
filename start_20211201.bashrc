. /root/.bashrc

sleep 30

DATE=$(date +%Y%m%d_%H%M%S)
START="/data2/PSETUP-at-$DATE.log"
PARSET="/data2/PARSET-at-$DATE.parset"
STARTERROR="/data2/PSETUP-at-$DATE.error"

ls -lhtr ~nenufarobs/parset/ > $START

if [ -f ~nenufarobs/parset/current.parset ]
then
    cp ~nenufarobs/parset/current.parset $PARSET
    rm ~nenufarobs/parset/current.parset
    echo "START RECIVED" >> $START
    python3 /home/louis/LUPPI_presetup/luppi_presetup.py  $PARSET >> $START 2>$STARTERROR
else
echo "ERROR ~nenufarobs/parset/current.parset unreachable" >> "$START"
fi
