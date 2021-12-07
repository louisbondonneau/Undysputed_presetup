luppi_presetup aims is to read the current.parset file to launche the appropriate executable with his arguments in function of the requesting mode and parrameters.

FOLD or SINGLE /home/louis/luppi_test_smart/python/luppi_setup.py (SHM initialisation)
               /home/louis/luppi_test_smart/luppi_daq_dedisp_GPU1 (real time pipeline)

TF   /home/cognard/bin/tf

WAVE /home/louis/luppi_test_smart/python/luppi_setup.py (SHM initialisation)
     /home/louis/luppi_test_smart/luppi_write_raw (real time pipeline)

WAVEOLAF  home/louis/olaf_script/dump_udp_ow_12_multicast


#####  HOW TEST IT

It can be test without launching any executable with option -test
python luppi_presetup.py -test my_test_parset.parset

Their behaviour on different machines can be forced with the options -bk1 -bk2 -bk3
python luppi_presetup.py -test -bk2 my_test_parset.parset







start_20211201.bashrc is the starting script in /home/cognard/start.script (same for undysp 1, 2 & 3)