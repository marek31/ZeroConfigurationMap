@echo off
start "" /LOW /B python zcm.py
python -c "import time; time.sleep(3)"
title Workerpool - Zero Configuration Map
