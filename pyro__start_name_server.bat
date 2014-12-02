@echo off
set PYRO_SERIALIZER=pickle
set PYRO_SERIALIZERS_ACCEPTED=pickle,json,marshal,serpent
title Pyro Name Server
python -m Pyro4.naming -n %computername%
