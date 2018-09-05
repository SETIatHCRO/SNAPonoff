# SNAPonoff

This program was originally called snap_observation_run.py written by 
Jack Hicish. It has been modified by Jon Richards to handle multiple RF 
switches to work in unison. It also determines which source is up and
which frequenies to observe. The goal is to make a program that can be
continually running, even when available sources are not up (it sleeps).

The entry point is snap_obs.py. This is modified severely from Jack
Hickish's wonderful snap_observation_run.py

A MySQL database is now use to record what ant pols have been observed 
on a particular source, and the frequency. Next time that source is up
the observing of that source picks up from where it left off.

The RF switch and attenuator origrams have been modifies to be a lot
faster, and their execution is threaded so as to not slow down the
observing.

This is very much a work in progress.

