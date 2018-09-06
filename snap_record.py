#!/usr/bin/python
import os
import argparse
import casperfpga
import adc5g
import time
import numpy as np
import matplotlib.pyplot as plt
import struct
import cPickle as pkl
from ata_snap import ata_control

parser = argparse.ArgumentParser(description='Plot ADC Histograms and Spectra',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('host', type=str,
                    help = 'Hostname / IP of SNAP')
parser.add_argument('fpgfile', type=str,
                    help = '.fpgfile to program')
parser.add_argument('-s', dest='srate', type=float, default=900.0,
                    help ='Sample rate in MHz for non-interleaved band. Used for spectrum axis scales')
parser.add_argument('-n', dest='ncaptures', type=int, default=16,
                    help ='Number of data captures (for each correlation product)')
parser.add_argument('-r', dest='rfc', type=float, default=0.0,
                    help ='RF centre frequency in MHz. 0 => Grab the frequency from the ATA control system')
parser.add_argument('-i', dest='ifc', type=float, default=629.1452,
                    help ='IF centre frequency in MHz')
parser.add_argument('-c', dest='comment', type=str, default="",
                    help ='comment to be appended at the end of the filename (eg, a source name)')
parser.add_argument('-p', dest='path', type=str, default="~/data",
                    help ='Directory in which to record data')
parser.add_argument('-a', dest='ant', type=str, default=None,
                    help ='ATA Antenna string (used for getting monitoring data')
parser.add_argument('-t', dest='target_rms', type=float, default=None,
        help ='Target RMS to achieve by tweaking USB attenuators. Default: Do not tune')

args = parser.parse_args()
out = vars(args).copy()

if args.rfc == 0.0:
    print "Reading Sky center frequency from the ATA control system"
    out["rfc"] = ata_control.get_sky_freq()
    print "Frequency is %.1f MHz" % out["rfc"]

print "Trying to get ATA status information"
try:
    out['ata_status'] = ata_control.get_ascii_status()
    print "Succeeded -- status will be written into the output file"
except:
    print "!!!!!!!!!!!!!!!!!!!!!!!!"
    print "!!!!!!   Failed   !!!!!!"
    print "!!!!!!!!!!!!!!!!!!!!!!!!"

if args.ant is not None:
    ata_control.get_pam_status(args.ant)


datadir = os.path.expanduser(args.path)

if not os.path.isdir(datadir):
    print "Chosen data directory: %s does not exist. Create it and run this script again!" % datadir
    exit()

filename = os.path.join(datadir, "%d_rf%.2f_n%d_%s.pkl" % (time.time(), out['rfc'], args.ncaptures, args.comment))
print "Output filename is %s" % filename

print "Using RF center frequency of %.2f" % out['rfc']
print "Using IF center frequency of %.2f" % args.ifc

print "Connecting to %s" % args.host
snap = casperfpga.CasperFpga(args.host)
print "Interpretting design data for %s with %s" % (args.host, args.fpgfile)
snap.get_system_information(args.fpgfile)


print "Figuring out accumulation length"
acc_len = float(snap.read_int('timebase_sync_period') / (4096 / 4))
print "Accumulation length is %f" % acc_len

print "Estimating FPGA clock"
fpga_clk = snap.estimate_fpga_clock()
out['fpga_clk'] = fpga_clk
print "Clock estimate is %.1f" % fpga_clk
print "args.srate = %.1f" % args.srate
assert np.abs((fpga_clk*4. / args.srate) - 1) < 0.01

mux_sel = {'auto':0, 'cross':1}

if args.target_rms is not None:
    print "Trying to tune power levels to RMS: %.2f" % args.target_rms
    max_attempts = 5
    num_snaps = 5
    atteni = 0
    attenq = 0
    try:
        for attempt in range(max_attempts):
            ata_control.set_atten_by_ant(args.ant + "x", atteni)
            ata_control.set_atten_by_ant(args.ant + "y", attenq)
            # Store attenuation values used
            out['attenx'] = atteni
            out['atteny'] = attenq
            chani = []
            chanq = []
            for i in range(num_snaps):
                all_chan_data = adc5g.get_snapshot(snap, 'ss_adc')
                chani += [all_chan_data[0::2][0::2]]
                chanq += [all_chan_data[1::2][0::2]]
            chani = np.array(chani)
            chanq = np.array(chanq)

            print "Channel I ADC mean/std-dev: %.2f / %.2f" % (chani.mean(), chani.std())
            print "Channel Q ADC mean/std-dev: %.2f / %.2f" % (chanq.mean(), chanq.std())
        
            delta_atteni = 20*np.log10(chani.std() / args.target_rms)
            delta_attenq = 20*np.log10(chanq.std() / args.target_rms)
        
            if (delta_atteni < 1) and (delta_attenq < 1):
                print "Tuning complete"
                break
            else:
                # Attenuator has 0.25dB precision
                atteni = int(4 * (atteni + delta_atteni)) / 4.0
                attenq = int(4 * (attenq + delta_attenq)) / 4.0
                if atteni > 30:
                    atteni = 30
                if attenq > 30:
                    attenq = 30
                print "New X-attenuation: %.3f" % atteni
                print "New Y-attenuation: %.3f" % attenq
    except:
        # For some reason the Attenuation setting routine failed.
        # Use -1 attenuation values to indicate this so that data files
        # can be flagged.
        print "Attenuator tuning failed!"
        out['attenx'] = -1
        out['atteny'] = -1

print "Grabbing ADC statistics to write to file"
adc0 = []
adc1 = []
for i in range(10):
    all_chan_data = adc5g.get_snapshot(snap, 'ss_adc')
    adc0 += [all_chan_data[0::2][0::2]]
    adc1 += [all_chan_data[1::2][0::2]]

adc0 = np.array(adc0)
adc1 = np.array(adc1)

out["adc0_bitsnaps"] = adc0
out["adc1_bitsnaps"] = adc1
out["adc0_stats"] = {"mean": adc0.mean(), "dev": adc0.std()}
out["adc1_stats"] = {"mean": adc1.mean(), "dev": adc1.std()}

print "ADC0 mean/dev: %.2f / %.2f" % (out["adc0_stats"]["mean"], out["adc0_stats"]["dev"])
print "ADC1 mean/dev: %.2f / %.2f" % (out["adc1_stats"]["mean"], out["adc1_stats"]["dev"])

out['fft_shift'] = snap.read_int('fft_shift')
if args.ant is not None:
    try:
        out['pam_stats'] = ata_control.get_pam_status(args.ant)
    except:
        pass

ants = ['auto']
out['auto0'] = []
out['auto0_timestamp'] = []
out['auto0_of_count'] = []
out['fft_of0'] = []
out['auto1'] = []
out['auto1_timestamp'] = []
out['auto1_of_count'] = []
out['fft_of1'] = []

for i in range(args.ncaptures):
    for ant in ants:
        print "Setting snapshot select to %s (%d)" % (ant, mux_sel[ant])
        snap.write_int('vacc_ss_sel', mux_sel[ant])
        print "Grabbing data (%d of %d)" % (i+1, args.ncaptures)
        x,t = snap.snapshots.vacc_ss_ss.read_raw()
        d = np.array(struct.unpack('>%dl' % (x['length']/4), x['data'])) / acc_len
        frange = np.linspace(out['rfc'] - (args.srate - args.ifc), out['rfc'] - (args.srate - args.ifc) + args.srate/2., d.shape[0])
        out['frange'] = frange
        out['auto0'] += [d[0::2]]
        out['auto0_timestamp'] += [t]
        out['auto0_of_count'] += [snap.read_int('power_vacc0_of_count')]
        out['fft_of0'] += [snap.read_int('fft_of')]
        out['auto1'] += [d[1::2]]
        out['auto1_timestamp'] += [t]
        out['auto1_of_count'] += [snap.read_int('power_vacc1_of_count')]
        out['fft_of1'] += [snap.read_int('fft_of')]

print "Dumping data to %s" % filename
pkl.dump(out, open(filename, 'w'))
