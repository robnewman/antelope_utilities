#!/usr/bin/env python

'''
CLI Python script to update endtimes to rrdcache tables

@package   Datascope 
@author    Rob Newman <robertlnewman@gmail.com>
@notes     Rewrite from PHP to Python
           Use parameter file for constants
'''

import sys
import os
import datetime
from time import time, gmtime, strftime
from optparse import OptionParser

# Load datascope functions
sys.path.append(os.environ['ANTELOPE'] + '/local/data/python/antelope')
import datascope
from stock import pfupdate, pfget

usage = "Usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("-v", action="store_true", dest="verbose", help="verbose output", default=False)
(options, args) = parser.parse_args()
if options.verbose:
    verbose = True
else:
    verbose = False

common_pf = "common.pf"
pfupdate(common_pf)
decom_dict = {}
dbnulls_dict = {}
dbmaster_fields = ['time', 'endtime', 'cert_time', 'decert_time']

rrddb = pfget(common_pf, 'RRDTOOL_DB_ACTIVE')
rrddb_im  = pfget(common_pf, 'RRDTOOL_DB_IM_ACTIVE')
rrddb_vtw = pfget(common_pf, 'RRDTOOL_DB_VTW_ACTIVE')
rrd_pointers = {
    'dataloggers':rrddb,
    'communications':rrddb_im,
    'opto':rrddb_vtw
}
dbmaster = pfget(common_pf, 'USARRAY_DBMASTER')

def get_nulls():
    """Return all the null
    values for the defined fields
    """
    nulls_dict = {}
    db = datascope.dbopen(dbmaster, 'r')
    db.lookup('', 'deployment', '', '')
    for f in dbmaster_fields:
        db.lookup('', '', f, 'dbNULL')
        nulls_dict[f] = db.getv(f)[0]
    return nulls_dict

def get_decom_stations():
    """Return a list of all
    decommissioned stations
    """
    db = datascope.dbopen(dbmaster, 'r')
    db.lookup('', 'deployment', '', '')
    db.subset('snet=~/TA/')
    db.sort(['sta','time'])
    db.group('sta')
    stas = group_loop(db)
    return stas

def group_loop(dbptr):
    """Loop over dbgroups
    and return station dictionary
    with field values
    """
    sta_dict = {}
    for i in range(dbptr.query('dbRECORD_COUNT')):
        dbptr[3] = i
        sta_sub = dbptr.getv('sta')[0]
        dbmg_sub = datascope.dbsubset(dbptr, 'sta=~/%s/' % sta_sub)
        dbmg_sub.ungroup()
        '''
        EARN stations will have more than one deployment record
        Test below will always get the most recent 
        record, provided we sorted on time earlier
        '''
        if dbmg_sub.query('dbRECORD_COUNT') > 1:
            row_pointer = dbmg_sub.query('dbRECORD_COUNT') - 1
            dbmg_sub[3] = row_pointer
        else:
            dbmg_sub[3] = 0

        if dbmg_sub.getv('endtime')[0] < time():
            sta_dict[sta_sub] = {}
            for f in dbmaster_fields:
                sta_dict[sta_sub][f] = dbmg_sub.getv(f)[0]
    return sta_dict

def main():
    """Process all decommissioned 
    stations
    """
    if verbose:
        print "Start updating RRD archive at %s" % strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        print "ACTIVE DATALOGGER RRD DB: %s" % rrddb
        print "ACTIVE COMMS RRD DB: %s" % rrddb_im
        print "ACTIVE VTW RRD DB: %s" % rrddb_vtw
        print "DBMASTER: %s" % dbmaster
        print "Get Antelope NULL values"
    dbnulls_dict = get_nulls()
    if verbose:
        print "Get endtimes for decommissioned stations"
    sta_grps = get_decom_stations()
    for rp_key,rp_val in rrd_pointers.items():
        dbpt = rrd_pointers[rp_key]
        if verbose:
            print "  - Start: Update RRD %s cache archive (%s)" % (rp_key, dbpt)
        dbrrd = datascope.dbopen(dbpt, 'r+')
        dbrrd.lookup('', 'rrdcache', '', '')
        dbrrd.subset("net=~/TA/ && endtime == '%.5f'" % dbnulls_dict['endtime']) 
        dbrrd.sort('sta')
        for j in range(dbrrd.query('dbRECORD_COUNT')):
            dbrrd[3] = j
            rrd_sta, rrd_sta_var, rrd_sta_time, rrd_sta_endtime = dbrrd.getv('sta', 'rrdvar', 'time', 'endtime')
            if rrd_sta in sta_grps:
                print '    - Sta: %s, Rrdvar: %s, Current endtime: %.5f, New endtime: %.5f' % (rrd_sta, rrd_sta_var, rrd_sta_endtime, sta_grps[rrd_sta]['endtime'])
                dbrrd.putv('endtime', '%s' % sta_grps[rrd_sta]['endtime'])
        if verbose:
            print '  - End: Update RRD %s cache archive (%s)' % (rp_key, dbpt)

    print 'Finished updating all RRD cache archives'

    if verbose:
        print "Finished updating RRD archive at %s\n" % strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())

if __name__ == '__main__':
    main()
