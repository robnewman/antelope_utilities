"""
Determine the channel that were collecting
data for different time periods for IU and
US contributing network stations

@author   Rob Newman <robertlnewman@gmail.com>
@created  2012-05-15
@modified 2012-05-15
@license  MIT-style license
"""

import os
from pprint import pprint

import antelope.datascope as adb
import antelope.stock as ast

dbprefix = '####'
dbs = {
    '2006': '%s/usarray_2006' % dbprefix,
    '2007': '%s/usarray_2007' % dbprefix,
    '2008': '%s/usarray_2008' % dbprefix,
    '2009': '%s/usarray_2009' % dbprefix,
    '2010': '%s/usarray_2010' % dbprefix,
    '2011': '%s/usarray_2011' % dbprefix,
    '2012': '%s/usarray_2012' % dbprefix
}

def main():
    """Main processing"""

    db = '/anf/TA/rt/usarray/usarray'
    dbptr = adb.dbopen(db, 'r')
    dbptr = adb.dblookup(dbptr, '', 'deployment', '', '')
    dbptr = adb.dbjoin(dbptr, 'sitechan')
    dbptr = adb.dbsubset(dbptr, 'snet=~/US/')
    dbptr = adb.dbsubset(dbptr, 'chan=~/BHZ.*/')
    dbptr = adb.dbsort(dbptr, ('sta'), unique=True)

    print "||STA||CHAN||START||END||\t"
    for i in range(adb.dbnrecs(dbptr)):
        dbptr[3] = i
        sta = adb.dbgetv(dbptr, 'sta')[0]
        for k in sorted(dbs.iterkeys()):
            dbptr2 = adb.dbopen(dbs[k], 'r')
            dbptr3 = adb.dblookup(dbptr2, '', 'wfdisc', '', '')
            dbptr4 = adb.dbsubset(dbptr3, 'sta=~/%s/ && chan=~/BHZ.*/' % sta)
            if adb.dbnrecs(dbptr4) > 0:
                dbptr5 = adb.dbgroup(dbptr4, 'chan')
                for j in range(adb.dbnrecs(dbptr5)):
                    dbptr5[3] = j
                    chan, [db, view, end_record, start_record] = adb.dbgetv(dbptr5, 'chan', 'bundle')
                    dbptr4[3] = start_record
                    chan, time, endtime = adb.dbgetv(dbptr4, 'chan', 'time', 'endtime')
                    readable_time = ast.epoch2str(time, '%Y-%m-%d %H:%M:%S')
                    readable_endtime = ast.epoch2str(endtime, '%Y-%m-%d %H:%M:%S')
                    print "|%s|%s|%s|%s|" % (sta, chan, readable_time, readable_endtime)
                adb.dbfree(dbptr5)
            adb.dbfree(dbptr4)
            adb.dbfree(dbptr3)
            adb.dbfree(dbptr2)

    adb.dbfree(dbptr)
    adb.dbclose(dbptr)
    return 0

if __name__ == '__main__':
    status = main()
    sys.exit(status)
else:
    raise Exception("Not a module to be imported")
    sys.exit()
