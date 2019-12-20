#!/usr/bin/env python 
#****************************************************************************/
# Author: Xuefeng Ding <xuefeng.ding.physics@gmail.com>
# Insitute: Princeton University, Princeton, NJ 08544, USA
# Date: 2019 December 19th
# Version: v1.0
# Description: BAMBI_luigi, an example luigi project that is used in production
#
# All rights reserved. 2019 copyrighted.
#****************************************************************************/
import datetime

date_format = "%Y_%b_%d"
ref_date = datetime.date(year = 2011, month = 12, day = 11)
wref_t = datetime.date(year = 2016, month = 1, day = 10)
prefix = '/x'
DST_strf = 'dst/cycle_19/%Y/dst_%Y_%b_%d_c19.root'
ntuple_str = 'users/x/CNO_ntuples_c19/OUTPUT/cno_ntuples_{}_c19.root'
ntuple_strf = ntuple_str.format(date_format)
script_dir = 'users/x/CNO_ntuples_c19/offline/bxfilter/'
merge_dir = 'users/x/BX-Analysis/InputProduction/4_cno_ntuple/3_finalize/'
merge_strf = 'cno_ntuples_%s-%s_stripped.root'

def date2str(t):
    return t.strftime(date_format)

def str2date(t):
    return datetime.datetime.strptime(t,date_format).date()

def n_week(t):
    return int((t - ref_date).days / 7)

def w2date(w):
    return ref_date + datetime.timedelta(days = w*7)

def normalize_date(date,last = False):    
    w = int((date - ref_date).days/7)
    return ref_date + datetime.timedelta(days = w*7 + (6 if last else 0))

import julian
def to_years(date):
    mjd = julian.to_jd(datetime.datetime.combine(date, datetime.time()), fmt='mjd')
    return (mjd-40587.)/365.2422+1970

def to_date(year):
    mjd = (year-1970)*365.2422+40587
    return julian.from_jd(mjd, fmt='mjd')

def config(file,var,value):
    import massedit 
    massedit.edit_files([file],[r"re.sub(r'^\\def\\"+var+r"\{.*\}', r'\\def\\"+var+
        "{"+str(value[0])+"}', line)"],dry_run = False)

def check_file(file,show = False):
    #print('checking {}'.format(file))
    import os
    if os.path.isfile(file):
        return True
    else:
        if show: print('missing {}'.format(file))
        return False

def check_both(t):
    # without DST, or with DST and with ntuple.
    return not check_file(t.strftime(prefix + DST_strf)) or \
        check_file(t.strftime(prefix + ntuple_strf),True)

def find_TODO():
    TODO = []
    for i in range(n_week(datetime.datetime.now().date())):
        t = w2date(i)
        if not check_both(t):
            TODO.append(t)
    print('found',len(TODO),'jobs')
    return TODO

def find_maxt():
    for i in range(n_week(datetime.datetime.now().date())-1,n_week(wref_t),-1):
        if check_file(prefix+merge_dir+merge_strf %
                           (wref_t.strftime(date_format),w2date(i).strftime(date_format))):
            return w2date(i)
    return wref_t

def write_job(job='test',TODO = [],prev_maxt = wref_t):
    with open(prefix + script_dir + job + '.sh','w') as out:
        out.write('#!/bin/bash\n')
        for t in TODO:
            print('writting %s ...'% t.strftime(date_format))
            out.write(job_strf %
                      (prefix + script_dir,
                       t.strftime(date_format),t.strftime(date_format),
                       t.strftime(prefix + DST_strf),
                       t.strftime(prefix + ntuple_strf),
                       t.strftime(prefix + ntuple_strf)
                      ))
    with open(prefix + merge_dir + job + '.sh','w') as out:
        out.write('#!/bin/bash\n')
        for t in TODO:
            out.write('./make_input %s %s\n' % (t.strftime(date_format),t.strftime(date_format)))
        out.write('hadd ')
        out.write(merge_strf % (wref_t.strftime(date_format),TODO[-1].strftime(date_format)))
        out.write(' '+merge_strf % (wref_t.strftime(date_format),prev_maxt.strftime(date_format)))
        for t in TODO:
            out.write(' '+merge_strf % (t.strftime(date_format),t.strftime(date_format)))
