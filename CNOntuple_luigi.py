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
import luigi
import BAMBIutil as bt
import datetime
import subprocess

class ExternalDST(luigi.ExternalTask):
    """
    input DST produced by x
    """
    date = luigi.DateParameter()

    def output(self):
        """
        /storage/gpfs_data/borexino/dst/cycle_19/%Y/dst_%Y_%b_%d_c19.root
        """
        return luigi.LocalTarget(self.date.strftime(bt.prefix + bt.DST_strf))

class MakeLiveT(luigi.Task):
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(MakeLiveT, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def output(self):
        return luigi.LocalTarget(f"../crosscheck/liveT_{self.date_str}.root")
    
    def requires(self):
        yield ExternalDST(date = self.date)

    def run(self):
        import os
        subprocess.call(['root','-b','-q',
          f'get_liveT.C+g("{self.date_str}")'],cwd = '../crosscheck')
    
from luigi.contrib.lsf import LSFJobTask
from luigi.contrib.lsf import LocalLSFJobTask

import logging
LOGGER = logging.getLogger('luigi-interface')

class MakeNtuple(LSFJobTask): # class MakeNtuple(LocalLSFJobTask)
    '''
    submit and run LSF job
    '''
    date = luigi.DateParameter()
    n_cpu_flag = 1
    queue_flag = 'borexino_run'
    shared_tmp_dir = bt.prefix+'tmp'
    extra_bsub_args = 'export PATH=$PATH;'
    memory_flag = 2097152
    resource_flag = 'mem=2097152'

    def output(self):
        return luigi.LocalTarget(self.date.strftime(bt.prefix+bt.ntuple_strf))
    
    def requires(self):
        return ExternalDST(date = self.date)

    def work(self):
        LOGGER.info('Running job {}'.format(self.job_name_flag))
        input_DST = self.input().path
        output_ntuple = self.output().path
        script_path = bt.prefix + bt.script_dir
        subprocess.call(['./main',input_DST,output_ntuple],cwd = script_path)
        subprocess.call(['chmod','+x',output_ntuple],cwd = script_path)


class StripNtuple(luigi.Task):
    '''
    strip ntuple
    '''
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(StripNtuple, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def output(self):
        return luigi.LocalTarget('3_finalize/'+bt.merge_strf % (self.date_str,self.date_str))

    def requires(self):
        return MakeNtuple(date = self.date)

    def run(self):
        subprocess.call(['./make_input',self.date_str,self.date_str],cwd = '3_finalize')
                
class MergeNtuples(luigi.Task):
    '''
    merge ntuple
    '''
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(MergeNtuples, self).__init__(*args, **kwargs)
        self.date_obj = bt.str2date(bt.date2str(self.date))
        self.date_str = bt.date2str(self.date_obj)

    def output(self):
        return luigi.LocalTarget('3_finalize/'+bt.merge_strf % (bt.date2str(bt.wref_t), self.date_str))
    
    def requires(self):
        yield MergeNtuples(date = self.date_obj - datetime.timedelta(days = 7))
        yield StripNtuple(date = self.date_obj)
    
    def run(self):
        cmd = ['hadd', self.output().path] + [ f.path for f in self.input()]
        print(' '.join(cmd))
        subprocess.call(cmd)

class MergeNtuplesLiveT(luigi.WrapperTask):
    '''
    MergeNtuples + MakeLiveT
    '''
    date = luigi.DateParameter()
    def requires(self):
       yield MergeNtuples(date = self.date) 
       yield MakeLiveT(date = self.date) 


class EasyMergeNtuples(luigi.WrapperTask):
    '''
    date can be any day of the week
    '''
    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(days=7))

    def requires(self):
        return MergeNtuplesLiveT(date = bt.normalize_date(bt.str2date(bt.date2str(self.date))))
