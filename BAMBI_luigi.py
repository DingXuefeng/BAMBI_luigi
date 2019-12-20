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
import CNOntuple_luigi as nt
import datetime
import os

import subprocess
class StabilityCheck(luigi.Task):
    """
    stability check
    """
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(StabilityCheck, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def output(self):
        for f in ['csv','pdf']:
            yield luigi.LocalTarget(f'3_finalize/stability/full_{self.date_str}.{f}')

    def requires(self):
        return nt.MergeNtuplesLiveT(date = self.date)

    def run(self):
        subprocess.call(['root','-b','-q',f'validation.C("{self.date_str}")']
                ,cwd='3_finalize/stability',stdout=subprocess.DEVNULL)

class PoTimeFit(luigi.Task):
    '''
    Do temporal fit
    '''
    date = luigi.DateParameter()
    job = luigi.Parameter(default='trend')
    mass = luigi.IntParameter(default=20)
    Ndur = luigi.IntParameter(default=6)

    def __init__(self, *args, **kwargs):
        super(PoTimeFit, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def output(self):
        return luigi.LocalTarget(
                f'3_finalize/Po210/{self.job}_{self.date_str}_{self.mass}t_{self.Ndur}')

    def requires(self):
        return nt.MergeNtuplesLiveT(date = self.date)

    def run(self):
        subprocess.call(['root','-b','-q',
            f'stability.C("{self.job}","{self.date_str}",{self.mass},{self.Ndur})'],
            cwd='3_finalize/Po210',stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

class PoMergedFit(luigi.Task):
    '''
    Do merged fit. w/ or w/o corrections
    '''
    date = luigi.DateParameter()
    job = luigi.Parameter(default='recent')
    start = luigi.FloatParameter(default = 2019.5)
    end = luigi.FloatParameter(default = 2050)
    correctz = luigi.BoolParameter(default = False)
    plateau = luigi.BoolParameter(default = False)

    def __init__(self, *args, **kwargs):
        super(PoMergedFit, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def output(self):
        for f in ['.csv','_2D.pdf','_projrho.pdf','_projz.pdf']:
            yield luigi.LocalTarget(f'3_finalize/Po210-aligned/{self.job}_{self.date_str}{f}')

    def requires(self):
        if self.correctz:
            yield PoTimeFit(date = self.date, job = 'zcalib', mass = 70, Ndur = 12)
        yield nt.MergeNtuples(date = self.date)

    def run(self):
        return subprocess.call(['root','-b','-q',
            f'aligned.C("{self.job}","{self.date_str}",{self.start},{self.end},20,'+
            f'{"true" if self.correctz else "false"},"../Po210/{os.path.basename(self.input()[0].path)}",'+
            f'{"true" if self.plateau else "false"})'],
            cwd='3_finalize/Po210-aligned')

class PlotZ(luigi.Task):
    '''
    plot z of bubble minimum
    '''
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(PlotZ, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def requires(self):
        yield PoTimeFit(date = self.date,job = 'trend')
        yield PoTimeFit(date = self.date, job = 'zcalib', mass = 70, Ndur = 12)

    def output(self):
        return luigi.LocalTarget(f'3_finalize/Po210/R_z0_{self.date_str}.pdf')

    def run(self):
        subprocess.call(['root','-b','-q',f'draw_z0.C("{self.date_str}")'],
                cwd='3_finalize/Po210',stdout=subprocess.DEVNULL)

class PlotR(luigi.Task):
    '''
    plot R of bubble minimum
    '''
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(PlotR, self).__init__(*args, **kwargs)
        self.date_str = bt.date2str(self.date)

    def requires(self):
        return PoTimeFit(date = self.date,job = 'trend')

    def output(self):
        return luigi.LocalTarget(f'3_finalize/Po210/R_{self.date_str}.pdf')

    def run(self):
        subprocess.call(['root','-b','-q',f'draw_Rexp.C("{self.date_str}")'],
                cwd='3_finalize/Po210',stdout=subprocess.DEVNULL)

class MakeReport(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(f'report/WeeklyPo210_{bt.date2str(self.date)}.pdf')

    def requires(self):
        return {'stability':StabilityCheck(date = self.date),
                'recent':PoMergedFit(date = self.date, plateau = True),
                'full':PoMergedFit(date = self.date,job = 'full', 
                    start = 2017, correctz = True),
                'z':PlotZ(date = self.date),
                'R':PlotR(date = self.date)}

    def run(self):
        report = 'report/WeeklyPo210.tex'
        import pandas as pd
        import itertools
        df = pd.read_csv(list(self.input()['stability'])[0].path)
        bt.config(report,'Tag',[bt.date2str(self.date)])
        date2str = lambda date : [date.strftime("%Y-%b-%d")]
        bt.config(report,'ThisWeek',date2str(self.date))
        last_run_date_str = df['last run (date)'][0]
        last_run_date = datetime.datetime.strptime(last_run_date_str, "%Y-%m-%d").date()
        bt.config(report,'LastWeekIncluded',date2str(bt.normalize_date(last_run_date)))
        bt.config(report,'BAMBIdays',df['live time (days)'])
        bt.config(report,'NRuns',df['Nrun'])
        bt.config(report,'RunFirst',df['first run'])
        bt.config(report,'RunFirstDate',df['first run (date)'])
        bt.config(report,'RunLast',df['last run'])
        bt.config(report,'RunLastDate',df['last run (date)'])
        bt.config(report,'NRunsBad',df['N low rate'])

        df = pd.read_csv(list(self.input()['recent'])[0].path)
        bt.config(report,'RecentNRuns',df['Nrun'])
        bt.config(report,'RecentRunFirst',df['first run'])
        bt.config(report,'RecentRunFirstDate',[bt.normalize_date(bt.to_date(df['first run time (year)']).date())])
        bt.config(report,'RecentRunLast',df['last run'])
        bt.config(report,'RecentRunLastDate',[bt.normalize_date(bt.to_date(df['last run time (year)']).date(),last = True)])
        bt.config(report,'RecentLiveT',round(df['live time (days)'],1))
        bt.config(report,'RecentRate',round(df['R (cpd/100t)'],1))
        bt.config(report,'RecentRateErr',round(df['Re (cpd/100t)'],1))
        bt.config(report,'RecentPlateauSize',round(df['pan size (t)'],2))

        df = pd.read_csv(list(self.input()['full'])[0].path)
        bt.config(report,'FullNRuns',df['Nrun'])
        bt.config(report,'FullRunFirst',df['first run'])
        bt.config(report,'FullRunFirstDate',[bt.normalize_date(bt.to_date(df['first run time (year)']).date())])
        bt.config(report,'FullRunLast',df['last run'])
        bt.config(report,'FullRunLastDate',[bt.normalize_date(bt.to_date(df['last run time (year)']).date(),last = True)])
        bt.config(report,'FullLiveT',round(df['live time (days)'],1))
        bt.config(report,'FullRate',round(df['R (cpd/100t)'],1))
        bt.config(report,'FullRateErr',round(df['Re (cpd/100t)'],1))

        files = list(self.input()['stability'])[1:]+list(self.input()['recent'])[1:]+ \
                list(self.input()['full'])[1:]+ [self.input()['z']]+[self.input()['R']]
        newfiles = []
        for file in files:
            subprocess.call(['cp',file.path,'report'],stdout=subprocess.DEVNULL)
            newfiles.append(os.path.basename(file.path))
        print('#'.join(['./laton','-o',self.output().path,'WeeklyPo210.tex','LOGO.png']+newfiles))
        subprocess.call(['./laton','-o',os.path.basename(self.output().path),
            'WeeklyPo210.tex','LOGO.png']+newfiles,
            cwd='report',stdout=subprocess.DEVNULL)

class SendReport(luigi.Task):
    date = luigi.DateParameter()
    test = luigi.BoolParameter()

    def output(self):
        return luigi.LocalTarget(f'report/sent_{self.date}')

    def requires(self):
        return MakeReport(date = self.date)

    def run(self):
        print('running..')
        import yagmail

        sender_email = 'xxx@gmail.com'
        receiver_emails = {'a@b.com' : '"NickA" <a@b.com>',
                           'c@d.com' : '"NickB" <c@d.com>'}
        if self.test:
            receiver_emails = 'm@x.com'
        subject = f'Po-210 Weekly Update (new DST = {self.date})'
        sender_password = 'xxx'

        import datetime
        try: 
            yag = yagmail.SMTP(user=sender_email, password=sender_password)

            contents = [
                    "<p><font size=6>Greetings Master!<br><br>",
                    "This is the Po-210 deamon created by Dr. Ding for sending the weekly report of Po-210.",
                    f"new DST detected: {self.date}!!",
                    "Please check the report in the attachment<br><br>",
                    "Best regards",
                    "The Borexino Po-210 daemon",
                    datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")+"</font></p>",
                    self.input().path
                    ]

            if yag.send(receiver_emails, subject, contents) != False:
                with open(self.output().path,'w') as f:
                    f.write('ok')

        except Exception as e:
            print(e)

class EasySendReport(luigi.WrapperTask):
    '''
    date can be any day of the week
    '''
    date = luigi.DateParameter(default=datetime.date.today()-datetime.timedelta(days=7))
    test = luigi.BoolParameter()

    def requires(self):
        return SendReport(date = bt.normalize_date(bt.str2date(bt.date2str(self.date))),test = self.test)
