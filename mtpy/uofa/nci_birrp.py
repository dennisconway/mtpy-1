# module load birrp

import os
import subprocess
import datetime
from glob import glob
import scipy.signal
import numpy as np
from mtpy.uofa.qel_monitoring_j2edi import convert2edi
# from simpleplotCOH import plotcoh
from mtpy.uofa.simpleplotEDI import plotedi

base_dir = '/g/data/my80/States_and_Territories/SA/Broadband/Renmark_2009/TS'
birrp_location = '/g/data/my80/proc/birrp_comp/birrp'
survey_cfg_fn = '/g/data/my80/proc/test_birrp/stations.cfg'
mtpy_location = '~/mtpy'
instr_resp_fn = ('/home/566/dc3755/mtpy/mtpy/uofa/lemi_coils_instrument' +
                 '_response_freq_real_imag.txt')
tmp_dir = '/tmp'
channels = ['BX', 'BY', 'EX', 'EY']
frequency = 500


def _make_gen(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024*1024)


def _linecount(filename):
    # https://stackoverflow.com/a/27518377
    f = open(filename, 'rb')
    f_gen = _make_gen(f.raw.read)
    return sum(buf.count(b'\n') for buf in f_gen)


def get_metadata(base_dir, frequency):
    """ Return information about each site """
    sites = [i.split('/')[-1] for i in glob(os.path.join(base_dir, '*'))]

    sites = dict([(i, {}) for i in sites])

    for site in sites.keys():
        sites[site]['name'] = site
        sites[site]['files'] = []
        days = sorted([i.split('/')[-1] for
                       i in glob(os.path.join(base_dir, site, '*'))])
        for idx, day in enumerate(days):
            files = glob(os.path.join(base_dir, site, day, '*'))
            if not files:
                continue
            sites[site]['files'].append(files)
            if idx == 0:
                start_time = files[0].split('/')[-1].split('_')[1].split('.')[0]
                start_time = datetime.datetime.strptime(start_time,
                                                        '%y%m%d%H%M%S')
                sites[site]['start_time'] = start_time
        sites[site]['files'] = [j for k in sites[site]['files'] for j in k]
        end_date = files[0].split('/')[-1].split('_')[1].split('.')[0]
        end_date = datetime.datetime.strptime(end_date, '%y%m%d%H%M%S')
        length = _linecount(files[0])
        end_time = end_date + datetime.timedelta(seconds=length/500)
        sites[site]['end_time'] = end_time
        sites[site]['samples'] = (sites[site]['end_time'] -
                                  sites[site]['start_time']).seconds * frequency
    return sites


def calc_intersection(local_site, remote_site):
    """ Calculate time overlap of a local site and remote site """
    int_start = max(local_site['start_time'], remote_site['start_time'])
    int_end = min(local_site['end_time'], remote_site['end_time'])
    if (int_end - int_start).total_seconds()/60/60 < 5:
        return
    else:
        return int_start, int_end


def write_files(files, num_skip, num_samples, channel, out_dir, remote=False):
    """ Write files """
    fn = 'local.' + channel if not remote else 'remote.' + channel
    ofile = open(os.path.join(out_dir, fn), 'w')
    ifile = open(files.pop(0))
    for _ in range(num_skip):
        next(ifile)
    for _ in range(num_samples):
        try:
            line = next(ifile)
        except StopIteration:
            try:
                ifile = open(files.pop(0))
            except IndexError:
                print('did not extract from {}'.format(files))
        ofile.write(line)


def write_birrp_inputs(local_site, remote_site, out_dir):
    """ Write out the intersection of two files """
    if calc_intersection(local_site, remote_site):
        int_start, int_end = calc_intersection(local_site, remote_site)
    local_skip = (int_start - local_site['start_time']).total_seconds()*500
    remote_skip = (int_start - remote_site['start_time']).total_seconds()*500
    local_skip = int(local_skip)
    remote_skip = int(remote_skip)
    num_samples = int((int_end - int_start).total_seconds()*500)
    if local_site['name'] == remote_site['name']:
        remote_skip += 1
        num_samples -= 1
    for channel in channels:
        files = sorted([i for i in local_site['files'] if channel in i])
        write_files(files, local_skip, num_samples, channel, out_dir)
    for channel in [i for i in channels if 'B' in i]:
        files = sorted([i for i in remote_site['files'] if channel in i])
        write_files(files, remote_skip, num_samples, channel, out_dir,
                    remote=True)
    
    return num_samples


def gen_birrp_script(out_dir, samples, sample_rate):  
    birrp_string = '\n'.join(['1', '2', '2', '2', '1', '3', '{2}',
                              '32768,2,10', '5,3,3', 'y', '2',  
                              '0,0.0001,0.9999', '0.0', '0.0', 'output{2}', '0', '0',                              
                              '1', '15', '0', '0', '{1}', '0', '{0}local.EY', '0',
                              '0', '{0}local.EX', '0', '0', '{0}local.BY', '0',
                              '0', '{0}local.BX', '0', '0', '{0}remote.BY',  
                              '0', '0', '{0}remote.BX', '0', '0,90,0',
                              '0,90,0', '0,90,0'])
    birrp_string = birrp_string.format(os.path.join(out_dir, ''),
                                       min(samples, 2e8), sample_rate)
    return birrp_string


def process_birrp_results(out_dir):
    convert2edi('output', out_dir , survey_cfg_fn, instr_resp_fn, None)
    edi = glob(os.path.join(out_dir, '*.edi'))
    plotedi(edi)
    return


def run_birrp(out_dir, birrp_script, birrp_location, mtpy_location,
              survey_cfg_fn):
    j2edi_location = os.path.join(mtpy_location, 'mtpy', 'uofa',
                                  'qel_monitoring_j2edi.py')
    plotedi_location = os.path.join(mtpy_location, 'mtpy', 'uofa',
                                    'simpleplotEDI.py')
    instr_resp_fn = os.path.join(mtpy_location, 'mtpy', 'uofa', ('lemi_coils' +
                                 '_instrument_response_freq_real_imag.txt'))
    script_fn = os.path.join(out_dir, 'script.txt')
    edi_fn = os.path.join(out_dir, 'qel_OUTPUT_000000.edi')
    with open(script_fn, 'w') as f:
        f.write(birrp_script)
    shell_script = '\n'.join(['#PBS -P nf4', '#PBS -q normal',
                              ('#PBS -l walltime=0:20:00,mem=32GB,ncpus=1,'+
                              'jobfs=100MB'),
                              '#PBS -l wd', '', ' < '.join([birrp_location,
                                                           script_fn]), 
                              ' '.join(['python', j2edi_location, 'output', 
                                       out_dir, survey_cfg_fn, instr_resp_fn]),
                              ' '.join(['python', plotedi_location, edi_fn])])
    shell_fn = os.path.join(out_dir, out_dir.split('/')[-1] + '_birrp.sh')
    with open(shell_fn, 'w') as f:
        f.write(shell_script)
    # os.system('qsub {}'.format(shell_fn))


def loop_sites():
    sites = get_metadata(base_dir, frequency)
    for local_site in sites:
        for remote_site in sites:
            dir_name = local_site['name']+remote_site['name']
            out_dir = os.path.join(tmp_dir, dir_name)
            os.makedirs(out_dir)
            samples = write_birrp_inputs(local_site, remote_site, out_dir)
            birrp_script = gen_birrp_script(out_dir, samples)
            run_birrp(out_dir, birrp_script)
            # process_birrp_results(out_dir)


def decimate_file(fn, decimation):
    directory, base = os.path.split(fn)
    base = base.split('.')
    new_fn = base[0] + 'dec' + str(decimation) + '.' + base[1]
    new_fn = os.path.join(directory, new_fn)
    f = np.genfromtxt(fn)
    f = scipy.signal.decimate(f, decimation, ftype='fir', zero_phase=True)
    np.savetxt(new_fn, f)


def run_one_site(local_site, remote_site):
    dir_name = local_site['name']+remote_site['name']
    out_dir = os.path.join(tmp_dir, dir_name)
    os.makedirs(out_dir)
    samples = write_birrp_inputs(local_site, remote_site, out_dir)
    birrp_script = gen_birrp_script(out_dir, samples)
    run_birrp(out_dir, birrp_script)

for i in ['local.BX', 'local.BY', 'local.EX', 'local.EY', 'remote.BX',
          'remote.BY']:
    decimate_file(i, 10)
    decimate_file(i, 100)