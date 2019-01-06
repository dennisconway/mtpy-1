# module load birrp

import os
import subprocess
import datetime
from glob import glob


base_dir = '/g/data/my80/States_and_Territories/SA/Broadband/Renmark_2009/TS'
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
          

def gen_birrp_script(out_dir):
    birrp_string =  '\n'.join(['1', '2', '2', '2', '0', '2', '-500',
                               '65536,2,12', '3,1,3', 'y', '2',
                               '0,0.0001,0.9999', '0', 'output', '0', '3',
                               '-46.875,-93.750,-156.25', '1', '15', '0', '0',
                               '1000000', '0', '{0}remote.BY', '0', '0',
                               '{0}remote.BX', '0', '0', '{0}local.BY', '0',
                               '0', '{0}local.BX', '0', '0', '{0}local.EY',
                               '0', '0', '{0}local.EX', '0', '0,90,0',
                               '0,90,0', '0,90,0'])
    birrp_string = birrp_string.format(os.path.join(out_dir, ''))
    return birrp_string


def run_birrp(out_dir, birrp_script):
    os.chdir(out_dir)
    p = subprocess.Popen(['birrp-5.3.2'], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         encoding='utf8')
    o, e = p.communicate(birrp_script)


def plot_birrp_results(out_dir):
    pass


def loop_sites():
    sites = get_metadata(base_dir, frequency)
    for local_site in sites:
        for remote_site in sites:
            dir_name = local_site['name']+remote_site['name']
            out_dir = os.path.join(tmp_dir, dir_name)
            os.makedirs(out_dir)
            write_birrp_inputs(local_site, remote_site, out_dir)
            birrp_script = gen_birrp_script(out_dir)
            run_birrp(out_dir, birrp_script)
            plot_birrp_results(out_dir)
