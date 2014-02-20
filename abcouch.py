#!/usr/bin/env python

""" abcouch.py

Very simple CouchDB benchmarking tool based on ab, inspired by
https://github.com/mikeal/relaximation

"""

import json
import optparse
import os
import requests
import subprocess

options = None
doc_files = {
    'small': 'small_doc.json',
    'large': 'large_doc.json'
}
timeout = 240
max_n = 2000000
filename = '{0}.{1}.{2}.{3}'
dburl = ''

def parse_arguments():
    global options
    parser = optparse.OptionParser()
    parser.add_option(
        '-w',
        dest='writers',
        help='Number of concurrent clients performing writes'
    )
    parser.add_option(
        '-r',
        dest='readers',
        help='Number of concurrent clients performing reads'
    )
    parser.add_option(
        '-s',
        dest='doc_size',
        help='Document size [large|small]'
    )
    parser.add_option(
        '-n',
        dest='number_of_tests',
        help='Number of tests to run'
    )
    parser.add_option(
        '-t',
        dest='test_runtime',
        help='Total number of seconds for which the test should run'
    )
    (options, args) = parser.parse_args()
    global dburl
    dburl = args[0]

def spawn_ab(args, url):
    full_args = ('ab',) + args + (url, )
    return subprocess.Popen(
        full_args,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE
    )

def spawn_readers(base_args, run):
    url = '{0}/ohai'.format(dburl)
    number_of_clients = int(options.readers)
    ab_args = (
        '-c {0}'.format(
            number_of_clients
        ),
        '-n {0}'.format(
            max_n
        ),
        '-e{0}'.format(
            filename.format(options.doc_size, 'readers', run, 'csv')
        ),
    )
    return spawn_ab(base_args + ab_args, url)

def spawn_writers(base_args, run):
    number_of_clients = int(options.writers)
    ab_args = (
        '-c {0}'.format(
            number_of_clients
        ),
        '-n {0}'.format(
            max_n
        ),
        '-T application/json',
        '-p{0}'.format(
            doc_files.get(options.doc_size, 'small')
        ),
        '-e{0}'.format(
            filename.format(options.doc_size, 'writers', run, 'csv')
        )
    )
    return spawn_ab(base_args + ab_args, dburl)

def setup_db():
    r = requests.put(dburl)
    try:
        r.raise_for_status()
    except Exception, e:
        if e.response.status_code == 412:
            r = requests.delete(dburl)
            r.raise_for_status()
            requests.put(dburl)
            r.raise_for_status()
    with file(doc_files.get(options.doc_size, 'small')) as doc_file:
        doc = json.load(doc_file)
        doc['_id'] = 'ohai'
    r = requests.post(
        dburl,
        headers={'Content-Type': 'application/json'},
        data=json.dumps(doc)
    )
    r.raise_for_status()

def postprocess(run):
    for client_type in ('readers','writers'):
        filename0 = filename.format(
            options.doc_size,
            client_type,
            run,
            'csv'
        )
        lines = file(filename0, 'r').readlines()
        raw_pcts = [lines[51], lines[91], lines[96], lines[100]]
        pcts = [float(raw_pct.split(',')[1]) for raw_pct in raw_pcts]
        print 'run {0}, {1}: p50={2}, p90={3}, p95={4}, p99={5}'.format(
            run,
            client_type,
            *pcts
        )

def main():
    parse_arguments()
    base_args = (
        '-t {0}'.format(
             options.test_runtime
        ),
       '-s {0}'.format(
            timeout
        ),
        '-r',
        '-k'
    )
    for run in range(int(options.number_of_tests)):
        setup_db()
        readers_pid = spawn_readers(base_args, run)
        writers_pid = spawn_writers(base_args, run)
        readers_pid.wait()
        with file(filename.format(
            options.doc_size,
            'readers',
            run,
            'ab'
        ), 'w') as about:
            about.write(readers_pid.communicate()[0])
        writers_pid.wait()
        with file(filename.format(
            options.doc_size,
            'writers',
            run,
            'ab'
        ), 'w') as about:
            about.write(writers_pid.communicate()[0])
        postprocess(run)


if __name__ == '__main__':
    main()
