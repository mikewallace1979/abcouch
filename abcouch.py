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
dburl ='http://127.0.0.1:5984/abcouch'
timeout = 60
filename = 'out.{0}.{1}.csv'

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
        dest='total_requests_per_client',
        help='Total number of requests each client should make'
    )
    (options, args) = parser.parse_args()

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
            number_of_clients * int(options.total_requests_per_client)
        ),
        '-e{0}'.format(
            filename.format('readers', run)
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
            number_of_clients * int(options.total_requests_per_client)
        ),
        '-T application/json',
        '-p{0}'.format(
            doc_files.get(options.doc_size, 'small')
        ),
        '-e{0}'.format(
            filename.format('writers', run)
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

def postprocess():
    runs = range(int(options.number_of_tests))
    for client_type in ('readers','writers'):
        filenames = ['out.{0}.{1}.csv'.format(
            client_type,
            run
        ) for run in runs]
        lines = [file(f, 'r').readlines() for f in filenames]
        raw_pcts = [(r[51], r[91], r[96], r[100]) for r in lines]
        pcts = [[float(raw_pct.split(',')[1]) for raw_pct in raw_pct_set] for raw_pct_set in raw_pcts]
        transformed = [[pct[i] for pct in pcts] for i in range(len(pcts[0]))]
        mean_pcts = [sum(pct) / len(pct) for pct in transformed]
        print '{0}: p50={1}, p90={2}, p95={3}, p99={4}'.format(
            client_type,
            *mean_pcts
        )

def main():
    parse_arguments()
    base_args = (
       '-t {0}'.format(
            timeout
        ),
        '-r'
    )
    for run in range(int(options.number_of_tests)):
        print 'Starting run {0}'.format(run)
        setup_db()
        readers_pid = spawn_readers(base_args, run)
        writers_pid = spawn_writers(base_args, run)
        readers_pid.wait()
        writers_pid.wait()
    postprocess()


if __name__ == '__main__':
    main()
