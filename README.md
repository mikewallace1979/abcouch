# abcouch

Really simple [ab](http://httpd.apache.org/docs/2.4/programs/ab.html) wrapper for generating concurrent read/write load against couchdb instances and measuring the response times.

## Example

Start five runs of a five minute test with 10 concurrent writers, 20 concurrent readers and a large document size, against a CouchDB instance at http://localhost:5984 using the database `test`:

    $ ./abcouch.py -w 10 -r 20 -t 300 -n 5 -s large http://localhost:5984/test

Run `./abcouch.py -h` for a full list of options.

## Requirements

An apache benchmark binary built from an httpd no earlier than version httpd-2.4.x.
