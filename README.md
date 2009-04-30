Cascade
-------

This is some rough work on playing on the chainable Map/Reduce implementation
for CouchDB. Its missing some important bits that will let you trash things
    pretty easy but I figure its better than nothing.

TODO
====

  * Add a gen_server to track in progress updates similar to the view
pattern. Right now simultaneous clients will end up stomping on each other.
  * Add status caching. Right now it still does a complete index scan when
propogating updates unconditionally. Most likely gonna solve this with a _local
doc on the root node.
  * Add arbitrary barnching of work flows. Right now each _design doc is limited
to a linear work flow to make the initial implementation easier.
  * Add merging. I really want to add the merge phase from Map/Reduce/Merge.
  * Better documentation...

Building
========

    $ make

If that fails, well, you got this far...

Installing
==========

No real installation yet.

Running
=======

I run this with the CouchDB dev server runner.

    $ cd $PATH_TO_COUCHDB
    $ ./bootstrap && ./configure && make dev
    $ ERL_LIBS=/path/to/cascade ./utils/run

Configuring
===========

Add the following to your local_dev.ini:

    [httpd_db_handlers]
    _cascade = {cascade, handle_cascade_req}

Testing
=======

    $ cd ./test
    $ ./curl.sh

This requires couchdb-python cause I was lazy. And curl. And some other things.
Hopefully the tests give you the right idea in the general work flow.


